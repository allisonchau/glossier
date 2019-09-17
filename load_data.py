import requests
from io import BytesIO
from zipfile import ZipFile
import sys

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import String, Boolean, BIGINT, DateTime, Float
import creds


import pandas as pd
import json
import datetime
from collections import Counter

# import os

# Declaring global dicts to be updated throughout file iteration
user_dict={}
line_item_dict={}


# Variables to include and exclude for each dict
order_exclude_list=['line_items','id']
user_include_list=['customer_locale','buyer_accepts_marketing','contact_email','phone']

# String variables to convert to datetime
timestamp_vars=['created_at','processed_at','updated_at','closed_at','cancelled_at']

# Declaring SQLalchemy data types
order_type_dict={
    'email':String,
    'number':BIGINT,
    'total_price':Float,
    'total_tax':Float,
    'subtotal_price':Float,
    'total_discounts':Float,
    'total_line_items_price':Float,
    'confirmed':Boolean,
    'checkout_id':BIGINT,
    'line_item_total':BIGINT,
    'processed_at':DateTime(timezone=True),
    'cancelled_at':DateTime(timezone=True),
    'created_at':DateTime(timezone=True),
    'updated_at':DateTime(timezone=True),
    'closed_at':DateTime(timezone=True)   
}

def download_extract_zip(url):
    # Function to load data from url and return fn, fobj as iterables
    r=requests.get(url)
    if (r.status_code==200):
        print('Loading data...')
    else:
        print('Error: URL unresponsive')
        return None
    with ZipFile(BytesIO(r.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile

def load_order_row(orderrows):
    # Function to load and return order data in a daily file
    # Also, to update global variables line_item_dict and order_dict
    # Probably a better way to do this

    order_dict={}
    
    for orderrow in orderrows:
        # Declare useful vars
        order_id=orderrow['id']
        user_id=orderrow['user_id']
        processed_at=convert_timestring_utc(orderrow['processed_at'])
        total_price=float(orderrow['total_price'])

        # Separate out line item array
        line_item_list=orderrow['line_items']
        line_item_total=len(line_item_list)
        # item_total=sum([x['quantity'] for x in line_item_list])

        # Update order dict
        order_dict[order_id]={k:v for k,v in orderrow.items() 
                                if( k not in set(order_exclude_list) 
                                and k not in set(user_include_list))}
        order_dict[order_id]['line_item_total']=line_item_total

        # Update line item dict
        for line_item in line_item_list:
            line_item_id=line_item['id']
            line_item_dict[line_item_id]={k:v for k,v in line_item.items() if k!='id'}
            line_item_dict[line_item_id]['order_id']=order_id

        # Update user dict
        if (user_id not in user_dict):
            user_dict[user_id]={k:v for k, v in orderrow.items() 
                                if k in set(user_include_list)}
            # Keep track of total orders and spend for future bucketing
            user_dict[user_id]['total_orders']=1
            user_dict[user_id]['total_spend']=total_price
            user_dict[user_id]['min_processed_at']=processed_at
            user_dict[user_id]['max_processed_at']=processed_at
            user_dict[user_id]['total_days']=1
        else:
            prev_min=user_dict[user_id]['min_processed_at']
            prev_max=user_dict[user_id]['max_processed_at']
            min_processed_at=min(prev_min,processed_at)
            max_processed_at=max(prev_max,processed_at)
            # Update user vars with most recent ones
            if max_processed_at!=prev_max:
                for (k, v) in orderrow.items():
                    if k in set(user_include_list):
                        user_dict[user_id][k]=v
            user_dict[user_id]['total_orders']+=1
            user_dict[user_id]['total_spend']+=total_price
            user_dict[user_id]['min_processed_at']=min_processed_at
            user_dict[user_id]['max_processed_at']=max_processed_at
            # Keep track of total days we have seen user
            user_dict[user_id]['total_days']=(max_processed_at-min_processed_at).days+1

    order_df=pd.DataFrame.from_dict(order_dict,orient='index')   
    return order_df


def convert_timestring_utc(x):
    # Given string, convert into standard UTC datetime object
    if x is None:
        return x
    x=x[:-6]+''.join(x[-6:].split(':'))
    return datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%S%z').astimezone(tz=datetime.timezone.utc)
    


def convert_to_datetime(df):
    # Convert all appropriate columns to datetime
    for var in timestamp_vars:
        if var in df:
            df[var]=df[var].map(convert_timestring_utc)
    return df

def append_df_to_psql(df,table_name,engine):
    # Add dataframe to PSQL table
    df=convert_to_datetime(df)
    df.to_sql(name=table_name, con=engine, if_exists = 'append', index=True,dtype=order_type_dict)

def main(args):
    url=args[1]
    datafiles=download_extract_zip(url)
    print('Connecting to PSQL...')
    # Connecting to PostgreSQL by providing a sqlachemy engine
    engine = create_engine('postgresql://'+creds.username+':'
                           +creds.password+'@'+creds.host+':'
                           +creds.port+'/'+creds.db,echo=False)
    # Declare fn_count and month to track load status
    fn_count=0
    month=''
    # Iterate through the extracted files
    for (fn,fobj) in datafiles:
        daily_order_data=json.load(fobj)['orders']
        order_df=load_order_row(daily_order_data)
        # Extract year and month from filename for tracking
        # and for table naming
        fn_month=fn[:4]+'_'+fn[5:7]
        table_name='orders__%s'%fn_month
        append_df_to_psql(order_df,table_name,engine)

        # Logic for printing out load status
        # Print out how many files were inserted into a monthly table
        if (fn_count==0):
            month=fn_month
            fn_count+=1

        elif (fn_month==month):
            fn_count+=1

        else:
            print('Loaded %d file(s) into %s'%(fn_count,'orders__%s'%month))
            month=fn_month
            fn_count=1
            break
    # Print out final insert statement
    print('Loaded %d file(s) into %s'%(fn_count,'orders__%s'%month))
    
    # Add in line item and user stat tables
    line_item_df=pd.DataFrame.from_dict(line_item_dict,orient='index')
    append_df_to_psql(line_item_df,'order_line_item_mapping',engine)
    print('Loaded %d record(s) into %s'%(len(line_item_df),'order_line_item_mapping'))
    user_df=pd.DataFrame.from_dict(user_dict,orient='index')
    append_df_to_psql(user_df,'user_stats',engine)
    print('Loaded %d record(s) into %s'%(len(user_df),'user_stats'))

if __name__ == "__main__":
    main(sys.argv)
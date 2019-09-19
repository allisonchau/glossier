import requests
from io import BytesIO
from zipfile import ZipFile
import sys

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import String, Boolean, BIGINT, DateTime, Float
import creds
import update_user_table_queries as queries


import pandas as pd
import json
import datetime
from collections import Counter
# import os

# Declaring global dict to be updated throughout file iteration
user_dict={}
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
        return r.content
    else:
        print('Error: URL unresponsive')
        return None
def read_table_index(table_name,colname,engine):
    try: 
        return set(pd.read_sql(table_name,engine,columns=[colname])[colname])
    except:
        return ()
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
def append_dict_to_psql(d,table_name,engine,action='append',verbose=False):
    # Add dictionary d to PSQL table
    df=pd.DataFrame.from_dict(d,orient='index') 
    df=convert_to_datetime(df)
    df.to_sql(name=table_name, con=engine, if_exists=action, index=True,dtype=order_type_dict)
    if verbose:
        print('Loaded %d record(s) into %s'%(len(df),table_name))

def main(args):
    url=args[1]
    datafiles=download_extract_zip(url)
    print('Connecting to PSQL...')
    # Connecting to PostgreSQL by providing a sqlachemy engine
    engine = create_engine('postgresql://'+creds.username+':'
                           +creds.password+'@'+creds.host+':'
                           +creds.port+'/'+creds.db,echo=False)
    existing_order_ids=read_table_index('order_line_item_mapping','order_id',engine)
    existing_line_item_ids=read_table_index('order_line_item_mapping','index',engine)
    existing_user_ids=read_table_index('user_stats','index',engine)
    # Declare fn_count and month to track load status
    fn_count=0
    month=''
    def load_order_row(orderrows, year_month):
        # Function to load and return order data in a daily file
        # Also, to update global variables line_item_dict and order_dict
        # Probably a better way to do this

        order_dict={}
        line_item_dict={}

        for orderrow in orderrows:
            # Declare useful vars
            order_id=orderrow['id']
            if order_id in existing_order_ids:
                continue
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
                if line_item_id in existing_line_item_ids:
                    continue
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
        if len(order_dict)==0:            
            return False
        else:
            table_name='orders__%s'%year_month
            append_dict_to_psql(order_dict,table_name,engine)
            append_dict_to_psql(line_item_dict,'order_line_item_mapping',engine)
            return True

    with ZipFile(BytesIO(datafiles)) as thezip:
         # Iterate through and extract files
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as fobj:
                fn=zipinfo.filename
                # if (fn=='2017-11-06.json'):
                #     break
                # Extract year and month from filename for tracking
                # and for table naming
                fn_month=fn[:4]+'_'+fn[5:7]
                daily_order_data=json.load(fobj)['orders']
                loaded=load_order_row(daily_order_data,fn_month)
                # Continue if no rows were appended
                if not loaded:
                    continue
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
                    # break
    # Print out final insert statement
    print('Loaded %d file(s) into %s'%(fn_count,'orders__%s'%month))
    # Dict to append the new user_id records
    user_stats_dict={k:v for k,v in user_dict.items() 
                        if k not in existing_user_ids}
    append_dict_to_psql(user_stats_dict,'user_stats',engine,verbose=True)
    # Dict to update the existing records
    user_staging_dict={k:v for k,v in user_dict.items() 
                        if k in existing_user_ids}
    append_dict_to_psql(user_staging_dict,'user_staging',engine,action='replace',verbose=True)
    # Need to make psycopg2 cursor to execute an update statement
    conn = psycopg2.connect("dbname='%s' port='%s' user='%s' \
                host='%s' \
                password='%s'" % (creds.db,creds.port,creds.username,creds.host,creds.password)
            )
    cursor=conn.cursor()
    # Query to update user_stats table
    cursor.execute(queries.update_query)
    print('user_stats status: '+cursor.statusmessage)
     # Query to update total_days column in user_stats table
    cursor.execute(queries.update_days_query)
    conn.commit()


if __name__ == "__main__":
    main(sys.argv)
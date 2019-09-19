# Glossier ETL Project

#### ETL for retail order data so that business analysts can query using SQL from a PSQL database. This program will create new tables if they do not already exist. This also supports daily loading of new data. 

1. Extract: s3 bucket containing data in raw JSON format is provided at https://s3.amazonaws.com/data-eng-homework/v1/data.zip 

2. Transform: 
- Create order - line item mapping table
- Date variables with time zone extracted, standardize w/ utc time
- Create new variables to track user metadata (total days, total spend, total orders)

3. Load into PSQL tables:
- Bypass all records that already exist in the tables
- Append unique order rows to orders__year_month tables
- Append unique line item rows to order_line_item_mapping table
- Append new users to user_stats table
- Create user_staging table to hold existing users and update their records

##### Logic for updating user_stats table for daily updating
- Queries are stored in 'update_user_table_queries.py', which updates the user_stats table with new data from the user_staging table. These queries are accessed from the main script 'load_data.py'.
```python
# global variable user_dict stores and updates all user records found in given s3 bucket url
if user_id exists in user_stats (PSQL table):
	add user_id to user_staging
else (user is new):
	append to user_stats table
finally, update the user_stats table with the user_staging table, using max_processed_at to ensure the older data is replaced with newest data
```

**To run:**
>python load_data.py [s3 bucket link]

**Example run status output:**
```
>Connecting to PSQL...
>Loading data...
>Loaded 3 file(s) into orders__2017_12
>Loaded 1 file(s) into orders__2017_10
>Loaded 3625 record(s) into order_line_item_mapping
>Loaded 8 record(s) into user_stats
>Loaded 34 record(s) into user_staging
>user_stats status: UPDATE 34
```

**Example of final tables within PSQL:**

                 List of relations

 Schema | Name
 ---|---
 public | order_line_item_mapping 
 public | orders__2017_10
 public | orders__2017_11         
 public | orders__2017_12         
 public | user_stats  
 public | user_staging            






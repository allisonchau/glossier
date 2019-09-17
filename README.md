# Glossier ETL Project

#### ETL for retail order data so that business analysts can query using SQL from a PSQL database.

1. Extract: s3 bucket containing data in raw JSON format is provided at https://s3.amazonaws.com/data-eng-homework/v1/data.zip 

2. Transform: 
- Date variables with time zone extracted, standardize w/ utc time
- Grouped and established hierarchy of vars

3. Load:
- First time: create new tables and load the transformed data
- ETL should run daily, so update tables with new information


-Python script to:
- Load filename and file object from s3 bucket
- Append the order data and store in PSQL
- Append line item data
- Update user stats

**To run:**
>python load_data.py [s3 bucket link]

**Example run status output:**
```
>Connecting to PSQL...
>Loading data...
>Loaded 3 file(s) into orders__2017_12
>Loaded 1 file(s) into orders__2017_10
>Loaded 3625 record(s) into order_line_item_mapping
>Loaded 23 record(s) into user_stats
```

**Example of final tables within PSQL:**

                 List of relations

 Schema | Name
 ---|---
 public | order_line_item_mapping 
 public | orders__2017_10         
 public | orders__2017_12         
 public | user_stats              






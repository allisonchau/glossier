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


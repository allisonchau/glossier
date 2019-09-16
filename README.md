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
-Create tables in PSQL

-Python script to:
-Load filename from s3 bucket
-Check if filename is loaded
-Update the order data and store in PSQL
-Update line item data
-Update user stats
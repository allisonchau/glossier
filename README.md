# glossier

## ETL for retail order data so that business analysts can query using SQL from a PSQL database.

Extract: s3 bucket containing data in raw JSON format is provided at https://s3.amazonaws.com/data-eng-homework/v1/data.zip 

Transform: 
    -Date variables with time zone extracted, standardize w/ utc time
    -

Load:
    -First time: create new tables and load the transformed data
    -ETL should run daily, so update tables with new information

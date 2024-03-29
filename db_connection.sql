psql -h data-engineering-test.dev.glossier.io -U allison -d allison_db -p 80

SELECT COUNT(DISTINCT user_id)
FROM orders__2017_12
;

/*
user_staging and user stats
user staging is a new batch, replaced every time





if not in existing user stats:
    if not in dict:
        NEW ENTRY DICT THEN APPEND PSQL
    if in dict:
        FIX DICT THEN APPEND SQL
if in existing user stats:
    if not in dict:
        NEW ENTRY DICT THEN Update PSQL
    if in dict:
        FIX DICT 

*/


update user_stats
    set 
    buyer_accepts_marketing = case
        when user_staging.max_processed_at>user_stats.max_processed_at
            then user_staging.buyer_accepts_marketing 
        end
    ,phone = case 
        when user_staging.max_processed_at>user_stats.max_processed_at
            then user_staging.phone 
        end
    ,customer_locale = case
        when user_staging.max_processed_at>user_stats.max_processed_at
            then user_staging.customer_locale
        end
    ,contact_email = case
        when user_staging.max_processed_at>user_stats.max_processed_at
            then user_staging.contact_email
        end
    ,total_orders=user_stats.total_orders+user_staging.total_orders
    ,total_spend=user_stats.total_spend+user_staging.total_spend
    ,min_processed_at = case 
        when user_staging.min_processed_at<user_stats.min_processed_at
            then user_staging.min_processed_at
        end
    ,max_processed_at = case
        when user_staging.max_processed_at>user_stats.max_processed_at
            then user_staging.max_processed_at
        end
from user_staging
where user_stats.index=user_staging.index
;   

update user_stats
    set 
       total_days=extract(days from max_processed_at-min_processed_at)+1
;

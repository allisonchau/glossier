update_query='''update user_stats
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
;'''

update_days_query='''update user_stats
    set 
       total_days=extract(days from max_processed_at-min_processed_at)+1
;'''

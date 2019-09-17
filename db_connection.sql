psql -h data-engineering-test.dev.glossier.io -U allison -d allison_db -p 80

SELECT COUNT(DISTINCT user_id)
FROM orders__2017_12
;
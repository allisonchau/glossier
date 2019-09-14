psql -h data-engineering-test.dev.glossier.io -U allison -d allison_db -p 80


create table incoming (
	id bigint encode zstd

)
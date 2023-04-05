----









select * from actor_table;


----
create or replace table mytable2 (id int, name text);
insert into mytable2 values (1, 'Tom'), (2, 'John');

create or replace procedure proc_test2(tablename text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        res := (select * from table(:tablename));
        return table(res);
    end;
    $$;

call proc_test2('mytable2');

execute immediate 'call proc_test2(\'mytable2\')';
select * from table(result_scan(last_query_id()));
--------------

use role accountadmin;
select *

 from table(information_schema.pipe_usage_history(

  date_range_start=>dateadd('day',-14,current_date()),

  date_range_end=>current_date(),

  pipe_name=>'mypipe'));

select * from snowflake.account_usage.pipe_usage_history;

use role sysadmin;
show stages;
list @feng_stage;
create or replace stage feng_s3_stage
  url = 's3://feng-public-bucket';
list @feng_s3_stage;

create or replace pipe mypipe
auto_ingest = true
as
copy into mytable from @feng_s3_stage/the_oscar_award.csv
file_format=(type=csv field_delimiter=',' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

describe pipe mypipe;




CREATE OR REPLACE pipe feng_database.feng_schema.flight_data_snowpipe
auto_ingest = TRUE
AS
COPY into feng_database.feng_schema.flight2015_staging_2
    from @feng_database.feng_schema_for_stage.feng_aws_s3_integration_stage
    file_format=(type=csv field_delimiter=',' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


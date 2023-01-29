create or replace procedure sleep_3600s_timeout ()
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import time

def run(session):
  time.sleep(3700)
  return 'slept 3700s'
$$;

call sleep_3600s_timeout();

-- " Can't parse '2013-10-15 12:57:59.000' as timestamp with format 'yyyy-mm-dd hh:mi:ss' "
-- solution
select to_timestamp('2013-10-15 12:57:59.000','yyyy-mm-dd hh24:mi:ss.ff');



select to_timestamp('12/13/2022 12:37:24 PM', 'MM/DD/YYYY HH24:MI:SS AM');

create or replace table mytable5 (sampling_time text);
insert into mytable5 values ('12/13/2022 12:37:24 PM');

select to_timestamp(sampling_time, 'MM/DD/YYYY HH24:MI:SS AM') from mytable5;

----
show functions like '%timezone%';
select current_time();

show parameters like '%timezone%';

alter session set timezone='America/Los_Angeles'; -- This is the default value
select current_time();
use role accountadmin;
select max(usage_date) from snowflake.account_usage.stage_storage_usage_history;
-- 2023-01-26
 
alter session set timezone='UTC';
select current_time();
select max(usage_date) from snowflake.account_usage.stage_storage_usage_history;
-- 2023-01-27

create or replace procedure sleep_3600s_timeout ()
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import time

def run(session):
  time.sleep(3700)
  return 'slept 3700s'
$$;

call sleep_3600s_timeout();

-- " Can't parse '2013-10-15 12:57:59.000' as timestamp with format 'yyyy-mm-dd hh:mi:ss' "
-- solution
select to_timestamp('2013-10-15 12:57:59.000','yyyy-mm-dd hh:mi:ss.ff'), create or replace procedure sleep_3600s_timeout ()
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import time

def run(session):
  time.sleep(3700)
  return 'slept 3700s'
$$;

call sleep_3600s_timeout();

show stages;
describe stage my_internal_stage;
select * from INFORMATION_SCHEMA.STAGES;

-- " Can't parse '2013-10-15 12:57:59.000' as timestamp with format 'yyyy-mm-dd hh:mi:ss' "
-- solution
select to_timestamp('2013-10-15 12:57:59.000','yyyy-mm-dd hh24:mi:ss.ff'), to_timestamp('2013-10-15 12:57:59.000','yyyy-mm-dd hh:mi:ss.ff');

alter session set timezone='UTC';
select * from snowflake.account_usage.stage_storage_usage_history order by usage_date desc limit 3;

alter session set timezone='America/Toronto'; 
select * from snowflake.account_usage.stage_storage_usage_history order by usage_date desc limit 3;

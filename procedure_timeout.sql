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

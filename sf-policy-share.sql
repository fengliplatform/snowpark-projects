
-- provider
use role accountadmin;
CREATE MANAGED ACCOUNT reader_acct1
    ADMIN_NAME = admin_user , ADMIN_PASSWORD = 'Admin12345' ,
    TYPE = READER;
drop managed account reader_acct1;

use role sysadmin;
create or replace table fengdb.public.policy_test_tbl (id int, name varchar);
insert into policy_test_tbl values (1, 'John'), (2, 'Tom');
select * from fengdb.public.policy_test_tbl;

use role useradmin;
create or replace role policyadmin;
grant role policyadmin to role sysadmin;
use role securityadmin;
GRANT CREATE MASKING POLICY on SCHEMA fengdb.public to ROLE policyadmin;
use role accountadmin;
GRANT APPLY MASKING POLICY on ACCOUNT to ROLE policyadmin;
grant usage on database fengdb to role policyadmin;
grant usage on schema fengdb.public to role policyadmin;


use role sysadmin;
grant usage on database fengdb to role dbadmin;
grant usage on schema fengdb.public to role dbadmin;
grant select on table fengdb.public.policy_test_tbl to role dbadmin;
grant usage on warehouse compute_wh to role dbadmin;






use role policyadmin;
CREATE or replace MASKING POLICY fengdb.public.name_masking_policy AS
(val STRING) RETURNS STRING ->
CASE
   WHEN CURRENT_ROLE() = 'DBADMIN' THEN val
   WHEN CURRENT_ROLE() = 'SYSADMIN' THEN regexp_replace(val,'.','*',2)
   ELSE '*** REDACTED ***'
END;

show masking policies;
desc masking policy fengdb.public.name_masking_policy;

use role sysadmin;
show masking policies;
desc masking policy fengdb.public.name_masking_policy2;
ALTER MASKING POLICY fengdb.public.name_masking_policy2 SET BODY ->
  CASE
    WHEN current_role() IN ('ANALYST') THEN VAL
    ELSE sha2(val, 512)
  END;

use role accountadmin;
show masking policies;
desc masking policy fengdb.public.name_masking_policy;
alter masking policy fengdb.public.name_masking_policy rename to fengdb.public.name_masking_policy2;


use role policyadmin;
alter table fengdb.public.policy_test_tbl modify column name set masking policy fengdb.public.name_masking_policy;

alter table fengdb.public.policy_test_tbl modify column name unset masking policy;

-- row access policy

use role sysadmin;
GRANT CREATE row access POLICY on SCHEMA fengdb.public to ROLE policyadmin;
use role accountadmin;
GRANT apply row access POLICY on account to ROLE policyadmin;

use role policyadmin;
create row access policy fengdb.public.name_row_access_policy as (name varchar) returns boolean ->
  case
    when name = 'John' then true
    else false
  end;

alter table fengdb.public.policy_test_tbl add row access policy fengdb.public.name_row_access_policy on (name);

select * from fengdb.public.policy_test_tbl;



-- use role accountadmin;
-- grant ownership on warehouse compute_wh to role sysadmin revoke current grants;

use role dbadmin;
select * from fengdb.public.policy_test_tbl;

use role accountadmin;
select * from fengdb.public.policy_test_tbl;

use role sysadmin;
select * from fengdb.public.policy_test_tbl;

use role securityadmin;
select * from fengdb.public.policy_test_tbl;


use role sysadmin;
alter table fengdb.public.policy_test_tbl modify column name unset masking policy;
drop masking policy fengdb.public.name_mask;
select * from fengdb.public.policy_test_tbl;
-- success


---------------------
use role accountadmin;
CREATE SHARE policy_test_tbl_share;
drop share policy_test_tbl_share;

grant usage on database fengdb to share policy_test_tbl_share;
grant usage on schema fengdb.public to share policy_test_tbl_share;
grant select on table fengdb.public.policy_test_tbl to share policy_test_tbl_share;

ALTER SHARE policy_test_tbl_share SET ACCOUNTS=BTB96759;

show grants to share policy_test_tbl_share;
select current_account();



-----------------


-- create policy and check if sysadmin can see it.



----
use role useradmin;
create or replace role dbadmin;
grant role dbadmin to role sysadmin;

use role sysadmin;
grant usage on database fengdb to role dbadmin;
grant usage on schema fengdb.public to role dbadmin;
grant select on table fengdb.public.policy_test_tbl to role dbadmin;
grant usage on warehouse compute_wh to role dbadmin;

------------------

-- consumer
create or replace warehouse reader_wh WITH WAREHOUSE_SIZE='XSMALL';

use role accountadmin;
show shares;

--use role sysadmin;
create database fengdb_consumer from share DILUXZQ.KIB05025.POLICY_TEST_TBL_SHARE;
select * from fengdb_consumer.public.policy_test_tbl;

use role accountadmin;
grant imported privileges on database fengdb_consumer to role sysadmin;
use role sysadmin;
select * from fengdb_consumer.public.policy_test_tbl;


use role useradmin;
create or replace role dbadmin;
grant role dbadmin to role sysadmin;
use role accountadmin;
GRANT IMPORTED PRIVILEGES ON DATABASE fengdb_consumer TO ROLE dbadmin;

use role dbadmin;
select * from fengdb_consumer.public.policy_test_tbl;



use role sysadmin;
grant usage on warehouse reader_wh to role dbadmin;







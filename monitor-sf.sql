--
select login_name, last_success_login from "SNOWFLAKE"."ACCOUNT_USAGE"."USERS"
    where datediff(day, last_success_login, current_timestamp()) > 6;

show grants to role sysadmin;

select * from snowflake.account_usage.data_transfer_history;
select * from snowflake.account_usage.grants_to_roles;
select * from snowflake.account_usage.grants_to_users;

select user_name, event_timestamp from "SNOWFLAKE"."ACCOUNT_USAGE"."LOGIN_HISTORY" 
WHERE REPORTED_CLIENT_TYPE = 'SNOWFLAKE_UI' 
and datediff(hour, event_timestamp, current_timestamp()) < 24;

select login_name, created_on from "SNOWFLAKE"."ACCOUNT_USAGE"."USERS"
    where not disabled and HAS_PASSWORD;
select * from "SNOWFLAKE"."ACCOUNT_USAGE"."USERS";


select query_id, user_name, start_time, error_code from "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" 
    where  ERROR_CODE is not null 
    and datediff(hour, start_time, current_timestamp()) < 1;

----

SELECT f.value:objectName, USER_NAME, MAX (QUERY_START_TIME) as LAST_ACCESSED_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY, TABLE (FLATTEN (BASE_OBJECTS_ACCESSED)) f
where f.value:objectDomain = 'Table'
group by 1,2;

select * from SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY;

--

-- Feb 9
-- use case 4

SELECT f.value:objectName as table_name, USER_NAME, MAX (QUERY_START_TIME) as LAST_ACCESSED_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY, TABLE (FLATTEN (BASE_OBJECTS_ACCESSED)) f
where f.value:objectDomain = 'Table' 
group by 1,2
having datediff(day, LAST_ACCESSED_TIME, current_timestamp()) > 365;


SELECT f.value:objectName as table_name, USER_NAME, MAX (QUERY_START_TIME) as LAST_ACCESSED_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY, TABLE (FLATTEN (BASE_OBJECTS_ACCESSED)) f
where f.value:objectDomain = 'Table' 
group by 1,2
having datediff(day, LAST_ACCESSED_TIME, current_timestamp()) > 30;


-- use case 3: write changes to table/view?
-- write changes: DML: insert, delete, modify, DDL: alter
select * from snowflake.account_usage.query_history 
    where (contains(query_text, 'insert') or
    contains(query_text, 'delete') or
    contains(query_text, 'modify') or
    contains(query_text, 'alter') )
    and datediff(hour, start_time, current_timestamp()) < 1000
    order by start_time desc;
    

-- user case 4: how long is a table being accessed last time?
-- table has created_on column in show tables.
--

-- use case 5: data movement from prod to uat, dev...


-- User case 2: If the user/service account got added or deleted
create user user1_usercase2;

select * from snowflake.account_usage.query_history 
    where query_type = 'CREATE_USER' limit 1;
    
select start_time, user_name, query_text, database_name from snowflake.account_usage.query_history 
    where query_type = 'CREATE_USER' and datediff(day, start_time, current_timestamp()) < 1
    order by start_time desc limit 5;


select start_time, user_name, query_text, database_name from snowflake.account_usage.query_history 
    where contains(query_text, 'create user') and datediff(day, start_time, current_timestamp()) < 1
    order by start_time desc limit 10;
    

select count(*) from snowflake.account_usage.query_history;
-- 158k row

drop user user1_usercase2;
select * from snowflake.account_usage.query_history where query_id='01aa33a9-0602-ffbd-003a-73030085311a';

select start_time, user_name, query_text, database_name from snowflake.account_usage.query_history 
    where query_type = 'DROP_USER' and datediff(day, start_time, current_timestamp()) < 1
    order by start_time desc limit 5;

select start_time, user_name, query_text, database_name from snowflake.account_usage.query_history 
    where contains(query_text, 'drop user') and datediff(day, start_time, current_timestamp()) < 1
    order by start_time desc limit 10;


--Feb 8
--
select * from snowflake.account_usage.replication_group_refresh_history;

-- Feb 7
-- Use case 1: if there are failed login?
select * from snowflake.account_usage.login_history 
    where is_success = 'NO'
    order by event_timestamp desc
    limit 5;

select * from snowflake.account_usage.login_history 
    where is_success = 'NO' and datediff(hour, event_timestamp, current_timestamp()) < 1000
    order by event_timestamp desc
    limit 5;

select count(*) from snowflake.account_usage.login_history 
    where is_success = 'NO' and datediff(hour, event_timestamp, current_timestamp()) < 1000
    order by event_timestamp desc
    limit 5;
    
-- demo table
create or replace table login_history_2 as
    select * from snowflake.account_usage.login_history order by event_timestamp desc limit 50;

select * from login_history_2;

select user_name, count(*) as number_failed_login
    from login_history_2
    group by user_name;
--    where is_success = 'NO' and datediff(hour, event_timestamp, current_timestamp()) < 1000;

with failed_logins (user_name, number_failed_login) as (
 select user_name, count(*) as number_failed_login
    from login_history_2
    group by user_name)
select user_name, number_failed_login from failed_logins where number_failed_login > 10;

--------- Step 1: get user name who failed 5+ login in past 1 hour
---------
with failed_logins (user_name, number_failed_login) as (
 select user_name, count(*) as number_failed_login
    from snowflake.account_usage.login_history
    where is_success = 'NO' and datediff(hour, event_timestamp, current_timestamp()) < 1000
    group by user_name)
select user_name, number_failed_login from failed_logins where number_failed_login > 1;
---------
with failed_logins (user_name, number_failed_login) as (
 select user_name, count(*) as number_failed_login
    from snowflake.account_usage.login_history
    where is_success = 'YES' and datediff(hour, event_timestamp, current_timestamp()) < 100
    group by user_name)
select user_name, number_failed_login from failed_logins where number_failed_login > 5;
---------

--------- step 2, get other user info
set past_hours = 1000;
with failed_logins (user_name, client_type, number_failed_login) as (
 select user_name, listagg(distinct reported_client_type, ',') as client_type, count(*) as number_failed_login
    from snowflake.account_usage.login_history
    where is_success = 'NO' and datediff(hour, event_timestamp, current_timestamp()) < $past_hours
    group by user_name)
select current_timestamp(), $past_hours as In_Past_Hours, user_name, client_type, number_failed_login 
    from failed_logins 
    where number_failed_login > 1;

--
select count(*) from mytable;
SELECT * from snowflake.account_usage.query_history where warehouse_id is null;
ALTER SESSION SET USE_CACHED_RESULT = true;

SELECT query_text, warehouse_id, execution_status from snowflake.account_usage.query_history 
    where warehouse_id is null and contains(query_text, 'count');
    
--
select distinct query_type from snowflake.account_usage.query_history;

select * from snowflake.account_usage.query_history where query_type='INSERT' limit 3;
select * from snowflake.account_usage.query_history where query_type='UPDATE' limit 3;
select * from snowflake.account_usage.query_history where contains(query_type,'ALTER_') limit 3;

select start_time, user_name, database_name, schema_name, query_text from snowflake.account_usage.query_history 
    where database_name ='FENGDB' and 
    (contains(query_type, 'ALTER_') or query_type = 'INSERT' or query_type = 'UPDATE') 
    and datediff(hour, start_time, current_timestamp()) < 10
    order by start_time desc;

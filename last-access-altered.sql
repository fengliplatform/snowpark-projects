SELECT f.value:objectName, USER_NAME, MAX (QUERY_START_TIME) as LAST_ACCESSED_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY, TABLE (FLATTEN (BASE_OBJECTS_ACCESSED)) f
where f.value:objectDomain = 'Table'
group by 1,2;

select * from SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY;

--
-- Even though the Snowflake documentation says that INFORMATION_SCHEMA.TABLES.LAST_ALTERED returns 
-- the latest DDL or DML, it doesn't for TABLE_TYPE='VIEW'. For TABLE_TYPE='VIEW' it only returns the last DDL date.
select table_catalog, table_schema, table_name, last_altered from fengdb.INFORMATION_SCHEMA.TABLES;


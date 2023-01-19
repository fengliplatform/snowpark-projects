-- get number of rows affected by a SQL statement
create or replace procedure TEST_ROW_COUNT("P_SCRIPT" VARCHAR(16777216))
returns varchar
language sql
as
 declare
 
  vAffectedRowCount integer;
 
begin 
 
 -- Update the rows in a table that have values less than 3.
 EXECUTE immediate :P_SCRIPT;
 -- SQLFOUND returns 'true' if the last DML statement
 -- (the UPDATE statement) affected one or more rows.
 if (sqlfound  = true) then
  vAffectedRowCount := sqlrowcount;
  return 'Updated ' || vAffectedRowCount || ' rows.';
 -- SQLNOTFOUND returns 'true' if the last DML statement
 -- (the update statement) affected zero rows.
 elseif (sqlnotfound  = true) then
  return 'No rows updated.';
 else
  -- return 'SQLFOUND and SQLNOTFOUND are not true.';
  return sqlrowcount;
 end if;
 
end;

call TEST_ROW_COUNT('update fengdb.fengschema.pay set country=\'CA\' where country=\'Canada\'');
call TEST_ROW_COUNT('insert into fengdb.fengschema.pay values (\'Teacher\', 100000, \'CA\')');
call TEST_ROW_COUNT('delete from fengdb.fengschema.pay where job_name=\'Teacher\'');

select * from fengdb.fengschema.pay;


-- https://docs.snowflake.com/en/developer-guide/snowflake-scripting/dml-status.html
execute immediate $$
begin

  -- Insert rows into a table.
  insert into fengdb.fengschema.pay values ('Teacher', 100000, 'CA');
  
  -- SQLROWCOUNT is not affected by statements
  -- that are not DML statements (e.g. SELECT statements).
  select * from fengdb.fengschema.pay;
  
  -- Returns the number of rows affected by
  -- the last DML statement (the INSERT statement).
  return sqlrowcount;

end;
$$;


-- public s3 stage without authentication
create stage like_a_window_into_an_s3_bucket 
 url = 's3://uni-lab-files';
list @like_a_window_into_an_s3_bucket;

--
--https://docs.snowflake.com/en/sql-reference/functions/regexp_substr.html
create table mytable (words text);
insert into mytable values ('word1: words1 words2 words3 word2:');
select words, regexp_substr(words,'word1:(.*)word 2:', 1, 1, 'e', 1) from mytable; 

select words, regexp_substr(words,'word1: (.*) word2:', 1, 1, 'e', 1) as regexp_substr_result,
       NULLIF(replace(regexp_substr_result, '\n', ''), 'None') as final_result

-- copy history
SELECT
  FILE_NAME,
  STAGE_LOCATION,
  LAST_LOAD_TIME,
  ROW_COUNT,
  ROW_PARSED,
  ERROR_COUNT,
  initcap(replace(STATUS, '_', ' '), ''),
  PIPE_NAME,
  PIPE_CATALOG_NAME,
  PIPE_SCHEMA_NAME,
  FILE_SIZE,
  FIRST_ERROR_MESSAGE,
  FIRST_ERROR_LINE_NUMBER,
  FIRST_ERROR_CHARACTER_POS,
  FIRST_ERROR_COLUMN_NAME,
  ERROR_LIMIT,
  PIPE_RECEIVED_TIME,
  TABLE_NAME,
  TABLE_SCHEMA_NAME,
  TABLE_CATALOG_NAME
from
  snowflake.account_usage.copy_history
WHERE
  LAST_LOAD_TIME >= TO_TIMESTAMP_LTZ('2022-12-21T05:00:00.000Z', 'AUTO')
  AND LAST_LOAD_TIME <= TO_TIMESTAMP_LTZ('2023-01-18T05:00:00.000Z', 'AUTO')
ORDER BY
  LAST_LOAD_TIME DESC
LIMIT
  250


--https://docs.snowflake.com/en/sql-reference/functions/regexp_substr.html
create table mytable (words text);
insert into mytable values ('word1: words1 words2 words3 word2:');
select words, regexp_substr(words,'word1:(.*)word 2:', 1, 1, 'e', 1) from mytable;

select words, regexp_substr(words,'word2:(.*)word 2:', 1, 1, 'e', 1) from mytable; 

create table mytable2 (words text);
insert into mytable2 values ('word1: abc word2:');
insert into mytable2 values ('word1: words1 words2 words3 word2:');

select words, regexp_substr(words,'word1: (.*) word2:', 1, 1, 'e', 1) as regexp_substr_result
    case
        when regexp_substr(words,'word1: (.*) word2:', 1, 1, 'e', 1) = 'None' then null
        else regexp_substr(words,'word1: (.*) word2:', 1, 1, 'e', 1)
    end as results
    from mytable2;
    
select words, regexp_substr(words,'word1: (.*) word2:', 1, 1, 'e', 1) as regexp_substr_result,
    case
        when regexp_substr_result = 'None' then null
        else regexp_substr_result
    end as final_result
    from mytable2;

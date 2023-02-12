------------------
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

    
------------------
copy into @json_stage_unload/<file_name>_yyyymmdd_hhmmss.json
from (select object_construct( 'cid', CID, 'CUS_NAME',CUSTOMER_NAME, 'MOB', MOBILE, 'city', city,
    'Address', array_construct(
      object_construct( 'ADDRESS_Type','Home','ADDRESS_1',ADDRESS_1 ),
      object_construct( 'ADDRESS_Type', 'Office','ADDRESS_2',ADDRESS_2)))
                         from customer_stream_t )
    file_format = (type = json);

create or replace table customer (cid int, name text, address_1 text, address_2 text);
insert into customer values (1, 'Tom', '1 Front St', '32 Steeles Ave'),
                            (2, 'John', '3 Main St', '555 Don Mills');

select object_construct( 'cid', cid, 'CUS_NAME',name,
                         'Address', array_construct(
                                       object_construct( 'ADDRESS_Type','Home','ADDRESS_1',address_1 ),
                                       object_construct( 'ADDRESS_Type', 'Office','ADDRESS_2',address_2)))
      from customer;

select object_construct( 'cid', cid, 'CUS_NAME',name,
                         'Address', array_construct(
                                       object_construct( 'ADDRESS_1',address_1 ),
                                       object_construct( 'ADDRESS_2',address_2)))
      from customer;

create or replace table JSON_Test_Table (uuid text, campaign text);
insert into JSON_Test_Table values ('fe881781-bdc2-41b2-95f2-e0e8c19dc597', 'Welcome_New'),
                                   ('77a41c02-beb9-48bf-ada4-b2074c1a78cb', 'Welcome_Existing');
SELECT object_construct(
     'UUID',UUID
     ,'CAMPAIGN',CAMPAIGN)
     FROM JSON_Test_Table
     LIMIT 2;
SELECT array_construct(object_construct(
 'UUID',UUID
 ,'CAMPAIGN',CAMPAIGN))
 FROM JSON_Test_Table
 LIMIT 2;
SELECT object_construct('Campaign',
                         array_construct( object_construct('UUID',UUID),
                                         object_construct('CAMPAIGN',CAMPAIGN))
                        )
 FROM JSON_Test_Table
 LIMIT 2;
 
----
create or replace table JSON_Test_Table (uuid text, campaign text);
insert into JSON_Test_Table values ('fe881781-bdc2-41b2-95f2-e0e8c19dc597', 'Welcome_New'),
                                   ('77a41c02-beb9-48bf-ada4-b2074c1a78cb', 'Welcome_Existing');
select array_construct( object_construct('UUID',UUID),
                        object_construct('CAMPAIGN',CAMPAIGN))
FROM JSON_Test_Table
LIMIT 2;


//************ SQL Proc Starts ************ 

Create or replace table Emp_Validation (

  Emp_Key number(1),

  Emp_Code varchar(5),

  Emp_Name varchar(10),

  Emp_State varchar(10)   

);
create or replace procedure copy_file_from_Stage(Target_Table_Name varchar, Stage_Name varchar, Source_File_Name varchar)
returns varchar not null
language sql
Execute as caller
as
$$
declare 
v_truncate_script varchar:='truncate '||:Target_Table_Name;
v_copy_into_script varchar:='copy into '||:Target_Table_Name||' from@'||:Stage_Name||'/'||:Source_File_Name||' on_error=continue';
v_write_errors_script varchar:='create or replace table '||:Target_Table_Name||'_REJECTED As Select * from TABLE(VALIDATE('||:Target_Table_Name||',job_id=>v_copy_into_script_qid))';

begin
  execute immediate :v_truncate_script;   
  execute immediate :v_copy_into_script;

  LET v_copy_into_script_qid varchar:='select last_query_id()';
  execute immediate :v_write_errors_script;
  return sqlrowcount::varchar;
exception
when other then
return sqlerrm;

end;
$$;
//************ SQL Proc Ends ************
//Calling above proc

call copy_file_from_Stage('AWS.PUBLIC.EMP_VALIDATION','AWS.PUBLIC.EMP_INTERNALSTAGE','Supplier.csv.gz');

list @my_internal_stage;
select $1 from @my_internal_stage/sample.csv;
create or replace file format my_csv_ff
    type = 'csv'
    field_delimiter =';'
    skip_header=0;

select $1 from @my_internal_stage/sample.csv (file_format=>my_csv_ff);

create or replace table my_table (id int, num number, name text, status text, date_str text, flag text);

copy into my_table
    from @my_internal_stage/sample.csv
    file_format = (format_name = my_csv_ff);

select * from my_table;
-- following works
copy into my_table
    from @my_internal_stage/sample.csv on_error=continue;
select last_query_id(); -- 01aa4935-0604-465d-0047-5e07000247f6
Select * from TABLE(VALIDATE(my_table,job_id=>'01aa4935-0604-465d-0047-5e07000247f6'));
create or replace table my_table_error As Select * from TABLE(VALIDATE(my_table,job_id=>'01aa4935-0604-465d-0047-5e07000247f6'));
select * from my_table_error;

--  
create or replace procedure copy_file_from_Stage(Target_Table_Name varchar, Stage_Name varchar, Source_File_Name varchar)
returns varchar not null
language sql
Execute as caller
as
$$
declare
v_copy_into_script_qid varchar:='';
v_truncate_script varchar:='truncate '||:Target_Table_Name;
v_copy_into_script varchar:='copy into '||:Target_Table_Name||' from@'||:Stage_Name||'/'||:Source_File_Name||' on_error=continue';
v_write_errors_script varchar;

begin
  execute immediate :v_truncate_script;   
  execute immediate :v_copy_into_script;

  v_write_errors_script :='create or replace table '||:Target_Table_Name||'_REJECTED As Select * from TABLE(VALIDATE('||:Target_Table_Name||',job_id=>\''||last_query_id()||'\'))';
  execute immediate :v_write_errors_script;
  return 'success';
end;
$$;

call copy_file_from_Stage('my_table','my_internal_stage','sample.csv');
select * from my_table_rejected;


create or replace table my_table_REJECTED As Select * from TABLE(VALIDATE(my_table,job_id=>'01aa492f-0604-465d-0047-5e07000247ba'));

create or replace table my_table_REJECTED As Select * from TABLE(VALIDATE(my_table,job_id=>'01aa4925-0604-465c-0047-5e0700025742'));
select * from snowflake.account_usage.query_history where query_id='01aa4925-0604-465c-0047-5e0700025742';
-- create or replace procedure

create or replace table my_table_REJECTED As Select * from TABLE(VALIDATE(my_table,job_id=>''));

create or replace table my_table_REJECTED As Select * from TABLE(VALIDATE(my_table,job_id=>'01aa48d5-0604-465c-0047-5e07000256ae'));
select * from snowflake.account_usage.query_history where query_id='01aa48d5-0604-465c-0047-5e07000256ae';
-- copy into table


execute immediate 'select * from my_table';
-- 01aa48e3-0604-4948-0047-5e0700034186
select last_query_id();


create or replace procedure my_sproc ()
returns varchar not null
language sql
Execute as caller
as
$$
begin
execute immediate 'select status from my_table';
return last_query_id();
end;

$$;
call my_sproc();
select * from my_table;

EXECUTE IMMEDIATE $$
BEGIN
  INSERT INTO my_values VALUES (1), (2), (3);
  RETURN SQLROWCOUNT;
END;
$$;
EXECUTE IMMEDIATE $$
BEGIN
  execute immediate 'INSERT INTO my_values VALUES (1), (2), (3)';
  RETURN SQLROWCOUNT;
END;
$$;


create or replace procedure my_sproc3 ()
returns varchar not null
language sql
Execute as caller
as
$$
begin
execute immediate 'insert into my_table values (1, 222, \'Tom\', \'active\', \'2023-10\', \'a\')';
return sqlrowcount;
end;

$$;
call my_sproc3();
select * from my_table;

;
--
create or replace procedure copy_file_from_Stage(Target_Table_Name varchar, Stage_Name varchar, Source_File_Name varchar)

returns varchar not null

language sql 

Execute as caller

as

declare 

 

v_truncate_script varchar:='truncate '||:Target_Table_Name;

 

v_copy_into_script varchar:='copy into '||:Target_Table_Name||' from@'||:Stage_Name||'/'||:Source_File_Name||' on_error=continue';

 

begin

  execute immediate :v_truncate_script;  

   

  execute immediate :v_copy_into_script;

 

  Let Reject_Table_Name varchar := :Target_Table_Name||'_REJECTED'; 

   

  create or replace table IDENTIFIER(:Reject_Table_Name) As Select * from 

  TABLE(VALIDATE(IDENTIFIER(:Target_Table_Name),job_id=>'_last'));

   

  return sqlrowcount::varchar;

 

exception

when other then

return sqlerrm;

end;

//************ SQL Proc Ends ************ 

 

call copy_file_from_Stage('AWS.PUBLIC.EMP_VALIDATION','AWS.PUBLIC.EMP_INTERNALSTAGE','Supplier.csv.gz');

 

Select * from AWS.PUBLIC.EMP_VALIDATION; -- Ran successfully, I got the correct records in this table

Select * from AWS.PUBLIC.EMP_VALIDATION_REJECTED; ---- Ran successfully, I got the errored records in this table



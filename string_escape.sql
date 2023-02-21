
desc table mytable3;
------ snowflake d;bt quick start
------ https://quickstarts.snowflake.com/guide/data_engineering_with_snowpark_python_and_dbt/index.html?index=..%2F..index#0
create database demo_db;
create schema demo_db.demo_schema;

show terse schemas in database DEMO_DB;
show terse objects in DEMO_DB.DEMO_SCHEMA;
----

CREATE OR REPLACE PROCEDURE sample_prc (in_tgt_schema varchar,in_tgt_table varchar)
RETURNS TABLE()
LANGUAGE
SQL
AS
$$
DECLARE
tgt_table varchar := :in_tgt_schema || '.' || :in_tgt_table;
res RESULTSET default (SHOW PRIMARY KEYS IN mytable3); --- THIS IS WHERE I NEED HELP
pk_cursor CURSOR for res;
BEGIN
FOR pk IN pk_cursor do
v_join:= v_join|| pk."column_name";
END FOR;
RETURN TABLE(res);
END;
$$;


create or replace table mytable3 (id int primary key, name text);
insert into mytable3 values (1, 'Tom'), (2, 'John');
show primary keys in mytable3;

select * from table(result_scan(last_query_id()));

set tablename = 'mytable3';
select $tablename;

execute immediate 'show primary keys in table($tablename)';
execute immediate 'select * from table($tablename)';

--
        --res := (show primary keys in table(:tablename));
        --res := (select * from table(:tablename));
create or replace table mytable4 (id int, name text);
insert into mytable4 values (1, 'Tom'), (2, 'John');
create or replace procedure proc_test4(schema_name text, table_name text)
    returns table()
    language sql
    as
    $$
    declare
        full_table_name text:= :schema_name||'.'||:table_name;
        res resultset;
    begin
        res := (select * from table(:full_table_name));
        return table(res);
    end;
    $$;
call proc_test4('demo_schema','mytable3');
;;;;

------------------
create or replace procedure proc_test5(schema_name text, table_name text)
    returns table()
    language sql
    as
    $$
    declare
        full_table_name text:= :schema_name||'.'||:table_name;
        res resultset;
    begin
        execute immediate 'show primary keys in '||:full_table_name;
        res := (select * from table(result_scan(last_query_id())));
        return table(res);
    end;
    $$;
call proc_test5('demo_schema', 'mytable3');
------------------
create or replace procedure proc_test(table_name text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        res := (select * from table(:table_name));
        return table(res);
    end;
    $$;
call proc_test('demo_db.demo_schema.mytable3');



---------

create or replace procedure proc_test(tablename text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        res := (show primary keys in table(:tablename));
        --res := (select * from table(:tablename));
        return table(res);
    end;
    $$;
call proc_test('mytable3');

create or replace procedure proc_test2(tablename text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        execute immediate 'show primary keys in '||:tablename;
        res := (select * from table(result_scan(last_query_id())));
        return table(res);
    end;
    $$;
call proc_test2('mytable3');
select "table_name", "column_name" from table(result_scan(last_query_id()));

create user feng_user password='Password123' default_role=sysadmin;
grant role sysadmin to user feng_user;

show terse schemas;

show terse users like '%feng%';
select name, "email" from table(result_scan(last_query_id())) where "email" = '';

list @fengdb.public.my_internal_stage;
select "name", "size" from table(result_scan(last_query_id()));



create or replace procedure show_users(username text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        execute immediate 'show users like \'%'||:username||'%\'';
        res := (select * from table(result_scan(last_query_id())));
        return table(res);
    end;
    $$;
call show_users('feng');




select * from table($tablename);



set myname = 'name';
select '$myname' from mytable3;



----
CREATE TASK t1
  SCHEDULE = '60 MINUTE'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);

create or replace procedure my_insert()
    returns text
    language sql
    as
    $$
    begin
    insert into TASK_TABLE_2 values ('2_1',SYSDATE());
    insert into TASK_TABLE_2 values ('2_2',SYSDATE());
    insert into TASK_TABLE_3 values ('3_1',SYSDATE());
    end;
    $$;
    

CREATE OR REPLACE TABLE TASK_TABLE_2(TBL_NAME VARCHAR, LAST_INSERTED_DATE TIMESTAMP);
CREATE OR REPLACE TABLE TASK_TABLE_3(TBL_NAME VARCHAR, LAST_INSERTED_DATE TIMESTAMP) ;
create or replace task DEMO_TASK_2

USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE ='1 minutes'
as
call my_insert()
;

show tasks;
alter task DEMO_TASK_2 resume;

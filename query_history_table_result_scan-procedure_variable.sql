--
create or replace procedure create_table_sproc (brand_name text)
returns varchar
language sql
as
$$
begin
     execute immediate 'create or replace table '|| :brand_name || '_table (id int)';
     return 'success';
end;
$$;

call create_table_sproc('brand1');

show tables like '%brand1%';

--

create or replace table source (id int,col1 int, col2 int) as
    select 1,101, 102
    union
    select 2,201, 202;
create or replace table target(id int, col1 int) as
    select 1,101
    union
    select 2,201;
create or replace stream my_stream on table source;
update source set col1 = 103 where id=1;
select * from my_stream;

merge into target t
    using (select id, col1, metadata$action as a, metadata$isupdate as i, metadata$row_id as r from my_stream) s
    on t.id = s.id
    when matched and s.a = 'INSERT' and s.i = 'TRUE' then
        update set t.col1 = s.col1;

select * from source;
select * from target;

--
create or replace temporary function split_sum(ids varchar(200))
 returns float
 LANGUAGE SQL
 as
 $$
  select sum(value) from table(SPLIT_TO_TABLE(ids, ','))
$$;

create or replace temporary table tmp_t1 as 
select 
 '167536,172850,172847,172836,16767' ids2
,'167536,172850,172847,172836,1676' ids3
,'1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7' ids4
,'1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,60' ids5
;

select sum(value) from table(SPLIT_TO_TABLE('1,2,3,4', ','));
select * from table(SPLIT_TO_TABLE('1,2,3,4', ','));


select * from tmp_t1;

select split_sum(t.ids2) from tmp_t1 t; --error Unsupported subquery type cannot be evaluated, ids2 has 33 chars
select * from table(get_query_operator_stats(last_query_id()));
-- 01aa4016-0604-45fa-0047-5e0700026412
select t.ids2 from tmp_t1 t;
select * from table(SPLIT_TO_TABLE('167536,172850,172847,172836,16767', ',')); --error Unsupported subquery type cannot be evaluated, ids2 has 33 chars
select  sum(value) from table(SPLIT_TO_TABLE('167536,172850,172847,172836,16767', ','));

select split_sum(t.ids3) from tmp_t1 t; --working, ids3 has 32 chars
select * from table(SPLIT_TO_TABLE('167536,172850,172847,172836,1676', ','));

select split_sum(t.ids4) from tmp_t1 t; --error Unsupported subquery type cannot be evaluated, ids4 has 33 chars
select * from table(SPLIT_TO_TABLE('1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17', ','));
select sum(value) from table(SPLIT_TO_TABLE('1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17', ','));

select split_sum(t.ids5) from tmp_t1 t;--working, ids5 has 32 chars

 

select split_sum('167536,172850,172847,172836,167671,167659,167658,167648,167571,167570,167562,167543,172939,154277,154161,154155,154147') always_working,

split_sum(t.ids3) from tmp_t1 t; -- always working even the hardcoded string is very long

--
create or replace function split_sum2(ids varchar(200))
 returns float
 LANGUAGE SQL
 as
 $$
  select sum(value) from table(SPLIT_TO_TABLE(ids, ','))
$$;

select split_sum2(t.ids2) from tmp_t1 t; --error Unsupported subquery type cannot be evaluated
--
create or replace procedure split_sum_sproc(ids varchar(200))
 returns float
 LANGUAGE SQL
 as
 $$
 declare 
   my_sum float;
 begin
  select sum(value) into :my_sum from table(SPLIT_TO_TABLE(:ids, ','));
  return my_sum;
  end;
$$;
call split_sum_sproc('167536,172850,172847,172836,167671,167659,167658,167648,167571,167570,167562,167543,172939,154277,154161,154155,154147'); 


create or replace procedure split_sum_sproc2(ids varchar(200))
 returns float
 LANGUAGE SQL
 as
 $$
 declare 
   my_sum float;
 begin
  my_sum := (select sum(value) from table(SPLIT_TO_TABLE(:ids, ',')));
  return my_sum;
  end;
$$;
call split_sum_sproc2('167536,172850,172847,172836,167671,167659,167658,167648,167571,167570,167562,167543,172939,154277,154161,154155,154147'); 



--
-- https://docs.snowflake.com/en/developer-guide/snowflake-scripting/variables.html
DECLARE
  id_variable INTEGER;
  name_variable VARCHAR;
BEGIN
  SELECT id, name INTO :id_variable, :name_variable FROM some_data WHERE id = 1;
  RETURN id_variable || ' ' || name_variable;
END;

--
create or replace procedure myprocedure()
  returns int
  language sql
  as
  $$
    DECLARE
        status string;
    BEGIN
        CREATE TABLE IF NOT EXISTS TEST(i int);
        status := (SELECT "status" FROM table(result_scan(last_query_id())));
        
        IF (status LIKE '%already exists%') THEN
            RETURN 1;
        ELSE
            RETURN 2;
        END IF;
    END;
$$;

--
UNSET todays_amount;
call todays_delivery_amount('10');
set todays_amount = (select TODAYS_DELIVERY_AMOUNT from table(result_scan(last_query_id())));

select name from student where id=1;
select * from table(result_scan(last_query_id()));


select *
    from table(get_query_operator_stats('01aa4016-0604-45fa-0047-5e0700026412'));
select *
    from table(get_query_operator_stats(last_query_id()));

--
list @my_internal_stage;
copy into @my_internal_stage/student.csv 
    from student;
select query_text, query_type, outbound_data_transfer_cloud, outbound_data_transfer_region, outbound_data_transfer_bytes
    from snowflake.account_usage.query_history where query_type='UNLOAD' order by start_time desc;
    

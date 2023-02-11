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

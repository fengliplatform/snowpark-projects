select SYSTEM$GET_PRIVATELINK_CONFIG;

select key, value from table(flatten(input=>parse_json(system$get_privatelink_config())));

select current_ip_address();

--
create or replace procedure tst(schema_name string,src_table_name string,tgt_table_name string)
returns string
language sql
as
$$
declare
    ins_stmt1 string;
begin
    if(tgt_table_name = 'CDH_GW_CLM_AUD') 
    then
      return 'tgt_table_name = "CDH_GW_CLM_AUD"';
    end if;
end;
$$
;
call tst('schema','src_tbl','CDH_GW_CLM_AUD');

----

// Create your table
create table sometable as      
select someint
      ,somename
  from values
      (0, 'bob')
     ,(1, 'pat')
     ,(2, 'ted')
     ,(3, 'jim')
     ,(4, 'sid')
     ,(5, 'ken')
   as tbl(someint
         ,somename)
;
 
// Test out the use of an array in plain sql:
select tbl.someint 
  from sometable tbl
  join table(flatten(['pat','bob','ken','sid','jim'])) arr
    on tbl.somename = arr.value
;
 
// Create your function          
create or replace function myfunc(somenames array)
returns table(someint integer)
as
$$
    select tbl.someint
      from sometable                   tbl
      join table(flatten(somenames))   arr
        on tbl.somename              = arr.value
$$
;
 
// And call it...
select t.someint
  from table(myfunc(['pat','bob','ken','sid','jim'])) t
 order by
       t.someint
;




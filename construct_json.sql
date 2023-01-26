-- how many days left for a trial account?
SELECT
DATEDIFF(D,CURRENT_DATE(),END_DATE - 1) DAYS_LEFT_IN_FREE_TRIAL
FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
;

-- number of views by database and schema
 select table_catalog as database_name,table_schema as schema_name,count(table_name) as view_count 
 from snowflake.account_usage.views 
 where deleted is null group by 1,2
 order by 1,2; 
 
 
select count (*) from demo_db.information_schema.tables
where table_schema <> 'INFORMATION_SCHEMA'
and table_type = 'MATERIALIZED VIEW';

--
create or replace table fengdb.fengschema.JSON_Test_Table (uuid text, campaign text, focal text);
insert into fengdb.fengschema.JSON_Test_Table values ('111222', 'welcome new customer', '{first_name: Tom, last_name: Martins}'),
                                                  ('333444', 'reward existing customer', '{first_name: John, last_name: Smith}');
SELECT object_construct(
 'UUID',UUID
 ,'CAMPAIGN',CAMPAIGN
,'focal', focal)
 FROM fengdb.fengschema.JSON_Test_Table
 LIMIT 2;
 
-- array_construct
 
 SELECT array_construct(object_construct(
 'UUID',UUID
 ,'CAMPAIGN',CAMPAIGN))
 FROM fengdb.fengschema.JSON_Test_Table
 LIMIT 2;
 
 select * from fengdb.fengschema.JSON_Test_Table;
 
--
create or replace table fengdb.fengschema.JSON_Test_Table2 (uuid variant, campaign variant);
insert into fengdb.fengschema.JSON_Test_Table2 values (to_variant('111222'), to_variant('welcome new customer')),
                                                  (to_variant('333444'), to_variant('reward existing customer'));
                                                  
                                                  
--
create or replace table mytable4 (id int, name text, age int);
insert into mytable4 values (1, 'Tom', 21),
                            (2, null, 19);
select * from mytable4;

select * from mytable4 WHERE name is null;
select * from mytable4 WHERE IFNULL(name, '') = '';
select * from mytable4 WHERE name='Tom';
set myname = 'Tom';
select * from mytable4 WHERE name=$myname;

--
copy into table1(filename, file_row_number, col1, col2)
from (select metadata$filename, metadata$file_row_number, t.$1, t.$2 from @mystage1/data1.csv.gz (file_format => myformat ) t)
on_error=continue ;

----
SELECT TEST, regexp_substr(TEST,'\\([[:alnum:]]+\\)') sbstr FROM;
SELECT TEST, substr(TEST,  position('(', TEST)+1,position(')', TEST) - position('(', TEST)-1) sbstr FROM;
-- , 1, 1, 'e', 1;
SELECT TEST, regexp_substr(TEST, '\(([^)]+)\)') sbstr FROM;
SELECT TEST, regexp_substr(TEST, '\(([^)]+)\)') sbstr FROM

   (

   SELECT '123 TEST_STRING (OLD)' TEST FROM DUAL union all

   SELECT '123 TEST_STRING (OLDEST)' TEST FROM DUAL union all

   SELECT '123 TEST_STRING (OLD-MONKEY)' TEST FROM DUAL union all

   SELECT '123 TEST_STRING (OLDOM)' TEST FROM DUAL union all

   SELECT '123 TEST_STRING (OLDUYGTF)' TEST FROM DUAL union all

   SELECT '123 TEST_STRING (OLDUYTRFDS)' TEST FROM DUAL

   );
   
   
----------------

set my_variable='example';
 
create function myfunc()
  returns varchar
  as
  $$
    select $my_variable
  $$
  ;
  
select myfunc(); --outputs 'example'

------------
   -- where is this wrong?
   
CREATE OR REPLACE FUNCTION calculate_circumference(radius number)
RETURNS number
LANGUAGE SQL
AS
'
begin
  if (radius = 0) THEN
  SET pi = 3.14;
  ELSE
  SET pi = 0;
  END IF;
  RETURN 2 * pi * radius;
end;
';

  DECLARE pi number;

-- subquery
-- https://www.red-gate.com/simple-talk/databases/sql-server/learn/subqueries-in-sql-server/
-- https://docs.snowflake.com/en/user-guide/querying-subqueries.html#correlated-vs-uncorrelated-subqueries

Create temporary table A ( x varchar, Y varchar);
Insert into A values ('Hi', 'Hi'),('Hi', 'Hi');
With cte as (select * from A)
    Select * from cte;
    

create or replace table table1 (time_started timestamp, equipment_tag text);
insert into table1 values
    ('2022-12-02 07:00:00.000':: timestamp, 'tag1'),
    ('2022-12-02 09:04:00.000':: timestamp, 'tag2'),
    ('2022-12-02 13:13:00.000':: timestamp, 'tag3'),
    ('2022-12-02 14:30:00.000':: timestamp, 'tag4');
select * from table1;

create or replace table table2 (float_value float, timestamp_adj timestamp, tag_path text);
insert into table2 values
    (1.2, '2022-12-02 06:00:00.000'::timestamp, 'tag1'),
    (2.2, '2022-12-02 05:00:00.000'::timestamp, 'tag5'),
    (3.2, '2022-12-02 08:00:00.000'::timestamp, 'tag1'),
    (4.2, '2022-12-02 14:00:00.000'::timestamp, 'tag2'),
    (5.2, '2022-12-02 14:00:00.000'::timestamp, 'tag4');
select * from table2;

-- following code give "Unsupported subquery type cannot be evaluated"
with cte_wuc_results as (
    select * from table1
)
select c.time_started, (select top 1 float_value
   from table2
   where timestamp_adj <= c.time_started and tag_path = c.equipment_tag
   Order by timestamp_adj desc) as STATIC_PRESSURE
from cte_wuc_results c;

--
with cte_wuc_results as (
    select * from table1
)
select c.time_started, (select top 1 float_value
   from table2
   where timestamp_adj <= c.time_started and tag_path = c.equipment_tag
   Order by timestamp_adj desc) as STATIC_PRESSURE
from cte_wuc_results c;

-- this code syntax is accepted
with cte_wuc_results as (
    select * from table1
)
select c.time_started, (select top 1 float_value
    from table2
   ) as STATIC_PRESSURE
from cte_wuc_results c;
-- syntax error of subquery with where clause
with cte_wuc_results as (
    select * from table1
)
select c.time_started, (select top 1 float_value
    from table2
    where c.equipment_tag=tag_path
   ) as STATIC_PRESSURE
from cte_wuc_results c;

with cte_wuc_results as (
    select * from table1
);

with cte_wuc_results as (
    select * from table1
)
select c.time_started, (select max(float_value)
   from table2
   where timestamp_adj <= c.time_started and tag_path = c.equipment_tag
   Order by timestamp_adj desc) as STATIC_PRESSURE
from cte_wuc_results c;


-- rewrite it using left join
with cte_wuc_results as (
    select * from table1
)
select c.time_started, t.float_value as STATIC_PRESSURE
from cte_wuc_results c
left join (select * from table2) t
on c.equipment_tag=t.tag_path
where t.timestamp_adj <= c.time_started;

select 
	t1.id,
  a1.v2
from test t1
left join (	  select t2.id, listagg(value2, ',') within group (order by value2) as v2
	  from test1 t2    
    group by t2.id) a1
on (t1.id = a1.id)
;

-- following code runs well in PostgreSQL
with cte_wuc_results as (
    select * from table1
)
select c.time_started, (select float_value
    from table2
    where timestamp_adj <= c.time_started and c.equipment_tag=tag_path
    order by float_value limit 1
   ) as STATIC_PRESSURE
from cte_wuc_results c;

---- subquery in where clause works well. But subquery in select clause may have issues like above tests.
create or replace table pay (name text, annual_wage float, country text);
insert into pay values('DE', 60000, 'CA'), ('SE',80000,'CA'),('DE',90000, 'US'),('SE', 120000, 'US');
create or replace table international_gdp (per_capita_gdp float, name text);
insert into international_gdp values(70000, 'CA'),(100000, 'US');

select p.annual_wage, p.country
  from pay as p
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);
                           
select p.annual_wage, p.country
  from pay as p
  where p.annual_wage < (select per_capita_gdp
                           from international_gdp i
                           where p.country = i.name);

select p.annual_wage, p.country,i.per_capita_gdp
  from pay as p, international_gdp as i 
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);
                           
select p.annual_wage, p.country, (select max(per_capita_gdp) 
                                  from international_gdp i 
                                  where p.country = i.name) as per_capita_gdp
  from pay as p 
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);

select p.annual_wage, p.country,i.per_capita_gdp
  from pay as p 
  left join (select per_capita_gdp,name from international_gdp) i
on p.country=i.name
where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);
  
select 
	t1.id, a1.v2
from test t1
left join (	  select t2.id, listagg(value2, ',') within group (order by value2) as v2
	  from test1 t2    
    group by t2.id) a1
on (t1.id = a1.id);


select p.annual_wage, p.country, (select top 1 per_capita_gdp from international_gdp)
  from pay as p 
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);
                  


------------ new test ---

create or replace table pay (job_name text, annual_wage float, country text);
insert into pay values('Data Engineer', 90000, 'CA'), 
                      ('Software Engineer',110000,'CA'),
                      ('Software Engineer',100000, 'US'),
                      ('Data Engineer', 120000, 'US');
create or replace table international_gdp (per_capita_gdp float, name text);
insert into international_gdp values(100000, 'CA'),(110000, 'US');

--basic subquery
select p.job_name, p.annual_wage, p.country
  from pay as p
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);

-- withou max
select p.job_name, p.annual_wage, p.country
  from pay as p
  where p.annual_wage < (select per_capita_gdp
                           from international_gdp i
                           where p.country = i.name);

-- use top 1 unsupported subquery
select p.job_name, p.annual_wage, p.country
  from pay as p
  where p.annual_wage < (select top 1 per_capita_gdp
                           from international_gdp i
                           where p.country = i.name);

-- use limit 1 unsupported subquery
select p.job_name, p.annual_wage, p.country
  from pay as p
  where p.annual_wage < (select per_capita_gdp
                           from international_gdp i
                           where p.country = i.name
                           limit 1);
                           
-- this ends up cross join - not we want
select p.job_name, p.annual_wage, p.country,i.per_capita_gdp
  from pay as p, international_gdp as i 
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);
                           
-- this works well                           
select p.job_name, p.annual_wage, p.country, (select max(per_capita_gdp) 
                                  from international_gdp i 
                                  where p.country = i.name) as per_capita_gdp
  from pay as p 
  where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);

-- this will fail as top 1, limit 1 is not supported in subquery you need
-- to use aggregation function max, min, avg to return one value from subquery
select p.annual_wage, p.country, (select top 1 per_capita_gdp 
                                  from international_gdp i 
                                  where p.country = i.name) as per_capita_gdp
  from pay as p 
  where p.annual_wage < (select per_capita_gdp
                           from international_gdp i
                           where p.country = i.name
                           limit 1);
                           
-- use join works
select p.job_name, p.annual_wage, p.country,i.per_capita_gdp
  from pay as p 
  left join (select per_capita_gdp,name from international_gdp) i
on p.country=i.name
where p.annual_wage < (select max(per_capita_gdp)
                           from international_gdp i
                           where p.country = i.name);


-- udf to calculate tax
create or replace function tax_from_wage_udf (wage float, rate float)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(wage, rate):
  return wage * rate
$$;
select job_name, annual_wage, tax_from_wage_udf(annual_wage, 0.28)::number(8,2) as tax, country 
    from pay;


--------------------------------------------------------
-- scripting
execute immediate $$
    declare
        profit number(38, 2) default 0.0;
    begin
        let cost number(38, 2) := 100.0;
        let revenue number(38, 2) default 110.0;

        profit := revenue - cost;
        return profit;
    end;
$$
;

--

create or replace procedure compare_json (json1 text, json2 text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run( json1, json2):
  json1_load=json.loads(json1)
  json2_load=json.loads(json2)
  if sorted(json1_load.items()) == sorted(json2_load.items()):
      return "Equal"
  else:
      return "Not Equal!"
$$;

set json1='{"Name":"ABC", "Age":"30"}';
set json2='{"Name":"ABC", "Age":"20"}';
select $json1;
select $json2;

call compare_json($json1, $json2);


-- udf
create or replace function compare_json_func (json1 text, json2 text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(json1, json2):
  json1_load=json.loads(json1)
  json2_load=json.loads(json2)
  if sorted(json1_load.items()) == sorted(json2_load.items()):
      return "Equal"
  else:
      return "Not Equal!"
$$;

set json1='{"Name":"ABC", "Age":"30"}';
set json2='{"Name":"ABC", "Age":"20"}';
select $json1;
select $json2;

select compare_json_func($json1, $json2);


-- procdure
create or replace procedure query_wage_from_pay_table (job_name text, country_name text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import pandas as pd
def run(session, job_name, country_name):
  sf_df_results = session.sql(f'''select annual_wage from pay 
                    where country='{country_name}' and job_name='{job_name}'
                  ''').collect()
  return sf_df_results
$$;

call query_wage_from_pay_table('Data Engineer', 'CA');


create or replace procedure query_wage_from_pay_table (job_name text, country_name text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import pandas as pd
def run(session, job_name, country_name):
  sf_df_results = pd.DataFrame(session.sql(f'''select annual_wage from pay 
                    where country='{country_name}' and job_name='{job_name}'
                  ''').collect()).iloc[0,0]
  return sf_df_results
$$;

call query_wage_from_pay_table('Data Engineer', 'CA');


list @my_internal_stage;



show transactions in account;





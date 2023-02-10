create or replace table person (person_id int, last_name text);
insert into person values (1, 'Tom');
create or replace table case_object (fdbl_caseworker_id int, case_id int);
insert into case_object values (1, 1001);

CREATE OR REPLACE FUNCTION fnRpt_GetCaseWorkers1(P_CASE_ID INT,P_CASE_WORKNUMBER INT)
RETURNS VARCHAR
LANGUAGE SQL
AS
'
SELECT CASE WHEN P_CASE_WORKNUMBER=1 THEN 
(
SELECT last_name
FROM person p
INNER JOIN case_object co 
 ON p.person_id = co.fdbl_caseworker_id
WHERE co.case_id = p_Case_ID
 )
 end';

set case_id=1001;
select fnRpt_GetCaseWorkers1(case_id,1) from case_object;
    
    -----------------
create or replace stage uni_klaus_clothing
  url = 's3://uni-klaus/clothing';
list @uni_klaus_clothing;

create or replace stage trails_geojson
  url = 's3://uni-lab-files-more/dlkw/trails/trails_geojson';
list @trails_geojson;

create or replace stage trails_parquet
  url = 's3://uni-lab-files-more/dlkw/trails/trails_parquet';
list @trails_parquet;

create or replace file format ff_json 
 type = 'json';
 
create or replace file format ff_parquet
  type = 'parquet';
  
select $1 from @trails_geojson
  (file_format => ff_json);
  
select $1 from @trails_parquet
  (file_format => ff_parquet);

select *
from table(information_schema.query_history_by_session())
order by start_time;

select *
from table(information_schema.query_history())
order by start_time;

----
show stages;
list @feng_stage;

list @FENG_S3_INTEGRATION_STAGE;


create or replace table myiris (sl float, sw float, pl float, pw float, sp varchar);
create or replace table myiris_raw (raw variant);

copy into myiris_raw 
  from @FENG_S3_INTEGRATION_STAGE/parquet/iris.parquet
  file_format = (type = parquet);

select * from myiris_raw;
select raw:PetalLengthCm::number(3, 1),
       raw:PetalWidthCm::number(3, 1),
       raw:SepalLengthCm::number(3, 1),
       raw:SepalWidthCm::number(3, 1),
       raw:Species::varchar
  from myiris_raw;

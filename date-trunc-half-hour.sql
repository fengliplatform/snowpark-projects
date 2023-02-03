select date_trunc('QUARTER', '2019-08-01 12:34:56.789'::timestamp_ntz) as "TRUNCATED",
       extract(   'QUARTER', '2019-08-01 12:34:56.789'::timestamp_ntz) as "EXTRACTED";
       
select date_trunc('minute', '2019-08-01 12:34:56.789'::timestamp_ntz) as "TRUNCATED",
       extract('minute', '2019-08-01 12:34:56.789'::timestamp_ntz) as mm,
       case
       when mm >=30 then 30
       when mm < 30 then 0
       end as half;
       
select '2019-08-01 12:54:56.789' as mytime,
    date_trunc('hour', mytime::timestamp_ntz) as to_hour,
    extract('minute', mytime::timestamp_ntz) as mm,
       case
       when mm >=30 then 30
       when mm < 30 then 0
       end as half,
   dateadd(minute, half, to_hour) as my_half;
       
--
 
 SELECT 
'2019-08-01 12:54:56.789' as STATUS_START_AT, 
DATE_TRUNC('hour', STATUS_START_AT::timestamp_ntz) as trunc_hour,
FLOOR(MINUTE(STATUS_START_AT::timestamp_ntz) / 30.0) as myfloor,
DATEADD(minute, 30 * myfloor, trunc_hour) as half_hour_interval;

------------

select * from pay;

create or replace view pay_view as
    select * from pay;

desc view pay_view;
comment on column pay_view.job_name is 'this is job name';
alter view testcomment_v alter v_location comment = 'New location in view';
alter view pay_view alter job_name comment = 'job name in this view';

desc table pay;
comment on column pay.job_name is 'this is job name';

create or replace view pay_view2 (job_name comment 'this is job name') as
    select job_name from pay;
describe view pay_view2;

----



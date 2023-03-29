create database fengdb;

create or replace table mytable(create_dt date);
insert into mytable values ('2023-01-31'::date);
update mytable set create_dt = dateadd(day, -1, '2023-01-31'::date);
select * from mytable;

select time, time::string from values ('2023-02-28 12:25:20.446137 +0000'::timestamp_tz) x(time);
show parameters like 'TIMESTAMP_OUTPUT_FORMAT';
alter user set TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM';
select time, time::string from values ('2023-02-28 12:25:20.446137 +0000'::timestamp_tz) x(time);

show parameters like '%binary%';

select HEX_ENCODE('Snowflake');
--536E6F77666C616B65
select to_binary('536E6F77666C616B65'); -- varchar to binary
-- 536e6f77666c616b65

select base64_encode('Snowflake');
-- U25vd2ZsYWtl
select to_binary('U25vd2ZsYWtl', 'base64');
-- 536e6f77666c616b65


--select utf8_encode('Snowflake');


select to_binary('U25vd2ZsYWtl', 'base64');
-- 536e6f77666c616b65

--
select '\u2744', 
       '❄', 
       unicode('❄'), 
       to_binary('❄', 'utf-8'), -- e29d84
       to_varchar('\u2744'), 
       to_varchar(to_binary('❄', 'utf-8'), 'utf-8'),
       hex_decode_string('e29d84'),
       base64_decode_string('4p2E');

--
create file format csv_ff2 type=csv, skip_header=1, binary_format='utf-8';

select $1, $2, $3 from 
  @feng_s3_stage/csv/students2.csv
  (file_format=>csv_ff2);


--select $1, $2, to_varchar(to_binary($3, 'utf-8'), 'utf-8') from
select $1, $2, to_binary($3) from
  @feng_s3_stage/csv/students2.csv
  (file_format=>csv_ff2);

show parameters like '%binary%';
alter session set BINARY_INPUT_FORMAT='UTF-8';
alter session set BINARY_OUTPUT_FORMAT='UTF-8';
--alter session set BINARY_OUTPUT_FORMAT='UTF-8';
alter session unset BINARY_INPUT_FORMAT;
alter session unset BINARY_OUTPUT_FORMAT;


SELECT TO_BINARY('SNOW', 'utf-8');
SELECT TO_VARCHAR(TO_BINARY('SNOW', 'utf-8'), 'HEX');

select hex_encode('❄'), to_binary('❄', 'utf-8');
select hex_encode('❄'), to_binary('❄', 'hex'); -- not a legal hex-encoded
select hex_encode('❄'), to_binary('❄', 'base64'); -- not a legal base64-encoded

select hex_decode_string('e29d84'), hex_decode_binary('e29d84');

--
select hex_encode('❄'); --E29D84
select hex_encode('✨'); --E29CA8
select base64_encode('❄'); --4p2E
select base64_encode('✨'); --4pyo

-----------------
create file format csv_ff type=csv, skip_header=1;

select $1, $2, $3 from 
  @feng_s3_stage/csv/students-hex.csv
  (file_format=>csv_ff);
  
--select $1, $2, base64_decode_string($3) from 
select $1, $2, hex_decode_binary($3) from 
  @feng_s3_stage/csv/students-hex.csv
  (file_format=>csv_ff);

create or replace table students_hex(name varchar, mark int, emoji binary) as
select $1, $2, hex_decode_binary($3) from 
  @feng_s3_stage/csv/students-hex.csv
  (file_format=>csv_ff);

select * from students_hex;

select name, mark, 
       to_varchar(emoji, 'hex') as my_emoji 
       from students_hex;
select name, mark, 
       hex_decode_string(to_varchar(emoji, 'hex')) as my_emoji 
       from students_hex;

create or replace table students_base64(name varchar, mark int, emoji binary) as
select $1, $2, base64_decode_binary($3) from 
  @feng_s3_stage/csv/students-base64.csv
  (file_format=>csv_ff);
select * from students_base64;

select name, mark, 
       base64_decode_string(to_varchar(emoji, 'base64')) as my_emoji 
       from students_base64;



       

  -----------------

--create file format csv_ff3 type=csv, skip_header=1, binary_format='utf-8';
create file format csv_ff3 type=csv, skip_header=1;

--select $1, $2, $3 from 
--select $1, $2, base64_decode_string($3) from 
select $1, $2, base64_decode_binary($3) from 
  @feng_s3_stage/csv/students3.csv
  (file_format=>csv_ff3);
-- e29d84 (hex)
alter session set BINARY_INPUT_FORMAT='BASE64';
alter session set BINARY_OUTPUT_FORMAT='BASE64';
show parameters like '%binary%';
select $1, $2, base64_decode_binary($3) from 
  @feng_s3_stage/csv/students3.csv
  (file_format=>csv_ff3);
--e29d84 (hex)

--select $1, $2, $3 from
--select $1, $2, base64_decode_string($3) from 
select $1, $2, base64_decode_binary($3) from 
--select $1, $2, base64_encode(base64_decode_string($3)) from 
--select $1, $2, to_binary(base64_encode(base64_decode_string($3)), 'base64') from 
  @feng_s3_stage/csv/students3.csv
  (file_format=>csv_ff3);


create or replace table students3 (name varchar, mark int, emoji binary) as
select $1, $2, base64_decode_binary($3) from 
  @feng_s3_stage/csv/students3.csv
  (file_format=>csv_ff3);

select * from students3;
select name, mark, hex_decode_string(to_varchar(emoji, 'hex')) as my_emoji from students3;
select typeof(emoji) from students3;

--
select '\u2728';
SELECT BASE64_DECODE_STRING('U25vd2ZsYWtl'); -- Snowflake
SELECT BASE64_DECODE_binary('U25vd2ZsYWtl'); -- 536e6f77666c616b65 (hex)

select TO_BINARY('536e6f77666c616b65', 'HEX');
select TO_BINARY('U25vd2ZsYWtl', 'BASE64');

select hex_encode('Snowflake'); -- output: 536E6F77666C616B65
select base64_encode('Snowflake'); -- output: U25vd2ZsYWtl
alter session set BINARY_OUTPUT_FORMAT='BASE64';
select TO_BINARY('536e6f77666c616b65', 'HEX'); -- output 536e6f77666c616b65
select to_varchar(TO_BINARY('536e6f77666c616b65', 'HEX'), 'hex');
select to_varchar(TO_BINARY('536e6f77666c616b65', 'HEX'), 'base64');

select TO_BINARY('U25vd2ZsYWtl', 'BASE64'); -- why this output is still hex string: 536e6f77666c616b65 ?
select to_varchar(TO_BINARY('U25vd2ZsYWtl', 'base64'), 'hex');
select to_varchar(TO_BINARY('U25vd2ZsYWtl', 'base64'), 'base64');


---------

select name, mark, 
       base64_decode_string(to_varchar(emoji, 'hex')) as my_emoji 
       from students_hex;

select hex_decode_string('4p2E');


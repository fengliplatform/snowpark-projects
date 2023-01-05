create or replace stage UNI_KLAUS_ZMD
  url = 's3://uni-klaus/zenas_metadata';
list @UNI_KLAUS_ZMD;

create or replace file format zmd_file_format_1
RECORD_DELIMITER = '^' field_delimiter = '=';

select $1,$2
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create or replace table mytbl (color varchar);

copy into mytbl from
    (select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1));

select * from mytbl;
truncate table mytbl;

create or replace procedure insert_file(file_name varchar)
returns varchar
language sql
as
$$
begin

execute immediate 'copy into mytbl from
    (select $1
from @uni_klaus_zmd/' || :file_name || '
(file_format => zmd_file_format_1))';

end;

$$;
call insert_file('product_coordination_suggestions.txt');

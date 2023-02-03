--

create table address (postal_code string);
insert into address values ('00000000000'), ('999999'), ('XXXX'), ('PPPP'), ('SNOWFLAKE');

WITH CTE as (
SELECT
postal_code,
length(a.postal_code) as string_length,
substr(postal_code,1,1) as first_char,
regexp_count(a.postal_code, first_char) as repitative_count,
CASE WHEN string_length = repitative_count then 1 else 0 end as COUNT
FROM address a
)
select sum (COUNT) from CTE; 

SELECT
postal_code,
length(a.postal_code) as string_length,
substr(postal_code,1,1) as first_char,
regexp_count(a.postal_code, first_char) as repitative_count,
CASE WHEN string_length = repitative_count then 1 else 0 end as COUNT
FROM address a;

--
select * from information_schema.packages where package_name like '%simplejson%';

-- share
create table pay2 clone pay;



--  encoding/decoding
create table mytable (col1 text);
insert into mytable values ('|ravÉi');
select * from mytable;

select col1,uncode(col1,ecoding='utf-8')  from mytable;
select  TO_BINARY(col1,'UTF-8') from mytable;

-- utf 8: CÔTE D'IVOIRE
SELECT
    C_BIRTH_COUNTRY,
    SUBSTR(C_BIRTH_COUNTRY, 2, 1),
    TO_BINARY('Ô','UTF-8'),
    TO_BINARY(SUBSTR(C_BIRTH_COUNTRY, 2, 1),'UTF-8')
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER WHERE C_CUSTOMER_ID = 'AAAAAAAAAFAAAAAA';

-- remove 
SELECT regexp_replace('123Ôddd|ravÉidd$%^', '[^a-zA-Z0-9]+');

-- 

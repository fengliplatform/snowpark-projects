----------------
-- array data type

select ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
select ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as myarr, myarr[0];

--set weekdays_abbr = (select array_construct('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'));
set weekdays_abbr = '["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]';
select $weekdays_abbr;
select to_array($weekdays_abbr);

select * from table(flatten(parse_json($weekdays_abbr)));

select array_construct('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');


set my_arr = '["Hello", "world"]';
select f.value::text  
from lateral flatten(input => parse_json($my_arr)) f;

create or replace table weekdays_tbl (name varchar, abbr varchar);
insert into weekdays_tbl values
        ('Monday', 'Mon'), ('Tuesday', 'Tue'),('Wednsday', 'Wed'),('Thursday', 'Thu'),
        ('Friday', 'Fri'),('Saturday', 'Sat'),('Sunday', 'Sun');
select * from weekdays_tbl;

create or replace function get_weekday_full_names(weekday_array array)
returns table(full_name varchar)
language sql
as $$
    select weekdays.name from weekdays_tbl weekdays
      join table(flatten(weekday_array)) given_array_tbl
      on weekdays.abbr = given_array_tbl.value
$$;

select * from table(get_weekday_full_names(['Tue', 'Fri']));
select * from table(get_weekday_full_names($weekdays_abbr));


-- return value change to array?
create or replace function get_weekday_full_names_2(weekday_array array)
returns array
language sql
as $$
    select array_construct(listagg(weekdays.name, ' ')) from weekdays_tbl weekdays
      join table(flatten(weekday_array)) given_array_tbl
      on weekdays.abbr = given_array_tbl.value
$$;

select get_weekday_full_names_2(['Tue', 'Fri']);

select array_construct("aaa");


set variable1 = '[value1,value2]';

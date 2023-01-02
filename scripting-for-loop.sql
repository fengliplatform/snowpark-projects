-- Table that has the dates we target
create or replace table some_date (id integer, mydate text);
insert into some_date (id, mydate) values
  (1, '2021-01-01'),
  (2, '2021-02-01'),
  (3, '2021-03-01');
-- Table that has full data or raw data
create or replace table data_tbl (price number(12, 2), mydate date);
insert into data_tbl (price, mydate) values
    (11.11, '2021-01-01'),
    (22.22, '2021-02-01'),
    (33.33, '2021-03-01'),
    (44.44, '2021-04-01'),
    (55.55, '2021-05-01'),
    (66.66, '2021-06-01');
select * from data_tbl;
-- Table that is for the date from targeted dates
create or replace table new_data_tbl (price number(12, 2), mydate date);

-- Create stored procedure using cursor to read targated dates, looping the 
-- results, retreiving raw data to save to target table.
-- date data type needs to be converted properly
create or replace procedure inserting_data()
returns text
language sql
as
$$
declare
  mydates cursor for select mydate from some_date;
  target_date date;
begin
  open mydates;
  for myrow in mydates do
      target_date := myrow.mydate::date;
      insert into new_data_tbl select price, mydate from data_tbl where mydate = :target_date;
  end for;
  return 'success';
end;
$$
;
call inserting_data();
select * from new_data_tbl;

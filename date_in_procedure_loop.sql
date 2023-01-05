create or replace table some_date (id integer, mydate text);
insert into some_date (id, mydate) values
  (1, '2021-01-01'),
  (2, '2021-02-01'),
  (3, '2021-03-01');
select * from some_date;


execute immediate $$
declare
  target_date date;
  mydates cursor for select mydate from some_date;
begin
  open mydates;
  for myrow in mydates do
      target_date := myrow.mydate::date;
      insert into new_data_tbl select price, mydate from data_tbl where mydate = :target_date;
  end for;
  close mydates;
  return 'success';
end;
$$;

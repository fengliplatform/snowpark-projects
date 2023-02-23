----
create or replace table tempTable (value1 text, value2 text, value3 text, value4 text, value5 text, value6 text);
insert into tempTable values('v1', 'v2', 'v3', null, null, 'v6');
select * from tempTable;

create or replace table myTable (a variant, b text);

select * from myTable;


create or replace procedure insert_proc()
  returns varchar
  language sql
  as
  $$
    declare
      myobj varchar:='';
    begin
      myobj := (select OBJECT_CONSTRUCT('key1', value4, 'key2', value5) from tempTable);
      if (myobj = '{}') then
        insert into myTable (a,b) select TO_VARIANT(ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('key1', value1, 'key2', value2))),
          value6 from tempTable;
      else
        insert into myTable (a,b) select TO_VARIANT(ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('key1', value1, 'key2', value2),OBJECT_CONSTRUCT('key1', value4, 'key2', value5))),
          value6 from tempTable;      end if;
    end;
  $$;

call insert_proc();
select * from myTable;

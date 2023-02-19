create or replace table mytable3 (id int primary key, name text);
insert into mytable3 values (1, 'Tom'), (2, 'John');

show primary keys in mytable3;
set last_query = (select last_query_id());
select * from table(result_scan($last_query));

create or replace procedure proc_test(tablename text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        -- res := (show primary keys in table(:tablename));
        res := (select * from table(:tablename));
        return table(res);
    end;
    $$;
call proc_test('mytable3');

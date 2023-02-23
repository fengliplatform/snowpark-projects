create or replace table mytable6 (id int primary key, name text);
insert into mytable6 values (1, 'Tom'), (2, 'John');

create or replace procedure proc_test6(tablename text)
    returns varchar
    language sql
    as
    $$
    declare
        mycount int;
    begin
        select count(*) into :mycount from table(:tablename);
        return mycount::varchar;
    end;
    $$;

execute immediate 'call proc_test6(\'mytable6\')';
set row_number = (select * from table(result_scan(last_query_id())));
select $row_number;


create or replace table mytable6 (id int primary key, name text);
insert into mytable6 values (1, 'Tom'), (2, 'John');
select max(name) from mytable6 where id=(select max(id) as max_id from mytable6);
select max(name) from mytable6 where id=(select min(id) from mytable6);

create or replace procedure proc_test7(tablename text)
    returns table()
    language sql
    as
    $$
    declare
        res resultset;
    begin
        res := (select * from table(:tablename));
        return table(res);
    end;
    $$;
call proc_test7('mytable6');
execute immediate 'call proc_test7(\'mytable6\')';
select * from table(result_scan(last_query_id()));

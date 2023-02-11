--
create or replace procedure create_table_sproc (brand_name text)
returns varchar
language sql
as
$$
begin
     execute immediate 'create or replace table '|| :brand_name || '_table (id int)';
     return 'success';
end;
$$;

call create_table_sproc('brand1');

show tables like '%brand1%';

--

create or replace table source (id int,col1 int, col2 int) as
    select 1,101, 102
    union
    select 2,201, 202;
create or replace table target(id int, col1 int) as
    select 1,101
    union
    select 2,201;
create or replace stream my_stream on table source;
update source set col1 = 103 where id=1;
select * from my_stream;

merge into target t
    using (select id, col1, metadata$action as a, metadata$isupdate as i, metadata$row_id as r from my_stream) s
    on t.id = s.id
    when matched and s.a = 'INSERT' and s.i = 'TRUE' then
        update set t.col1 = s.col1;

select * from source;
select * from target;

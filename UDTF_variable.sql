create or replace table mytable3 (col1 varchar, col2 varchar);
insert into mytable3 values ('John', 'Smith');
select * from mytable3;

create or replace function func (input_tablename varchar) 
returns table (col1 varchar, col2 varchar)
as $$
	    SELECT * from mytable3
$$;
select * from table(func('mytable3'));

-- not working
set table_name='mytable3';
execute immediate $$
select * from :table_name
$$;

-- procedure can accept table name as parameter properly
create or replace procedure myproc (input_tablename varchar) 
returns text
as $$
begin
	execute immediate 'insert into '||:input_tablename|| ' values (\'Tom\', \'Martins\')';
    return 'succeed';
end;
$$;

call myproc('mytable3');

-- not working in function using table name as parameter
create or replace function myfunc (input_tablename varchar) 
returns table (col1 text)
as $$
	select col1 from :input_tablename
$$;

select * from table(myfunc('mytable3'));

-- this Not work as variable is in from clause
create or replace function myfunc (input_tablename varchar, column_name varchar, name varchar) 
returns table (col1 text, col2 text)
as $$
	select * from :input_tablename where col1='Tom'
$$;

-- this Not work as variable is in select clause
-- error: Bind variables not allowed in view and UDF definitions.
create or replace function myfunc (input_tablename varchar, column_name varchar, name varchar) 
returns table (col1 text)
as $$
	select :column_name from mytable3 where col1='Tom'
$$;

-- this Not work as variable is in select clause, definition has no error. but it returns 
-- 'col1' as a string result instead of the value of the column col1.
create or replace function myfunc (input_tablename varchar, column_name varchar, name varchar) 
returns table (col1 text)
as $$
	select column_name from mytable3 where col1='Tom'
$$;
select * from table(myfunc('mytable3', 'col1', 'Tom'));

-- this works as variable is in where clause
create or replace function myfunc (input_tablename varchar, column_name varchar, name varchar) 
returns table (col1 text, col2 text)
as $$
	select * from mytable3 where col1=name
$$;

select * from table(myfunc('mytable3', 'col1', 'Tom'));

select column_name from mytable3 where col1='Tom'

-- example from doc 
create or replace table orders (
    product_id varchar, 
    quantity_sold numeric(11, 2)
    );

insert into orders (product_id, quantity_sold) values 
    ('compostable bags', 2000),
    ('re-usable cups',  1000);
create or replace function orders_for_product(prod_id varchar)
    returns table (product_id varchar, quantity_sold numeric(11, 2))
    as
    $$
        select product_id, quantity_sold 
            from orders 
            where product_id = prod_id
    $$
    ;
select product_id, quantity_sold
    from table(orders_for_product('compostable bags'))
    order by product_id;

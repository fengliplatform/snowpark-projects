-- https://docs.snowflake.com/en/sql-reference/constructs/group-by-grouping-sets.html

-- Create some tables and insert some rows.
create table products (product_id integer, wholesale_price real);
insert into products (product_id, wholesale_price) values 
    (1, 1.00),
    (2, 2.00);

create table sales (product_id integer, retail_price real, 
    quantity integer, city varchar, state varchar);
insert into sales (product_id, retail_price, quantity, city, state) values 
    (1, 2.00,  1, 'SF', 'CA'),
    (1, 2.00,  2, 'SJ', 'CA'),
    (2, 5.00,  4, 'SF', 'CA'),
    (2, 5.00,  8, 'SJ', 'CA'),
    (2, 5.00, 16, 'Miami', 'FL'),
    (2, 5.00, 32, 'Orlando', 'FL'),
    (2, 5.00, 64, 'SJ', 'PR');
    
select * from sales;
select state from sales;

-- group by one item: state
select state, sum(retail_price*quantity) as avenue from sales
    group by (state);

-- group by one item: city
select city, sum(retail_price*quantity) as avenue from sales
    group by (city);

-- group by grouping sets of two items
-- union all of above typical 
select state, city, sum(retail_price*quantity) as avenue from sales
    group by grouping sets (state, city);
    
--
-- typical group by uing two items, to form a hierachy
select state, city, sum(retail_price*quantity) as avenue from sales
    group by (state, city);
-- group by rollup
select state, city, sum(retail_price*quantity) as avenue from sales
    group by rollup (state, city)
    order by state;

-- group by cube
select state, city, sum(retail_price*quantity) as avenue from sales
    group by cube (state, city)
    order by state;

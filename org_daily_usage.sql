select * from SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY;

select week(usage_date) as usage_week, avg(usage_in_currency) 
    from SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
    group by usage_week;
    
select month(usage_date) as usage_month, avg(usage_in_currency) 
    from SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
    group by usage_month;
    
    
-----------------------------------

CREATE OR REPLACE TABLE ASSIGNMENT2(

       order_id VARCHAR(30) ,

       order_date DATE PRIMARY KEY,

       ship_date DATE,

       ship_mode VARCHAR(35),

       customer_name VARCHAR(35),

       segment VARCHAR(25),

       state VARCHAR(50),

       country VARCHAR(50),

       market VARCHAR(40),

       region VARCHAR(40),

       product_id VARCHAR(40),

       category VARCHAR(40),

       sub_category VARCHAR(40),

       product_name VARCHAR(200),

       sales INT,

       quantity INT,

       discount INT,

       profit INT,

       shipping_cost INT,

       order_priority VARCHAR(40),

       year VARCHAR(10)

 

);
CREATE OR REPLACE TABLE ASSIGNMENT3(
       order_id VARCHAR(30) ,
       order_date DATE PRIMARY KEY,
       ship_date DATE,
       ship_mode VARCHAR(35)
);
insert into assignment3 values(1, '2023-01-01'::date, '2023-01-05'::date, 'Standard');
select * from assignment3;

alter table assignment3 add column process_days int;
select * from assignment3;

update assignment3 set process_days = datediff(day, order_date, ship_date);
select * from assignment3;

create or replace table CURRENCY_CONVERSION (date date, rate float, rate_type varchar);
insert into CURRENCY_CONVERSION values ('2023-01-03'::date, 1.1, 'c'), 
                                       ('2023-01-06'::date, 1.5, 'c'),
                                       ('2023-02-07'::date, 0.9, 'c');
create or replace table PURCHASE_ORDER (rate_type varchar, posting_date date, amount float);
insert into PURCHASE_ORDER values ('c', '2023-01-04'::date, 1200);

select p.rate_type, p.posting_date, 
  ( select rate from CURRENCY_CONVERSION 
        where RATE_TYPE =  p.rate_type  
        and DATE >=  p.posting_date                              
        order by DATE asc limit 1) as rate 
from PURCHASE_ORDER p;

create or replace function test_func2(rate_type varchar, posting_date date)
    returns float
    language sql
    as $$
     select rate from CURRENCY_CONVERSION 
        where RATE_TYPE =  rate_type  
        and DATE >=  posting_date                              
        order by DATE asc limit 1
    $$;

select amount, rate_type, posting_date, test_func2(rate_type, posting_date) as rate
from PURCHASE_ORDER;

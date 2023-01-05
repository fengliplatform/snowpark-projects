create or replace table order_tbl (amount float, order_date text);
insert into order_tbl values
    (12.5, '25/05/2022'),
    (17.75, '27/05/2022'),
    (2.5, '12/06/2022'),
    (7.25, '17/06/2022'),
    (12.5, '25/06/2022'),
    (27.5, '02/07/2022'),
    (32.5, '15/07/2022'),
    (7.25, '20/07/2022')
    ;
--truncate table order_tbl;
select * from order_tbl;

select avg(amount), date_trunc(month, to_date(order_date,'dd/mm/yyyy')) as order_month from order_tbl
    group by order_month order by order_month;

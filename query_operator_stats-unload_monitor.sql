UNSET todays_amount;
call todays_delivery_amount('10');
set todays_amount = (select TODAYS_DELIVERY_AMOUNT from table(result_scan(last_query_id())));

select name from student where id=1;
select * from table(result_scan(last_query_id()));

select *
    from table(get_query_operator_stats('01aa4016-0604-45fa-0047-5e0700026412'));
select *
    from table(get_query_operator_stats(last_query_id()));

--
list @my_internal_stage;
copy into @my_internal_stage/student.csv 
    from student;
select query_text, query_type, outbound_data_transfer_cloud, outbound_data_transfer_region, outbound_data_transfer_bytes
    from snowflake.account_usage.query_history where query_type='UNLOAD' order by start_time desc;

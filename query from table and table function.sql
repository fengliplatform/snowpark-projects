     create or replace table dates_of_interest (event_date date);
insert into dates_of_interest (event_date) values
    ('2021-06-21'::date),
    ('2022-06-21'::date);

create or replace function record_high_temperatures_for_date(d date)
    returns table (event_date date, city varchar, temperature number)
    as
    $$
    select d, 'New York', 65.0
    union all
    select d, 'Los Angeles', 69.0
    $$;
    
select
        doi.event_date as "Date", 
        record_temperatures.city,
        record_temperatures.temperature
    from dates_of_interest as doi,
         table(record_high_temperatures_for_date(doi.event_date)) as record_temperatures
      order by doi.event_date, city;
      
---- above example in doc works. but below get_query_operator_stats and result_scan failed.
---- get_query_operator_stats: Invalid value [variable query id input] for function 'get_query_operator_stats' at position 1
---- result_scan: Error: Invalid result query ID, found 'sq' (line 350)

SELECT * 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY as Q
    ,TABLE(GET_QUERY_OPERATOR_STATS(Q.QUERY_ID)) as STATS
LIMIT 100;

SELECT SQ.QUERY_ID as q_id, stats.operator_type as operator_type
FROM smaller_query_history as SQ
    ,TABLE(GET_QUERY_OPERATOR_STATS('01a9576d-3200-9cf5-0000-0001f65280e1')) as STATS
LIMIT 100;
SELECT smaller_query_history.query_id, stats.*
FROM smaller_query_history
    ,TABLE(result_scan(smaller_query_history.query_id)) as stats
order by sq.query_id desc
LIMIT 100;

SELECT sq.query_id, stats.query_id, stats.operator_type as operator_type
FROM smaller_query_history as sq
    ,TABLE(GET_QUERY_OPERATOR_STATS(sq.query_id)) as stats
order by sq.query_id desc
LIMIT 100;

SELECT sq.query_id, stats.query_id, stats.operator_type as operator_type
FROM smaller_query_history as sq
    ,TABLE(result_scan(sq.query_id)) as stats
order by sq.query_id desc
LIMIT 100;


create or replace table smaller_query_history as
    SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY limit 1;
select sq.* from smaller_query_history as sq;
select sq.query_id from smaller_query_history as sq;
desc table smaller_query_history;
update  smaller_query_history set query_id='01a9838d-3200-9e0d-0001-f6520003a0f6';
select *
    from table(get_query_operator_stats(last_query_id()));

SELECT Q.query_id 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
LIMIT 100;

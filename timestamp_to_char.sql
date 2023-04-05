select
  year(current_timestamp)::varchar,
  to_char(current_timestamp, 'YYYYMM'),
  to_char(current_timestamp, 'YYYYMMDD');

select $1:year_published::date from @json_stage/book.json (file_format => json_file_format);
select to_date($1:year_published::text, 'YYYY') 
    from @json_stage/book.json (file_format => json_file_format);

select to_date('2001', 'YYYY');
select YEAR(to_date('2001-01-01', 'YYYY-MM-DD'));

SELECT YEAR(CURRENT_DATE())||'-'||WEEK(CURRENT_DATE());
SELECT YEAR(CURRENT_DATE());

select to_date($1:year_published::text) from @json_stage/book2.json (file_format => json_file_format);
select $1:year_published::date from @json_stage/book2.json (file_format => json_file_format);

select $1:year_published::date from @json_stage/book3.json (file_format => json_file_format);

select to_date($1:year_published::text, 'MM-DD-YY') from @json_stage/book3.json (file_format => json_file_format);

select to_date($1:year_published::text, 'MM-DD-YY') 
    from @json_stage/book3.json (file_format => json_file_format);

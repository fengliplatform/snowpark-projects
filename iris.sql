-- download iris dataset json version from Kaggle
create or replace file format json_ff type = json STRIP_OUTER_ARRAY = TRUE;

create or replace table iris_table (petal_length float, 
                                    petal_width float, 
                                    sepal_length float,
                                    sepal_width ,
                                    species varchar) as  
    select $1:petalLength as pl, $1:petalWidth as pw, 
         $1:sepalLength as sl, $1:sepalWidth as sw,
         $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff);

select * from iris_table;    

    where pl > 2 and pw < 1.5 order by random() limit 10;
    
create or replace table iris_table as
  select * from @feng_s3_stage/iris_json/iris.json (file_format => json_ff);
select * from iris_table;


select * from iris_table sample(10);

select * from iris_table where petal_length > 5;

select * from iris_table sample (10) where petal_length > 5;

select * from iris_table where petal_length > 5 sample (10);

select * from 
    (select * from iris_table where petal_length > 5)
    sample (10);

select * from iris_table where petal_length > 5
    order by random() limit 4;

    

set percent_float = 0.2;
set total_row_count = (select count(*) from iris_table where petal_length > 5);
select $total_row_count;
set my_row_count = (select floor($total_row_count * $percent_float));
select * from iris_table where petal_length > 5
    order by random() limit $my_row_count;


show stages;
list @feng_s3_stage;

create or replace stage feng_s3_stage url = 's3://feng-public-bucket';
list @feng_s3_stage/iris_json;






SELECT metadata$filename FROM @feng_s3_stage/;
SELECT metadata$filename FROM @feng_s3_stage/book_json/food_book.json;



create or replace file format json_ff type = json STRIP_OUTER_ARRAY = TRUE;
select $1 from @feng_s3_stage/iris_json/iris.json (file_format => json_ff);

---- filter by columns
select $1:book_title, $1:published, $1:authors[0]:first_name from @feng_s3_stage/book_json/food_book.json (file_format => json_ff);

select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff);
    
create or replace table iris_sample_table (petal_length float, petal_with float, species varchar) as  
  select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff);
select * from iris_sample_table;

select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff)
    where pl > 2 and pw < 1.5
    order by random() limit 10;

create or replace table iris_sample_table (petal_length float, petal_with float, species varchar) as  
  select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff)
    where pl > 2 and pw < 1.5 order by random() limit 10;
select * from iris_sample_table;


insert into iris_sample_table 
  (select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff)
    where pl > 2 and pw < 1.5 order by random() limit 10);

select * from iris_sample_table sample(10) where petal_length > 5;
select * from iris_sample_table  where petal_length > 5 sample(10);
select * from iris_sample_table  where petal_length > 6;
select * from (select * from iris_sample_table  where petal_length > 6) sample(10);

insert into iris_sample_table 
  (select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff)
--    from @feng_s3_stage/iris_json/iris.json (file_format => json_ff) sample(10)
    where pl > 2 and pw < 1.5 sample(10) );

create or replace table iris_sample_table (petal_length float, petal_width float, species varchar);
copy into iris_sample_table
  from (select $1:petalLength as pl, $1:petalWidth as pw, $1:species 
        from @feng_s3_stage/iris_json/iris.json (file_format => json_ff)
        where pl > 2 and pw < 1.5);
select * from iris_sample_table;


---- filter by rows
select $1, $2, $3, $4, $5, $6, $7 from @feng_s3_stage/the_oscar_award.csv limit 5;
select $1 from @feng_s3_stage/food_book.json (file_format => json_ff);


create stage an_s3_bucket_stage 
 url = 's3://uni-lab-files';
list @an_s3_bucket_stage;
select $1 from @an_s3_bucket_stage/author_with_header.json (file_format => json_ff);

-- filter by file name patterns
copy into iris_sample_table
     from @feng_s3_stage/log_json/ file_format = (type = json)
     pattern = '.*2023-03.log'
     on_error = continue;

truncate table iris_sample_table;
copy into iris_sample_table
  from (select $1:petalLength, $1:petalWidth, $1:species 
               from @feng_s3_stage/log_json/ 
                    (file_format => json_ff))
  pattern = '.*2023-03.log'
  on_error = continue;
                  

copy into log_sample_table
  from (select $1:timestamp, $1:source, $1:target 
               from @feng_s3_stage/log_json/ 
                    (file_format => json_ff))
  pattern = '.*2023-03.log'
  on_error = continue;

  
---- External Table
CREATE or REPLACE EXTERNAL TABLE food_book_ext_table (raw variant as value)
WITH LOCATION = @feng_s3_stage/book_json/
AUTO_REFRESH = FALSE
FILE_FORMAT = (TYPE = JSON);

select raw:book_title from food_book_ext_table;
select * from food_book_ext_table order by random() limit 2;
select raw:authors[0]:first_name as fn from food_book_ext_table where fn = 'Gian';


CREATE or REPLACE EXTERNAL TABLE oscar_ext_table (
  year_film varchar as to_varchar(value:c1),
  year_ceremony varchar as to_varchar(value:c2),
  ceremony varchar as to_varchar(value:c3::varchar),
  category varchar as to_varchar(value:c4::varchar),
  name varchar as to_varchar(value:c5::varchar),
  film varchar as to_varchar(value:c6::varchar),
  winner varchar as to_varchar(value:c7::varchar)
)
WITH LOCATION = @feng_s3_stage/oscar_award_csv/
AUTO_REFRESH = FALSE
FILE_FORMAT = (TYPE = CSV skip_header=1);

select * from oscar_ext_table order by random() limit 10;

----------------

SELECT *
FROM (
  SELECT *, PERCENT_RANK() OVER (ORDER BY testcolumn DESC) AS percentile
  FROM sample_table
) t
WHERE percentile <= 0.1;

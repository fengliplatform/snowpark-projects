-- https://quickstarts.snowflake.com/guide/vhol_snowflake_data_wrangler/index.html?index=..%2F..index#1
/*--------------------------------------------------------------------------------
  Snowflake and Amazon SageMaker Data Wrangler VHOL

  This Vitual Hands on Lab consists of configuring a Snowflake account with
  data sets to simulate internal data as well as data from the Snowflake Data Marketplace.
  It also creates the required objects and integraiton to work with SageMaker Data Wrangler.

  Authors:  Andries Engelbrecht
  Updated:  May 24, 2022

--------------------------------------------------------------------------------*/


/* ---------------------------------------------------------------------------
First we configure the user and role that will be used.
To simplify steps we will use a single role and assign all priviles to it.
Typically this will be done by multiple personas and roles in production.
----------------------------------------------------------------------------*/

USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE ML_ROLE COMMENT='ML Role';
GRANT ROLE ML_ROLE TO ROLE SYSADMIN;

CREATE OR REPLACE USER ML_USER PASSWORD='AWSSF123'
	DEFAULT_ROLE=ML_ROLE
	DEFAULT_WAREHOUSE=ML_WH
	DEFAULT_NAMESPACE=ML_WORKSHOP.PUBLIC
	COMMENT='ML User';
GRANT ROLE ML_ROLE TO USER ML_USER;

-- Grant privliges to role
USE ROLE ACCOUNTADMIN;

GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE ML_ROLE;
GRANT IMPORT SHARE ON ACCOUNT TO ML_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE ML_ROLE;

/*-----------------------------------------------------------------------------
Get Snowflake Account Identifier
Then run Create SageMaker & Storage Integration Cloud Formation Template

- This will take a few minutes, we will continue with the lab and come back to it
------------------------------------------------------------------------------*/
--Get Snowflake Region and copy to notepad
SELECT CURRENT_REGION();


--Get Snowflake Account Name and copy to notepad
SELECT CURRENT_ACCOUNT();

--Your Snowflake Account Identifier is
-- Output of current_account dot . current_region
-- i.e. KCA74228.AWS_US_EAST_1

-- DX61972.AWS_CA_CENTRAL_1
-- dx61972.ca-central-1.aws
--

/*VERIFY ACCOUNT ROLE AND USER */
--VERIFY ROLE 

USE ROLE ML_ROLE;

SHOW GRANTS TO ROLE ML_ROLE;
-- NOW RUN Cloudfortmation template


/*---------------------------------------------------------------------------
Next we will create a virual warehouse that will be used
---------------------------------------------------------------------------*/
USE ROLE SYSADMIN;


--Create Warehouse for AI/ML work
CREATE OR REPLACE WAREHOUSE ML_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

GRANT ALL ON WAREHOUSE ML_WH TO ROLE ML_ROLE;



/*-------------------------------------------------------------------------
We now will create a database and schema, as well as objects needed
to load data to simulate internal system data that has been loaded
in Snowflake.
This data is representing lender information as well as data collected 
to see if the lenders defaulted on the loans provided to them.
---------------------------------------------------------------------------*/

--First we will start to set the Snowflake context for execution
USE ROLE ML_ROLE;
USE WAREHOUSE ML_WH;

--Next we will create the first database that represents the internal data 
--from various internal systems that has been loaded into Snowflake

CREATE DATABASE IF NOT EXISTS LOANS_V2;

--Create loan_data table

CREATE OR REPLACE TABLE LOAN_DATA (
    LOAN_ID NUMBER(38,0),
    LOAN_AMNT NUMBER(38,0),
    FUNDED_AMNT NUMBER(38,0),
    TERM VARCHAR(16777216),
    INT_RATE VARCHAR(16777216),
    INSTALLMENT FLOAT,
    GRADE VARCHAR(16777216),
    SUB_GRADE VARCHAR(16777216),
    EMP_TITLE VARCHAR(16777216),
    EMP_LENGTH VARCHAR(16777216),
    HOME_OWNERSHIP VARCHAR(16777216),
    ANNUAL_INC NUMBER(38,0),
    VERIFICATION_STATUS VARCHAR(16777216),
    PYMNT_PLAN VARCHAR(16777216),
    URL VARCHAR(16777216),
    DESCRIPTION VARCHAR(16777216),
    PURPOSE VARCHAR(16777216),
    TITLE VARCHAR(16777216),
    ZIP_SCODE VARCHAR(16777216),
    ADDR_STATE VARCHAR(16777216),
    DTI FLOAT,
    DELINQ_2YRS NUMBER(38,0),
    EARLIEST_CR_LINE DATE,
    INQ_LAST_6MON NUMBER(38,0),
    MNTHS_SINCE_LAST_DELINQ VARCHAR(16777216),
    MNTHS_SINCE_LAST_RECORD VARCHAR(16777216),
    OPEN_ACC NUMBER(38,0),
    PUB_REC NUMBER(38,0),
    REVOL_BAL NUMBER(38,0),
    REVOL_UTIL FLOAT,
    TOTAL_ACC NUMBER(38,0),
    INITIAL_LIST_STATUS VARCHAR(16777216),
    MTHS_SINCE_LAST_MAJOR_DEROG VARCHAR(16777216),
    POLICY_CODE NUMBER(38,0),
    LOAN_DEFAULT NUMBER(38,0),
    ISSUE_MONTH NUMBER(2,0),
    ISSUE_YEAR NUMBER(4,0)
);


--Load data from a public S3 bucket to simulate the internal systems data

--Use an External Stage to Load data from S3
--For simplicity we are using a public S3 bucket
--Please refer to Snowflake documentation for secure options in configuring
-- Storage Integrations and Stages with S3

CREATE OR REPLACE STAGE LOAN_DATA
  url='s3://snowflake-corp-se-workshop/VHOL_Snowflake_Data_Wrangler/V2/data/';


--Load data by using the COPY command

COPY INTO LOAN_DATA FROM @LOAN_DATA/loan_data.csv 
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
    
--Let's have a quick look at the loan data
--Notice all the types of information that are in the columns
--Note column LOAN_DEFAULT that indicates whether the lender defaulted on the loan

SELECT * FROM LOAN_DATA LIMIT 100;



/*-----------------------------------------------------------------------
We can now enrich the loan data by using the Snowflake Data Marketplace.
Let's add unemployment data to provide. 

Using the Preview App subscribe to Marketplace Knoema Labor Data Atlas.

Create database KNOEMA_LABOR_DATA_ATLAS from Marketplace data
-------------------------------------------------------------------------*/

-- The Knoema Marketplace data has a lot of information
-- For the purposes of the workshop we will just focus on the unemployment data

-- Create a Knoema Employment data View
-- To ensure we use the LOANS_V2 databse we will set it

USE LOANS_V2.PUBLIC;

CREATE OR REPLACE VIEW KNOEMA_EMPLOYMENT_DATA AS (
SELECT *
  FROM (SELECT "Measure Name" MeasureName, "Date", "RegionId" State, AVG("Value") Value FROM "KNOEMA_LABOR_DATA_ATLAS"."LABOR"."BLSLA" WHERE "RegionId" is not null and "Date" >= '2018-01-01' AND "Date" < '2018-12-31' GROUP BY "RegionId", "Measure Name", "Date")
	PIVOT(AVG(Value) FOR MeasureName IN ('civilian noninstitutional population', 'employment', 'employment-population ratio', 'labor force', 'labor force participation rate', 'unemployment', 'unemployment rate'))
  	AS p (Date, State, civilian_noninstitutional_population, employment, employment_population_ratio, labor_force, labor_force_participation_rate, unemployment, unemployment_rate)
);


-- We will now create a new table to asociate the specific loan_id to the unployment data at the same time
-- This is done by joining the Knoema employment data view with the existing loan data using state and date

CREATE OR REPLACE TABLE UNEMPLOYMENT_DATA AS
	SELECT l.LOAN_ID, e.CIVILIAN_NONINSTITUTIONAL_POPULATION, e.EMPLOYMENT, e.EMPLOYMENT_POPULATION_RATIO, e.LABOR_FORCE,
    	e.LABOR_FORCE_PARTICIPATION_RATE, e.UNEMPLOYMENT, e.UNEMPLOYMENT_RATE
	FROM LOAN_DATA l LEFT JOIN KNOEMA_EMPLOYMENT_DATA e
    	on l.ADDR_STATE = right(e.state,2) and l.issue_month = month(e.date) and l.issue_year = year(e.date);


-- Quick view of the UNEMPLOYMENT data
-- Note the different labor data columns

SELECT * FROM UNEMPLOYMENT_DATA LIMIT 100;


/*------------------------------------------------------------------------------------------
We will now use Snowflake's unique Zero Copy Cloning functionlity to clone the data set.
This will allow the data science team to work on the full data set without impacting
production users. 
Snowflake can efficiently clone very large data sets using Zero Copy Clone technology as
it only copies the metadata.
This can also be used to create versioning of training data sets for ML models to provide
a complete history of traning data sets used for ML Model versions.
------------------------------------------------------------------------------------------*/

-- Zero Copy Cloning can clone a complete database
-- In this case will first create a new database and then only clone the tables

CREATE OR REPLACE DATABASE ML_LENDER_DATA;
CREATE OR REPLACE SCHEMA ML_LENDER_DATA.ML_DATA;
USE ML_LENDER_DATA.ML_DATA;


--Clone production data for the data science team

CREATE TABLE LOAN_DATA_ML CLONE LOANS_V2.PUBLIC.LOAN_DATA;
CREATE TABLE UNEMPLOYMENT_DATA CLONE LOANS_V2.PUBLIC.UNEMPLOYMENT_DATA;

--Create table for ML_Results
CREATE OR REPLACE TABLE ML_RESULTS (LABEL NUMBER, PREDICTIONS NUMBER, P_DEFAULT FLOAT);


/*-----------------------------------------------------------------------------
Storage integration Information
------------------------------------------------------------------------------*/
-- Get Storage Integration name
SHOW INTEGRATIONS;

-- You can get the S3 bucket and more informaiton
-- Change the INTEGRATION name to what yours is scalled i.e. SMSNOW_STORAGE_INTEGRATION
DESC INTEGRATION <INTEGRATION NAME>;



/*-----------------------------------------------------------------------------
Let's switch over to SageMaker and Data Wrangler
------------------------------------------------------------------------------*/






/*-----------------------------------------------------------------------------
Review ML Results in the table that was written back from SageMaker
------------------------------------------------------------------------------*/

USE ROLE ML_ROLE;
USE ML_LENDER_DATA.ML_DATA;

SELECT * FROM ML_RESULTS;






/*-----------------------------------------------------------------------------
Reset and clean up of account after lab
------------------------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;
-- Default integration name is SMSNOW_STORAGE_INTEGRATION
SHOW INTEGRATIONS;
DROP INTEGRATION <REPLACE_ME_WITH_YOURSTORAGEINT>;
DROP WAREHOUSE ML_WH;
DROP USER ML_USER;
DROP ROLE ML_ROLE;
DROP DATABASE KNOEMA_LABOR_DATA_ATLAS;
DROP DATABASE LOANS_V2;
DROP DATABASE ML_LENDER_DATA;


--------------------
-- https://docs.snowflake.com/en/sql-reference/constructs/order-by.html
order by column1 desc nulls last;
order by column1 desc nulls first;

--
create or replace table TREASURIES(payload_date int, event_ts int);
insert into TREASURIES values(19342, 1671213179049),
                    (19342, 1671213165522),
                    (1671062400000, 1671156018304),
                    (1671062400000, 1671126782361);
                    
SELECT IFF(event_ts >= 1671213165522, DATEADD('day', payload_date, '1970-01-01'), DATEADD('ms', payload_date, '1970-01-01'))::DATE
    FROM TREASURIES;
    
    
SELECT IFF(event_ts < 1671213165522, DATEADD('ms', payload_date, '1970-01-01'), DATEADD('day', payload_date, '1970-01-01'))::DATE
FROM TREASURIES;

----
-- https://docs.snowflake.com/en/developer-guide/snowflake-scripting/variables.html
create or replace procedure get_max_date( )

returns datetime not null

language sql

as

$$

declare

max_date datetime;

begin

select max(last_updated) into :max_date from control_variables;

return max_date;

end

$$;
call get_max_date();

----

CREATE OR REPLACE MASKING POLICY COL_MASKING_POLICY as (val string) returns string ->
CASE WHEN
     EXISTS (SELECT 1 FROM GOVERNANCE_DB.TBLS.MASKING_POLICY WHERE 
    ColmNm='<Column Name>'  AND IS_ROLE_IN_SESSION(RoleNm) = TRUE) THEN val
    ELSE NULL
END;

--- sequence
create or replace sequence seq1;

create or replace table foo (id number, name text);

insert into foo values (seq1.nextval, 'student1'), 
                       (seq1.nextval, 'student2'), 
                       (seq1.nextval, 'student3');
select * from foo;
select seq1.nextval;
select seq1.getnextval;

---
create or replace table emp (empno int, deptno int, name text);
insert into emp values (1, 1, 'Tom'),
                       (2, 1, 'John'),
                       (3, 2, 'Helen'),
                       (4, 2, 'Chelsea'),
                       (5, 3, 'Tim');
create or replace table dept (deptno int, dname text, date_created date);
insert into dept values (1, 'dev', '2019-01-01'::date),
                        (2, 'sales', '2019-02-01'::date),
                        (3, 'finance', '2019-01-15'::date);
 
 select empno,
(select dname from dept a where a.deptno= e.deptno and date_created = (select max(date_created) from dept b where a.deptno =b.deptno ) )as deptno
 from emp e;
 
select empno,
(select dname from dept a where a.deptno= e.deptno and date_created = (select max(date_created) from dept b where a.deptno =b.deptno ) )as deptno
 from emp e;

select empno,
(select dname from dept a where a.deptno= e.deptno)as deptno
 from emp e;

select empno,
(select max(dname) from dept)as deptno
 from emp e;

show grants to ML_ROLE;

----
create or replace table account_table (id int, name text, type text);
insert into account_table values (1, 'IT', ''),
                                 (2, 'Sales',''),
                                 (2, 'Sales', '');
select * from account_table;
select id, count(*) over(partition by id order by id asc) as id_count
    from account_table;
select id, sum(id) over(partition by id order by id asc) as id_count
    from account_table;


----
--
create or replace table a1 (num number);
select count(*) from a1;
--begin name t4;
begin;
select current_transaction();
insert into a1 values (1), (2);
select count(*) from a1;
rollback;
select count(*) from a1;

--
show users;
alter user ml_user set email='ml_user@example.com';
begin;
alter user ml_user set email='change1@example.com';
rollback;

--
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

--
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
    

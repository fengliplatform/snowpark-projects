-- https://docs.snowflake.com/en/developer-guide/native-apps/tutorials/getting-started-tutorial#create-an-application-package
use role accountadmin;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE accountadmin;

-- create app package
CREATE APPLICATION PACKAGE hello_snowflake_package;
SHOW APPLICATION PACKAGES;

-- create chema this app package
USE APPLICATION PACKAGE hello_snowflake_package;
CREATE SCHEMA stage_content;
-- create name stage
CREATE OR REPLACE STAGE hello_snowflake_package.stage_content.hello_snowflake_stage
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1);

-- upload app structure/files to app stage
--
LIST @hello_snowflake_package.stage_content.hello_snowflake_stage/scripts;

-- Modified setup.sql
-- created app role, schema, procedure. Granted permission of schema/procedure to app role

-- start of test 
-- install/create app at local account
CREATE APPLICATION HELLO_SNOWFLAKE_APP
  FROM APPLICATION PACKAGE HELLO_SNOWFLAKE_PACKAGE
  USING '@hello_snowflake_package.stage_content.hello_snowflake_stage';

SHOW APPLICATIONS;

CALL core.hello();
-- end of test

-- create database for app package
USE APPLICATION PACKAGE hello_snowflake_package;
CREATE SCHEMA IF NOT EXISTS hello_snowflake_package.shared_data;
CREATE TABLE IF NOT EXISTS hello_snowflake_package.shared_dataaccounts (ID INT, NAME VARCHAR, VALUE VARCHAR);
INSERT INTO accounts VALUES
  (1, 'Nihar', 'Snowflake'),
  (2, 'Frank', 'Snowflake'),
  (3, 'Benoit', 'Snowflake'),
  (4, 'Steven', 'Acme');
SELECT * FROM hello_snowflake_package.shared_data.accounts;
-- grant persmission to schema/table (provided by app) so the schema/table can be shared to app consumers
GRANT USAGE ON SCHEMA shared_data TO SHARE IN APPLICATION PACKAGE hello_snowflake_package;
GRANT SELECT ON TABLE accounts TO SHARE IN APPLICATION PACKAGE hello_snowflake_package;

-- modify setup.sql adding view
-- upload

--
DROP APPLICATION hello_snowflake_app;

CREATE APPLICATION hello_snowflake_app
  FROM APPLICATION PACKAGE hello_snowflake_package
  USING '@hello_snowflake_package.stage_content.hello_snowflake_stage';

SELECT * FROM code_schema.accounts_view;
SELECT * FROM shared_data.accounts;-- Schema 'HELLO_SNOWFLAKE_APP.SHARED_DATA' does not exist or not authorized.

-- added more to setup.sql
-- upload python/hello_python.py

--
DROP APPLICATION hello_snowflake_app;

CREATE APPLICATION hello_snowflake_app
  FROM APPLICATION PACKAGE hello_snowflake_package
  USING '@hello_snowflake_package.stage_content.hello_snowflake_stage';


-- test
SELECT code_schema.addone(1);
SELECT code_schema.multiply(3,2);

-- added streamlit obj in setup.sql
-- add python streamlit, uploaded

--
DROP APPLICATION hello_snowflake_app;
CREATE APPLICATION hello_snowflake_app
  FROM APPLICATION PACKAGE hello_snowflake_package
  USING '@hello_snowflake_package.stage_content.hello_snowflake_stage';

  -- add version to app package
ALTER APPLICATION PACKAGE hello_snowflake_package
  ADD VERSION v1_0 USING '@hello_snowflake_package.stage_content.hello_snowflake_stage';
show versions in application package hello_snowflake_package;

-- re create app from given version
DROP APPLICATION hello_snowflake_app;
CREATE APPLICATION hello_snowflake_app
  FROM APPLICATION PACKAGE hello_snowflake_package
  USING VERSION V1_0;

-- set default release version
SHOW VERSIONS IN APPLICATION PACKAGE hello_snowflake_package;
ALTER APPLICATION PACKAGE hello_snowflake_package
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = v1_0
  PATCH = 0;
  
-- Create list


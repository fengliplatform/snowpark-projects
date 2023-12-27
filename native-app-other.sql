use role sysadmin;
CREATE DATABASE IP2LOCATION;
CREATE SCHEMA IP2LOCATION.IP2LOCATION;

CREATE TABLE LITEDB11 (
ip_from INT,
ip_to INT,
country_code char(2),
country_name varchar(64),
region_name varchar(128),
city_name varchar(128),
latitude DOUBLE,
longitude DOUBLE,
zip_code varchar(30),
time_zone varchar(8)
);
--Create a file format for the file
CREATE OR REPLACE FILE FORMAT LOCATION_CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
COMPRESSION = AUTO;
--create a stage so we can upload the file
CREATE STAGE LOCATION_DATA_STAGE
file_format = LOCATION_CSV;

----------------
COPY INTO LITEDB11 FROM @LOCATION_DATA_STAGE;
SELECT COUNT(*) FROM LITEDB11;
SELECT * FROM LITEDB11 LIMIT 10;

---------------

USE DATABASE IP2LOCATION;
USE SCHEMA IP2LOCATION;
--create the new stage
CREATE STAGE APPLICATION_STAGE;

-- application package
use role accountadmin;
--create the application package
CREATE APPLICATION PACKAGE IP2LOCATION_APP;
--set context to the application package
USE IP2LOCATION_APP;
CREATE SCHEMA IP2LOCATION_APP.IP2LOCATION;

--we need to create a proxy artefact here referencing the data we want to use
CREATE VIEW IP2LOCATION_APP.IP2LOCATION.LITEDB11
AS
SELECT * FROM IP2LOCATION.IP2LOCATION.LITEDB11;
--Grant the application permissions on the schema we just created
GRANT USAGE ON SCHEMA IP2LOCATION_APP.IP2LOCATION TO SHARE IN APPLICATION PACKAGE IP2LOCATION_APP; 
--grant permissions to the application
GRANT SELECT ON VIEW IP2LOCATION_APP.IP2LOCATION.LITEDB11 TO SHARE IN APPLICATION PACKAGE IP2LOCATION_APP;

--grant permissions on the database where our data resides
GRANT REFERENCE_USAGE ON DATABASE IP2LOCATION TO SHARE IN APPLICATION PACKAGE IP2LOCATION_APP;

-- upload manifest.yml to ip2location.ip2locatin.application_stage;
-- fengliplatform#COMPUTE_WH@IP2LOCATION.IP2LOCATION>put file://manifest.yml @application_stage auto_compress=false;
list @ip2location.ip2location.application_stage; -- application_stage/manifest.yml

-- upload setup_script.sql
-- fengliplatform#COMPUTE_WH@IP2LOCATION.IP2LOCATION>put file://setup_script.sql @application_stage auto_compress=false;
list @ip2location.ip2location.application_stage;

-- create streamlit
-- upload streamlit
-- fengliplatform#COMPUTE_WH@IP2LOCATION.IP2LOCATION>put file://setup_script.sql @application_stage/ui auto_compress=false;
SHOW VERSIONS IN APPLICATION PACKAGE IP2LOCATION_APP;

SHOW RELEASE DIRECTIVES IN APPLICATION PACKAGE IP2LOCATION_APP;

ALTER APPLICATION PACKAGE IP2LOCATION_APP
  ADD VERSION MyFirstVersion
  USING '@IP2LOCATION.IP2LOCATION.APPLICATION_STAGE';

ALTER APPLICATION PACKAGE IP2LOCATION_APP
  DROP VERSION MyFirstVersion;
  
ALTER APPLICATION PACKAGE IP2LOCATION_APP
  ADD VERSION MySecondtVersion
  USING '@IP2LOCATION.IP2LOCATION.APPLICATION_STAGE';

ALTER APPLICATION PACKAGE IP2LOCATION_APP SET DEFAULT RELEASE DIRECTIVE VERSION = MYSECONDTVERSION PATCH = 0;

-- consumer side
-- create app and deploy to local account (same account as app provider)
Drop application APPL_MAPPING;

CREATE  APPLICATION APPL_MAPPING
FROM APPLICATION PACKAGE IP2LOCATION_APP
USING VERSION MYSECONDTVERSION;





-- consumer data
CREATE DATABASE TEST_IPLOCATION;
CREATE SCHEMA TEST_IPLOCATION;

CREATE OR REPLACE TABLE TEST_IPLOCATION.TEST_IPLOCATION.TEST_DATA (
	IP VARCHAR(16),
	IP_DATA VARIANT
);

INSERT INTO TEST_DATA(IP) VALUES('73.153.199.206'),('8.8.8.8');




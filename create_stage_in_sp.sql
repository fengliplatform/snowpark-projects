show stages;

use role accountadmin;
create or replace storage integration s3_integration
 TYPE = EXTERNAL_STAGE
 STORAGE_PROVIDER = S3
 ENABLED = TRUE 
 STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xx:role/Snowflake_read_S3'
 STORAGE_ALLOWED_LOCATIONS = ('s3://feng-public-bucket')
 COMMENT = 'To access S3 by assuming a role';

drop stage feng_s3_integration_stage;


use role sysadmin;
CREATE OR REPLACE stage feng_s3_integration_stage
   url='s3://feng-public-bucket'
   STORAGE_INTEGRATION = s3_integration;

use role accountadmin;
grant usage on integration s3_integration to role sysadmin;

use role sysadmin;
CREATE OR REPLACE stage feng_s3_integration_stage
   url='s3://feng-public-bucket'
   STORAGE_INTEGRATION = s3_integration;

drop stage feng_s3_integration_stage;

create or replace procedure create_stage_sp()
returns varchar
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
as $$

def run (session):
    si = "s3://feng-public-bucket"
    query = f'''
        CREATE OR REPLACE stage feng_s3_integration_stage
            url={si}
            STORAGE_INTEGRATION = s3_integration
    '''
    session.sql(query).collect()
    return 'success'

$$;
call create_stage_sp();
show stages;

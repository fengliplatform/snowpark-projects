drop integration sf_oauth_security_integration;
drop integration sf_oauth_security_integration_2;

use role accountadmin;
CREATE SECURITY INTEGRATION sf_oauth_security_integration
  TYPE=OAUTH
  ENABLED=TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE='CONFIDENTIAL'
  OAUTH_REDIRECT_URI='https://oauth.pstmn.io/v1/browser-callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 86400;

show integrations;
describe security integration sf_oauth_security_integration;

SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SF_OAUTH_SECURITY_INTEGRATION');

ALTER USER fengliplatform ADD DELEGATED AUTHORIZATION
    OF ROLE sysadmin
    TO SECURITY INTEGRATION SF_OAUTH_SECURITY_INTEGRATION;
    
grant all on integration SF_OAUTH_SECURITY_INTEGRATION to role sysadmin;

describe user fengliplatform;
alter user fengliplatform set default_role=sysadmin;


show tables;
select * from actor_table;












CREATE SECURITY INTEGRATION sf_oauth_security_integration_2
  TYPE=OAUTH
  ENABLED=TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE='CONFIDENTIAL'
  OAUTH_REDIRECT_URI='https://oauth.pstmn.io/v1/browser-callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 86400;

describe integration sf_oauth_security_integration_2;

SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SF_OAUTH_SECURITY_INTEGRATION_2');


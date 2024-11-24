create or replace api integration rotate_keypair_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::159407359917:role/snowflake_external_function_for_api_gateway_role'
api_allowed_prefixes = ('https://v46fg48ap5.execute-api.us-east-1.amazonaws.com/v1/rotate_keypair')
enabled = true
;
desc integration rotate_keypair_api_integration;

-- external function
create or replace external function rotate_keypair_external_function (user_name varchar)
returns variant
api_integration = rotate_keypair_api_integration
as 'https://v46fg48ap5.execute-api.us-east-1.amazonaws.com/v1/rotate_keypair'
;

create user user10;
create user user1;
show users like 'user1';

select rotate_keypair_external_function('user1');

create or replace procedure set_user_public_key(user_name varchar)
returns varchar
language python
runtime_version=3.10
packages=('snowflake-snowpark-python','pandas')
handler='set_public_key'
as $$
import pandas as pd
def set_public_key(session, user_name):
    public_key = pd.DataFrame(session.sql(f'''
                  select rotate_keypair_external_function('{user_name}')
                ''').collect()).iloc[0,0]
    public_key = public_key[1:-1]
    my_sql = f'''alter user {user_name} set rsa_public_key='{public_key}' '''
    session.sql(my_sql).collect()
    return public_key
$$;
call set_user_public_key('user10');

alter user user1 set rsa_public_key='MII...kiQIDAQAB';

describe user fengliplatform;

select trim((select "value" from table(result_scan(last_query_id))) where "property" = 'RAS_PUBLIC_KEY_FP'), 'SHA256');

use role accountadmin;
CREATE PASSWORD POLICY USER_PASSWORD_POLICY_1
    PASSWORD_MAX_AGE_DAYS = 90
    COMMENT = 'normal user password policy';
    
describe password policy USER_PASSWORD_POLICY_1;

use role accountadmin;
show users;
create or replace user user1 password='user123';

describe user user1;

alter user user1 set password policy USER_PASSWORD_POLICY_1;

--
use role accountadmin;
create or replace procedure rotate_pki_keypair_sp (user_name varchar)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python', 'cryptography')
HANDLER = 'run'
execute as owner
AS
$$
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

def run(session, user_name):
  private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048
  )

  pem_private_key = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
  )

  pem_public_key = private_key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
  )

  pem_private_key_decoded = pem_private_key.decode("utf-8")
  pem_public_key_decoded = pem_public_key.decode("utf-8")
  
  public_key=''.join(pem_public_key_decoded.split('\n')[1:-2])
  query_set_public_key = f'alter user {user_name} set RSA_PUBLIC_KEY = \'{public_key}\''
  session.sql(query_set_public_key).collect();

  return pem_private_key_decoded
$$;

call rotate_pki_keypair_sp('user1');

grant usage on database fengdb to role public;
grant usage on schema fengdb.public to role public;
grant usage on procedure fengdb.public.rotate_pki_keypair_sp(varchar) to role public;

grant usage on warehouse compute_wh to role public;

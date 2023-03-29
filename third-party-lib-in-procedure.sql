create or replace stage feng_stage;
create or replace table pay (job_name text, annual_wage float, country text);
insert into pay values('Data Engineer', 90000, 'CANADA'), 
                      ('Software Engineer',110000,'CANADA'),
                      ('Software Engineer',100000, 'United States'),
                      ('Data Engineer', 120000, 'United States');
create or replace table international_gdp (per_capita_gdp float, name text);
insert into international_gdp values(100000, 'CA'),(110000, 'US');
select * from pay;
select * from international_gdp;

create or replace procedure query_country_code (job_name text, wage float)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python', 'pandas')
imports = ('@feng_stage/pycountry-main.zip')
HANDLER = 'run'
AS
$$
import sys
import zipfile
import pandas as pd

import_dir = sys._xoptions.get("snowflake_import_directory")
target_dir = "/tmp/pycountry_src_package"
with zipfile.ZipFile(import_dir + "pycountry-main.zip", 'r') as pycountry_zip_file:
    pycountry_zip_file.extractall(target_dir)

sys.path.append(target_dir + '/pycountry-main/src')

import pycountry

def run(session, job_name, annual_wage):
  sf_df_results = pd.DataFrame(session.sql(f'''select country from pay 
                    where annual_wage='{annual_wage}' and job_name='{job_name}'
                  ''').collect()).iloc[0,0]
  
  return str(pycountry.countries.search_fuzzy(sf_df_results)[0].alpha_2)
  
$$;
call query_country_code('Data Engineer', 90000);

----------------


create or replace function PROCESS_COUNTRY_TEST(country_value varchar)
   returns varchar
   language python
   runtime_version = 3.8
   handler = 'process_country'
   packages = ('snowflake-snowpark-python')
   imports = ('@feng_stage/pycountry-main.zip')
   as
$$
import fcntl
import os
import sys
import threading
import zipfile

# File lock class for synchronizing write access to /tmp
class FileLock:
   def __enter__(self):
      self._lock = threading.Lock()
      self._lock.acquire()
      self._fd = open('/tmp/lockfile.LOCK', 'w+')
      fcntl.lockf(self._fd, fcntl.LOCK_EX)

   def __exit__(self, type, value, traceback):
      self._fd.close()
      self._lock.release()

# Get the location of the import directory. Snowflake sets the import
# directory location so code can retrieve the location via sys._xoptions.
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

# Get the path to the ZIP file and set the location to extract to.
zip_file_path = import_dir + "pycountry-main.zip"
extracted = '/tmp/pycountry_pkg_dir'

# Extract the contents of the ZIP. This is done under the file lock
# to ensure that only one worker process unzips the contents.
with FileLock():
   if not os.path.isdir(extracted + '/pycountry-main'):
      with zipfile.ZipFile(zip_file_path, 'r') as myzip:
         myzip.extractall(extracted)

sys.path.append(extracted + '/pycountry-main/src')

import pycountry

def process_country(country_value) -> str:
    return str(pycountry.countries.search_fuzzy(country_value)[0].alpha_2)
    

$$;

select PROCESS_COUNTRY_TEST('GBR');
select PROCESS_COUNTRY_TEST('CANADA');

-------------
create or replace table pay (job_name text, annual_wage float, country text);
insert into pay values('Data Engineer', 90000, 'CA'), 
                      ('Software Engineer',110000,'CA'),
                      ('Software Engineer',100000, 'US'),
                      ('Data Engineer', 120000, 'US');

create or replace procedure query_wage_from_pay_table (job_name text, country_name text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python', 'pandas==1.5.3')
HANDLER = 'run'
AS
$$
import pandas as pd

def run(session, job_name, country_name):
  sf_df_results = pd.DataFrame(session.sql(f'''select annual_wage from pay 
                    where country='{country_name}' and job_name='{job_name}'
                  ''').collect()).iloc[0,0]
  return sf_df_results
$$;

call query_wage_from_pay_table('Data Engineer', 'CA');


select * from information_schema.packages where language = 'python';

select * from information_schema.packages where (package_name = 'pandas' and language = 'python');

-------------------------
create or replace procedure test_mysql_connector ()
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'psycopg2', 'cx_oracle')
imports = ('@feng_stage/mysql_connector_lib.zip')
HANDLER = 'run'
AS
$$
import sys
import zipfile
import pandas as pd

import_dir = sys._xoptions.get("snowflake_import_directory")
target_dir = "/tmp/mysql_connector"
with zipfile.ZipFile(import_dir + "mysql_connector_lib.zip", 'r') as mysql_connector_zip_file:
    mysql_connector_zip_file.extractall(target_dir)

sys.path.append(target_dir + '/mysql_connector_lib')

import mysql.connector
import psycopg2
import cx_Oracle

def run(session):
  return "success"
  
$$;
call test_mysql_connector();



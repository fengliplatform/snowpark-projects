create or replace stage feng_stage;

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

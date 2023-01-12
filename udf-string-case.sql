CREATE OR REPLACE function "TEST2"()
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'urllib3')
HANDLER = 'TESTING'
as
$$
import requests
 
def TESTING():
    url       = "http://142.251.32.78/"
    result    = requests.get(url)
    return(str(result.text))
$$;
 
select TEST2();


select to_date(concat(year(current_date), '-01-01'));

alter session set week_of_year_policy=1, week_start=7;
select week(to_date(concat(year(current_date), '-01-01')));


select length('TheMisSioNs');
select translate('TheMisSioNs','ABCDEFGHIJKLMNOPQRSTUVWXYZ','''');
select length(translate('TheMisSioNs','ABCDEFGHIJKLMNOPQRSTUVWXYZ',''''));
select length('TheMisSioNs') - length(translate('TheMisSioNs','ABCDEFGHIJKLMNOPQRSTUVWXYZ','''')) as uppercase_letters
      ,length('TheMisSioNs') - length(translate('TheMisSioNs','abcdefghijklmnopqrstuvwxyz','''')) as lowercase_letters
;

-- https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-creating.html

-- 1 procedure
create or replace procedure compare_json (json1 text, json2 text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session, json1, json2):
  json1_load=json.loads(json1)
  json2_load=json.loads(json2)
  if sorted(json1_load.items()) == sorted(json2_load.items()):
      return "Equal"
  else:
      return "Not Equal!"
$$;

set json1='{"Name":"ABC", "Age":"30"}';
set json2='{"Name":"ABC", "Age":"20"}';
select $json1;
select $json2;

call compare_json($json1, $json2);


-- 2 udf
create or replace function compare_json_func (json1 text, json2 text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(json1, json2):
  json1_load=json.loads(json1)
  json2_load=json.loads(json2)
  if sorted(json1_load.items()) == sorted(json2_load.items()):
      return "Equal"
  else:
      return "Not Equal!"
$$;

set json1='{"Name":"ABC", "Age":"30"}';
set json2='{"Name":"ABC", "Age":"20"}';
select $json1;
select $json2;

select compare_json_func($json1, $json2);

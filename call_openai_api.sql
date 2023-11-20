PS C:\Users\feng\workspace\snowflake\external_access> curl https://api.openai.com/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer <API key>" -d '{
>>      "model": "gpt-3.5-turbo",
>>      "messages": [{"role": "user", "content": "Classify this sentiment: OpenAI is awesome!"}],
>>      "temperature": 0.7
>> }'
{
  "id": "chatcmpl-xxxx",
  "object": "chat.completion",
  "created": 1700432367,
  "model": "gpt-3.5-turbo-0613",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "The sentiment \"OpenAI is awesome!\" is classified as positive."
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 17,
    "completion_tokens": 13,
    "total_tokens": 30
  }
}



-- external access integration - OpenAI
use feng_database;
use schema feng_database.feng_schema;

use role accountadmin;
-- create network rule to enable access to OpenAI API URI
CREATE OR REPLACE NETWORK RULE OPENAI_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.openai.com');
  
-- create external access integration using above network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION OPENAI_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (OPENAI_NETWORK_RULE)
  ENABLED = TRUE;
 
GRANT USAGE ON INTEGRATION OPENAI_ACCESS_INTEGRATION TO ROLE SYSADMIN;
show integrations;

-- create UDF using EXTERNAL_ACCESS_INTEGRATIONS parameter
-- so UDF is able to call OpenAI APIs
use role sysadmin;
CREATE OR REPLACE function feng_database.feng_schema.function_openai_completions(my_sentence text)
RETURNS text
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'openai_completions'
EXTERNAL_ACCESS_INTEGRATIONS = (OPENAI_ACCESS_INTEGRATION)
AS $$
import snowflake.snowpark as snowpark
import requests
import json

def call_completions_api(url, my_sentence):
    my_header = {'Authorization': 'Bearer <OpenAI API Key>'}
    my_payload_dict = {"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": my_sentence}], "temperature": 0.7}
    
    json_data = requests.post(url, headers = my_header, json=my_payload_dict).json()
    return json_data["choices"][0]["message"]["content"]
    
def openai_completions(my_sentence): 
    openai_completions_url = "https://api.openai.com/v1/chat/completions"
    
    # read json data from api
    sentimental_result_json_data = call_completions_api(openai_completions_url, my_sentence)

    return sentimental_result_json_data
$$;

-- Call this UDF
select function_openai_completions('Classify this sentiment: OpenAI is awesome!');

# Jupyter nodebook code

import boto3

AWS_REGION = 'us-east-1'
# boto3 looks for AWS credentions from ~/.aws/credentials where you set access key and secret key
dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)


table = dynamodb_resource.Table('customer')
response = table.scan()
customer_data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    customer_data.extend(response['Items'])
    
customer_data

from dynamodb_json import json_util as json
customer_pd_df = pd.DataFrame(json.loads(customer_data))
customer_pd_df

from snowflake.snowpark.session import Session

def create_session_object():
   connection_parameters = {
      "account": "xxxx.us-east-1",
      "user": "yyyy",
      "password": "zzzz",
      "role": "sysadmin",
      "warehouse": "compute_WH",
      "database": "fengdb",
      "schema": "public"
   }
   session = Session.builder.configs(connection_parameters).create()
   print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
   return session

session = create_session_object() 

customer_sp_df = session.create_dataframe(customer_pd_df)
customer_sp_df

customer_sp_df.write.save_as_table(
    table_name='customer_table_from_dynamodb',
    mode='overwrite'
)


    

use role sysadmin;
CREATE OR REPLACE PROCEDURE feng_database.feng_schema.sp_load_fdc_data()
RETURNS varchar
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'requests')
HANDLER = 'load_fdc_data'
EXTERNAL_ACCESS_INTEGRATIONS = (FDC_ACCESS_INTEGRATION)
AS $$
import snowflake.snowpark as snowpark
import pandas as pd
import requests

def read_fdc_api(url):
    json_data = requests.get(url).json()
    return json_data["foods"]

def pd_reads_json(fdc_json_data):
    pd_df = pd.DataFrame(fdc_json_data)
    return pd_df

def pd_write_to_sf(sf_session, pd_df):
    snowpark_df = sf_session.create_dataframe(pd_df)
    snowpark_df.write.save_as_table(
        table_name='fdc_cheddar_cheese_table',
        #table_type="temporary",
        mode='overwrite'
    )

def load_fdc_data(session: snowpark.Session): 
    fdc_cheddar_cheese_url = "https://api.nal.usda.gov/fdc/v1/foods/search?api_key=xxxx&query=Cheddar%20Cheese"
    # read json data from api
    fdc_cheddar_cheese_json_data = read_fdc_api(fdc_cheddar_cheese_url)

    # read json to Pandas df
    fdc_cheddar_cheese_pd_df = pd_reads_json(fdc_cheddar_cheese_json_data)
    print(fdc_cheddar_cheese_pd_df)

    # sf
    pd_write_to_sf(session, fdc_cheddar_cheese_pd_df)

    return "Success!"

$$;

call sp_load_fdc_data();


show tables;
select * from FDC_CHEDDAR_CHEESE_TABLE;

## https://community.snowflake.com/s/question/0D7Do000000cEX8KAM/detail

# Create a DataFrameReader using the Session
 df_reader = DataFrameReader(session)
    
# Create a Schema that has only one String column 
schema = StructType([StructField("all", StringType())])
        
# Read the CSV file from Snowflake into a DataFrame
result = df_reader.option('field_delimiter', None).schema(schema).csv(f'@{stage_name}/{file_name}').collect()
 
#Read result row objects to strings
csv_data = [row[0] for row in result]
    
# Create the buffer object
buffer = io.StringIO()
    
# Write the strings to the buffer
for s in csv_data:
  buffer.write(s+'\n')      
 
# Reset the buffer's position to the beginning
buffer.seek(0)
  
# Convert the reader object to a dataframe, using the headers as the column names
df = pd.read_csv(buffer, delimiter=delimiter, quotechar=quotechar)


## there is another relavent Medium post
## https://medium.com/snowflake/loading-csv-as-semi-structured-files-in-snowflake-d7d76dfc37bf

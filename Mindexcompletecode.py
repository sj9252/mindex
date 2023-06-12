#!/usr/bin/env python
# coding: utf-8

# In[18]:


get_ipython().system('pip install boto3==1.17.99')
get_ipython().system('pip install pandas==1.3.0')
get_ipython().system('pip install sqlalchemy==1.4.16')
get_ipython().system('pip install psycopg2-binary --no-cache-dir')


# In[19]:


import boto3
import pandas as pd
import io
from sqlalchemy import create_engine
import psycopg2


from io import BytesIO
from io import StringIO

class DataProcessor:
    def __init__(self, access_key, secret_key, database, user, password, host, port):
        self.access_key = access_key
        self.secret_key = secret_key
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        
    # Establish connection with AWS S3
    def connect_to_s3(self, access_key, secret_key):
        print('Connecting to aws')
        print('-----------------')
        
        return boto3.client('s3',
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)
    
    # Rename columns in DataFrame
    
    def rename_dataframe_columns(self, dataframe, rename_dict):
        print('Renamed dataframes accordingly')
        print('-----------------')

        return dataframe.rename(columns=rename_dict)
    
    # Merge multiple DataFrames
    def merge_dataframes(self, dfs, merge_key='Week', merge_how='left'):
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = pd.merge(merged_df, df, on=merge_key, how=merge_how)
        print('Merged all files')
        print('-----------------')
        return merged_df
    
    # Replace values in a DataFrame
    def replace_dataframe_values(self, dataframe, replacement_dict):
        print("Replace 1 to win and 0 to loss")
        print('-----------------')
        return dataframe.replace(replacement_dict)
    
    # Establish connection with PostgreSQL database
    def connect_to_postgres(self):
        conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        return conn
    
    # Display list of tables in the database
    def display_table_names(self):
        # Create connection to PostgreSQL database
        print('List of table names')
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        # Fetch table names from the public schema
        cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
        table_names = cursor.fetchall()

        for table in table_names:
            print(table[0])

        # Close the cursor and database connection
        print('-----------------')
        cursor.close()
        conn.close()
        
     # Delete column names and data from table if it exists
    def delete_columns_and_data(self):
        conn = self.connect_to_postgres()
        cur = conn.cursor()
        
        print('Any columns or data in the table deleted if exists')
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'shakti_jagadish'")
        columns = cur.fetchall()

        for column in columns:
            cur.execute(f"ALTER TABLE shakti_jagadish DROP COLUMN {column[0]}")

        cur.execute("DELETE FROM shakti_jagadish")

        conn.commit()
        
       
        print('-----------------')

        cur.close()
        conn.close()
        
    # Check if column names exist in the database after adding columns and data
    def check_column_names(self):
        print('Check if column name exists')
        conn = self.connect_to_postgres()
        cur = conn.cursor()

        schema_name = 'public'
        table_name = 'shakti_jagadish'
        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"

        cur.execute(query)
        column_names = [row[0] for row in cur.fetchall()]

        for column in column_names:
            print(column)
            
        print('-----------------')

        cur.close()
        conn.close()
        
    def add_columns_to_table(self):
        # Create connection to PostgreSQL database
        
        conn = self.connect_to_postgres()
        cur = conn.cursor()
        
        print('Add columns to table')
       
        # Get the column names from the DataFrame
        column_names = merged_df.columns.tolist()
        
       

        # Generate the ALTER TABLE statements to add columns
        alter_table_statements = [f"ALTER TABLE shakti_jagadish ADD COLUMN {column_name} VARCHAR(255);" for column_name in column_names]

        # Execute the ALTER TABLE statements
       
        for statement in alter_table_statements:
            cur.execute(statement)

        # Commit the changes
        conn.commit()
        
       
        print('-----------------')


        # Close the cursor and connection
        cur.close()
        conn.close()
        
    def load_data_to_table(self):
        
        print("Data load started")
        
        # Create connection to PostgreSQL database
        conn = self.connect_to_postgres()
        cur = conn.cursor()
       
        # Create a buffer to hold the DataFrame data
        buffer = StringIO()
        merged_df.to_csv(buffer, index=False, header=False, sep='\t')

        # Reset the buffer position to the start
        buffer.seek(0)

        
        cur.copy_from(buffer,'shakti_jagadish', sep='\t')

        # Commit the changes
        conn.commit()
        print('-----------------')


        # Close the cursor and connection
        cur.close()
        conn.close()
        
    def check_loaded_data(self):
        
        print("Check for loaded data")
        
        # Create connection to PostgreSQL database
        conn = self.connect_to_postgres()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM shakti_jagadish LIMIT 5")

        # Fetch the records
        records = cur.fetchall()

        # Print the fetched records
        for row in records:
            print(row)
            
        print('-----------------')

        # Close the cursor and connection
        cur.close()
        conn.close()
        
        
if __name__ == "__main__":
    
    data_processor = DataProcessor(
    access_key='AKIAZZ33YB65GZIN656A',
    secret_key='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',
    database='postgres',
    user='shakti_jagadish',
    password='jhaktisagadish',
    host='ls-2619b6b15c9bdc80a23f6afb7eee54cf0247da21.ca3yee6xneaj.us-east-1.rds.amazonaws.com',
    port='5432')

    # Call the connect_to_s3 function
    s3_client = data_processor.connect_to_s3(access_key=data_processor.access_key, secret_key=data_processor.secret_key)


    # Download files from S3 bucket and read them into DataFrames
    bucket_name = 'mindex-data-analytics-code-challenge'
    file_keys = ['bengals.csv', 'boyd_receiving.csv', 'chase_receiving.csv', 'higgins_receiving.csv']

    dfs = []  # List to store the DataFrames

    for file_key in file_keys:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        dfs.append(df)

    df, df1, df2, df3 = dfs  # Assign each DataFrame to respective variables

    # Rename columns in df1, df2, and df3
    rename_dict1 = {'Yards': 'Boyd_Yards', 'TD': 'Boyd_TD'}
    rename_dict2 = {'Yards': 'Chase_Yards', 'TD': 'Chase_TD'}
    rename_dict3 = {'Yards': 'Higgins_Yards', 'TD': 'Higgins_TD'}

    df1 = data_processor.rename_dataframe_columns(df1, rename_dict1)
    df2 = data_processor.rename_dataframe_columns(df2, rename_dict2)
    df3 = data_processor.rename_dataframe_columns(df3, rename_dict3)

    # Merge the downloaded DataFrames
    merged_df = data_processor.merge_dataframes([df, df1, df2, df3])

    # Replace values in the DataFrame
    replacement_dict = {'Result': {1.0: 'Win', 0.0: 'Loss'}}
    merged_df = data_processor.replace_dataframe_values(merged_df, replacement_dict)



    # Display table names
    data_processor.display_table_names()



    #Delete column names and data inside the table if exists
    data_processor.delete_columns_and_data()



    # Call the check_column_names function
    data_processor.check_column_names()

    # Add column names to the table
    data_processor.add_columns_to_table()

    # Call the check_column_names function to check if columns are loaded
    data_processor.check_column_names()

    # Load the data to tablle
    data_processor.load_data_to_table()

    # Call the check_loaded_data function to check if columns are loaded
    data_processor.check_loaded_data()




    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





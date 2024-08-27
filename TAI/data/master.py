# data_master.py
from TAI.utils import ConfigLoader

import json, os, sys, inspect, logging
from datetime import datetime

import pandas as pd
import polars as pl
import psycopg2

import boto3
from botocore.exceptions import NoCredentialsError

logging.basicConfig(level=logging.INFO)

class RedshiftAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.conn = psycopg2.connect(
            host=config_loader.get_config('Redshift', 'host'),
            user=config_loader.get_config('Redshift', 'user'),
            port=config_loader.get_config('Redshift', 'port'),
            password=config_loader.get_config('Redshift', 'password'),
            database=config_loader.get_config('Redshift', 'database')
        )

    def get_connection(self):
        return self.conn
    
class Redshift:
    def __init__(self, use_connection_pooling=True):
        self.auth = RedshiftAuth()
        self.conn = self.auth.get_connection()
        self.use_connection_pooling = use_connection_pooling
    
    def run_sql(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        if not self.use_connection_pooling:
            self.conn.close()
        return pd.DataFrame(rows, columns=columns)

class DataMaster:
    def __init__(self):
        pass
    
    def run_sql_cache(self, csv_path, query, refresh=False): #check if csv is exist, read the csv instead of running the query
        if refresh==True:
            return Redshift.run_sql(query).to_csv(csv_path, header=True, index=False)
        else:
            if os.path.exists(csv_path): 
                df = pd.read_csv(csv_path)
            else:
                df = Redshift.run_sql(query)
                df.to_csv(csv_path, header=True, index=False)
                return df
            
    def get_current_dir(self):
        """
        This function returns the directory of the user's script that is running.
        """
        try:  # Adjusting to check if the stack has enough frames
            if len(inspect.stack()) > 2:
                # Get the filename of the script that called this function two levels up in the stack
                caller_file = inspect.stack()[2].filename
            else:# If the stack isn't deep enough, fallback to the immediate caller
                caller_file = inspect.stack()[1].filename  # Get the directory of that script
            current_directory = os.path.dirname(os.path.abspath(caller_file))
            return current_directory
        except IndexError: # Fallback: if there's any issue, return the current working directory
            return os.getcwd()

    def create_dir(self, full_path=None, dir_name="data"): # dir_name = "" to save to the current dir
        """
        If full_path is not provided, create a "data" folder (default, but can change) 
        under the directory where the user's script is run. 
        The full_path should include the folder name, not a file name.
        If the folder already exists, it does nothing.
        """
        if full_path == None:            # Use current folder as the base path
            folder_path = os.path.join(self.get_current_dir(), dir_name)
        else:
            folder_path == full_path
    
        if not os.path.exists(folder_path):        # Create the folder if it doesn't exist
            os.makedirs(folder_path)
            print(f"Folder created: {folder_path}")
        else:
            print(f"Folder already exists: {folder_path}")

    def save_data(self, 
                df, 
                file_name,
                type="parquet",
                dir_name="data", 
                how = "in_dir"): # in_dir, or calendar
        # locate the data folder
        default_path = os.path.join(self.get_current_dir(), dir_name)
        # print (default_path)
        if how =='calendar':
            today = datetime.now()
            year, month, day = today.year, today.month, today.day
            dir_path = os.path.join(default_path, f"{year}", f"{month:02d}", f"{day:02d}")
            # print (dir_path)
            os.makedirs(dir_path, exist_ok=True)
        if how =='in_dir':
            dir_path = default_path
            # self.create_dir() # NEED FIX, when calling from the library, it will create dir under library folder
        
        file_path = os.path.join(dir_path, f"{file_name}.{type}")
        if type == "parquet":
            df.to_parquet(file_path)
        elif type == "csv":
            df.to_csv(file_path)#, header=True, index=False)
        else:
            raise ValueError(f"Unsupported file format: {type}")

    def set_s3_client(aws_session=None):
        return aws_session.client('s3') if aws_session else None
    
    def list_files_in_directory(data_folder):
        # Initialize an empty list to store the filenames
        # # Example usage
        # directory_path = '/path/to/your/directory'  # Replace with your directory path
        # file_list = list_files_in_directory(directory_path)
        # print(file_list)

        file_list = []
        for root, dirs, files in os.walk(data_folder):  # Walk through the directory
            for file in files:   # Check if the file ends with .csv or .parquet
                if file.endswith('.csv') or file.endswith('.parquet'):
                    file_list.append(file)   # Append the filename to the list
        return file_list

    def polars_load_dfs(self, data_folder,list_tables=None, load_all=True): #if list_tables have value, load_all has to be False
        if load_all == True:
            list_tables = self.list_files_in_directory(data_folder)
        else:
            list_tables = list_tables

        dataframes = {}
        for table_name in list_tables:
            csv_file = os.path.join(data_folder, f'{table_name}.csv')
            parquet_file = os.path.join(data_folder, f'{table_name}.parquet')
            if os.path.exists(csv_file):
                dataframes[table_name] = pl.read_csv(csv_file)
            elif os.path.exists(parquet_file):
                dataframes[table_name] = pl.read_parquet(parquet_file)
            else:
                raise FileNotFoundError(f"No data file found for table '{table_name}' in folder '{data_folder}'.")
        return dataframes
    
    def polars_run_query(self, polars_dfs, sql_query):
        # example : porlars_run_query(dfs, """SELECT * FROM table1""")
        context = pl.SQLContext(self.polars_dfs)
        try:
            result = context.execute(sql_query).collect()
            return result
        except Exception as e:
            raise f"Query execution error: {e}"

    def load_from_local(self, file_name, file_format, base_path='in_dir'):
        if base_path =='in_dir':
            dir_path = os.path.join(self.get_current_dir(), 'data')

        full_path = os.path.join(dir_path, file_name)

        if file_format == 'csv':
            return pd.read_csv(full_path)
        elif file_format == 'parquet':
            return pd.read_parquet(full_path)

    def save_to_s3(self, bucket_name, object_name, data, aws_session=None):
        s3_client = self.set_s3_client(aws_session)
        try:
            s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
            logging.info(f"File {object_name} uploaded to {bucket_name}")
        except NoCredentialsError:
            logging.error("Credentials not available")

    def load_from_s3(self, bucket_name, file_key, file_format, aws_session=None):
        s3_client = self.set_s3_client(aws_session)
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            if file_format == 'csv':
                return pd.read_csv(obj['Body'])
            elif file_format == 'parquet':
                return pd.read_parquet(obj['Body'])
        except Exception as e:
            logging.error(f"Error loading from S3: {e}")

class Embedding: #STILL WORKING IN PROGRESS
    def __init__(self, aws_bedrock, config_file):
        """
        Init Embedding with json config file.
        """
        with open(config_file, 'r') as file:
            config = json.load(file)
        
        self.aws_bedrock = aws_bedrock
        self.text_embedding_method = self._get_embedding_method(config['text'])
        self.table_embedding_method = self._get_embedding_method(config['table'])
        self.table_embeddings = {}

    def _get_embedding_method(self, form):
        """ get embedding method from JSON file, methods are different between text and table embedding """
        if form == 'sentence_transformers':
            return self._sentence_transformer_embedding
        elif form == 'aws_bedrock':
            return self._aws_bedrock_embedding
        else:
            raise ValueError(f"Unknown embedding method: {form}")

    def generate_embedding(self, form, method, text_input):
        embedding_array = self.aws_bedrock.generate_embedding(text_input)
        pass

# Example usage:
if __name__ == "__main__":
    aws_session = boto3.Session()
    data_master = DataMaster(aws_session=aws_session)

    # Save data locally
    df_example = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    data_master.save_data({'example': df_example}, file_format='csv')

    # Load data locally
    df_loaded = data_master.load_from_local('data/2024/07/25/example.csv', 'csv')
    print(df_loaded)

    # Save data to S3
    data_master.save_to_s3('your-bucket-name', 'example.csv', df_example.to_csv(index=False))

    # Load data from S3
    df_loaded_s3 = data_master.load_from_s3('your-bucket-name', 'example.csv', 'csv')
    print(df_loaded_s3)

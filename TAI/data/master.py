# data_master.py
from TAI.utils import ConfigLoader

import json, os, sys, inspect, logging
from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd
import numpy as np
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

    def create_dir(self, method: str = 'data', 
                parent_dir: str = '.', 
                bucket_name: str = None, 
                s3_directory: str = '', 
                aws_region: str = 'us-west-2', 
                is_s3: bool = False):
        """
        Creates a directory either locally or in S3 based on the specified method.

        Parameters:
        - method: The method for creating the directory. Options are 'data' or 'calendar'.
        - parent_dir: The parent directory where the folder will be created. Defaults to the current directory.
        - bucket_name: The S3 bucket name. Required if creating in S3.
        - s3_directory: The directory in the S3 bucket where the folder should be created. Used only if is_s3 is True.
        - aws_region: The AWS region where the S3 bucket is located (default: 'us-west-2').
        - is_s3: If True, creates the directory in S3. If False, creates the directory locally.
        """
        if method == 'data':
            dir_name = 'data'
        elif method == 'calendar':
            now = datetime.now()
            dir_name = f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"
        else:
            raise ValueError("Invalid method. Use 'data' or 'calendar'.")

        if is_s3:
            if not bucket_name:
                raise ValueError("bucket_name is required when creating directories in S3.")
            
            s3_client = boto3.client('s3', region_name=aws_region)
            s3_path = f"{s3_directory}/{dir_name}/" if s3_directory else f"{dir_name}/"

            # Check if the directory already exists
            result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_path, Delimiter='/')
            if 'CommonPrefixes' in result:
                print(f"Directory already exists at S3://{bucket_name}/{s3_path}")
            else:
                # Create an empty object with a trailing slash to signify a directory in S3
                s3_client.put_object(Bucket=bucket_name, Key=s3_path)
                print(f"Directory created at S3://{bucket_name}/{s3_path}")
        else:
            full_path = os.path.join(parent_dir, dir_name)
            os.makedirs(full_path, exist_ok=True)
            print(f"Directory created at {full_path}")
    # create_dir(method='data', parent_dir='')
    # create_dir(method='data', bucket_name='jtrade1-dir', s3_directory='data/dd', is_s3=True)
    # create_dir(method='calendar', parent_dir='data/')
    # create_dir(method='calendar', bucket_name='jtrade1-dir', s3_directory='data', is_s3=True)

    def dir_locator(self, method: str = 'data', 
                          parent_dir: str = '.', 
                          bucket_name: str = None, 
                          s3_directory: str = '', 
                          aws_region: str = 'us-west-2', 
                          is_s3: bool = False) -> str:
        """
        Locates the directory created by create_dir either locally or in S3.
        """
        if method == 'data':
            dir_name = 'data'
        elif method == 'calendar':
            now = datetime.now()
            dir_name = f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"
        else:
            raise ValueError("Invalid method. Use 'data' or 'calendar'.")

        if is_s3:
            if not bucket_name:
                raise ValueError("bucket_name is required when locating directories in S3.")
            
            s3_path = f"{s3_directory}/{dir_name}/" if s3_directory else f"{dir_name}/"
            print(f"Directory located at S3://{bucket_name}/{s3_path}")
            return s3_path
        else:
            full_path = os.path.join(parent_dir, dir_name)
            print(f"Directory located at {full_path}")
            return full_path
    # dm.dir_locator(method='data', parent_dir='')
    # dm.dir_locator(method='calendar', bucket_name='jtrade1-dir', s3_directory='', is_s3=True)

    def list_files(self, data_folder: str, 
                        from_s3: bool = False, 
                        aws_region: str = 'us-west-2'):
        """
        Lists all CSV and Parquet file names from a local directory or a selected S3 folder.
        Parameters:
        - data_folder: Path to the local directory or 'bucket_name/s3_directory' if from_s3 is True.
        - from_s3: If True, list files from S3. If False, list files from the local directory.
        - aws_region: The AWS region where the S3 bucket is located (default: 'us-east-1').
        Returns:
        - A list of file names from the specified data source (local directory or S3).
        """
        file_list = []
        if from_s3:   # Parse the bucket name and S3 directory from data_folder
            bucket_name, s3_directory = data_folder.split('/', 1)
            s3_client = boto3.client('s3', region_name=aws_region)  # Initialize S3 client
            list_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)
            if 'Contents' in list_objects:
                for obj in list_objects['Contents']:
                    s3_file = obj['Key']
                    if s3_file.endswith('.csv') or s3_file.endswith('.parquet'):
                        file_list.append(os.path.basename(s3_file))  # Append only the file name to the list
        else:   # List files from the local directory
            for root, dirs, files in os.walk(data_folder):
                for file in files:
                    if file.endswith('.csv') or file.endswith('.parquet'):
                        file_list.append(file)  # Append only the file name to the list
        return file_list
    # ls = list_files('jtrade1-dir/data/dd', from_s3=True)
    # print(ls)

    def load_file(self, path, use_polars=False, is_s3=False, s3_client=None):
        """
        Helper function to load a single file, either from a local path or from S3.

        Parameters:
        - path: The local path or S3 key to the file.
        - use_polars: Whether to use Polars instead of Pandas (for DataFrames).
        - is_s3: If True, load the file from S3.
        - s3_client: The Boto3 S3 client, required if is_s3 is True.

        Returns:
        - A DataFrame (Polars or Pandas) or a Python dictionary if loading a JSON file.
        """
        if is_s3 and s3_client:
            response = s3_client.get_object(Bucket=path[0], Key=path[1])
            if path[1].endswith('.csv'):
                data = response['Body'].read().decode('utf-8')
                return pl.read_csv(BytesIO(data.encode())) if use_polars else pd.read_csv(StringIO(data))
            elif path[1].endswith('.parquet'):
                data = response['Body'].read()
                return pl.read_parquet(BytesIO(data)) if use_polars else pd.read_parquet(BytesIO(data))
            elif path[1].endswith('.json'):
                data = response['Body'].read().decode('utf-8')
                return json.loads(data)
        else:
            if path.endswith('.csv'):
                return pl.read_csv(path) if use_polars else pd.read_csv(path)
            elif path.endswith('.parquet'):
                return pl.read_parquet(path) if use_polars else pd.read_parquet(path)
            elif path.endswith('.json'):
                with open(path, 'r') as json_file:
                    return json.load(json_file)
        raise ValueError("File extension not supported. Please use .csv, .parquet, or .json.")

    def load_s3(self, bucket_name: str, 
                s3_directory: str = '',
                file_name: str = '', 
                aws_region: str = 'us-west-2', 
                use_polars: bool = False, 
                load_all: bool = False, 
                selected_files: list = None):
        """
        Loads files from an S3 directory into Pandas DataFrames, Polars DataFrames, or Python dictionaries.

        Parameters:
        - bucket_name: The S3 bucket name.
        - s3_directory: The directory in the S3 bucket where the files are located.
        - file_name: The name of the single file to read from the S3 bucket. Optional if load_all is True.
        - aws_region: The AWS region where the bucket is located (default: 'us-west-2').
        - use_polars: Whether to use Polars instead of Pandas for DataFrames (default: False).
        - load_all: If True, loads all data files from the specified S3 directory (default: False).
        - selected_files: List of specific files to load (overrides file_name). Only applicable if load_all is True.

        Returns:
        - A DataFrame (Polars or Pandas) or a Python dictionary if a single file is loaded.
        - A dictionary of DataFrames or Python dictionaries if multiple files are loaded, with keys as file names.
        """
        s3_client = boto3.client('s3', region_name=aws_region)
        
        if not load_all:
            s3_path = f"{s3_directory}/{file_name}" if s3_directory else file_name
            data = self.load_file((bucket_name, s3_path), use_polars, is_s3=True, s3_client=s3_client)
            print(f"File loaded from S3://{bucket_name}/{s3_path}")
            return data
        
        list_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)
        all_files = [obj['Key'] for obj in list_objects.get('Contents', []) if obj['Key'].endswith(('.csv', '.parquet', '.json'))]
        
        if selected_files:
            files_to_load = [file for file in all_files if any(file.endswith(selected_file) for selected_file in selected_files)]
        else:
            files_to_load = all_files

        dataframes = {}
        for s3_path in files_to_load:
            data = self.load_file((bucket_name, s3_path), use_polars, is_s3=True, s3_client=s3_client)
            file_key = os.path.basename(s3_path)
            dataframes[file_key] = data
            print(f"File loaded from S3://{bucket_name}/{s3_path}")
        return dataframes

    def load_local(self, data_folder: str, 
                   file_name: str = '', 
                   use_polars: bool = False, 
                   load_all: bool = False, 
                   selected_files: list = None):
        """
        Loads files from a local directory into Pandas DataFrames, Polars DataFrames, or Python dictionaries.

        Parameters:
        - data_folder: The local directory where the files are located.
        - file_name: The name of the single file to read from the local directory. Optional if load_all is True.
        - use_polars: Whether to use Polars instead of Pandas for DataFrames (default: False).
        - load_all: If True, loads all data files from the specified directory (default: False).
        - selected_files: List of specific files to load (overrides file_name). Only applicable if load_all is True.

        Returns:
        - A DataFrame (Polars or Pandas) or a Python dictionary if a single file is loaded.
        - A dictionary of DataFrames or Python dictionaries if multiple files are loaded, with keys as file names.
        """
        if not load_all:
            local_path = os.path.join(data_folder, file_name)
            data = self.load_file(local_path, use_polars)
            print(f"File loaded from {local_path}")
            return data

        all_files = [f for f in os.listdir(data_folder) if f.endswith(('.csv', '.parquet', '.json'))]

        if selected_files:
            files_to_load = [file for file in all_files if file in selected_files]
        else:
            files_to_load = all_files

        dataframes = {}
        for file in files_to_load:
            local_path = os.path.join(data_folder, file)
            data = self.load_file(local_path, use_polars)
            dataframes[file] = data
            print(f"File loaded from {local_path}")
        return dataframes
    # df = load_s3('jtrade1-dir', 'data', 'hhh.csv', use_polars=False)# Single file loading
    # df_dict = load_s3('jtrade1-dir', 'data',use_polars=True, load_all=True) # Massive loading: all files in the directory
    # df_dict = load_s3('jtrade1-dir', 'data', use_polars=False,load_all=True, selected_files=['hhh.csv', 'eeeee.parquet'])
    # load_local(data_folder='data', file_name = 'orders.csv',use_polars = True,load_all = True, selected_files = ['products.csv','users.csv'])

    def save_file(self, data, file_path, use_polars=False, delete_local=False):
        """
        Helper function to save a DataFrame or dictionary to a local file, either as CSV, Parquet, or JSON.

        Parameters:
        - data: The data to save (can be Pandas DataFrame, Polars DataFrame, or a Python dictionary).
        - file_path: The local path to save the file.
        - use_polars: Whether to save using Polars instead of Pandas (for DataFrames).
        - delete_local: Whether to delete the local file after saving (default: True).
        """
        if isinstance(data, dict):
            # Convert int64 to int for JSON serialization
            def convert_np_types(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, default=convert_np_types)
            print(f"Dictionary saved as JSON to {file_path}")
        else:
            if file_path.endswith('.csv'):
                if use_polars:
                    data.write_csv(file_path)
                else:
                    data.to_csv(file_path, index=False)
            elif file_path.endswith('.parquet'):
                if use_polars:
                    data.write_parquet(file_path)
                else:
                    data.to_parquet(file_path, index=False)
            else:
                raise ValueError("File extension not supported. Please use .csv, .parquet, or .json.")

        if delete_local:
            os.remove(file_path)
            print(f"Local file {file_path} has been deleted.")
        else:
            print(f"Local file {file_path} has been kept.")
    
    def save_s3(self, data, 
                bucket_name: str, 
                s3_directory: str, 
                file_name: str, 
                aws_region: str = 'us-west-2', 
                use_polars: bool = False, 
                delete_local: bool = True):
        """
        Saves a DataFrame or dictionary to an S3 directory as a CSV, Parquet, or JSON file.

        Parameters:
        - data: The data to save (can be Pandas DataFrame, Polars DataFrame, or a Python dictionary).
        - bucket_name: The S3 bucket name.
        - s3_directory: The directory in the S3 bucket where the file should be saved.
        - file_name: The name of the file to save in the S3 bucket (e.g., 'myfile.csv', 'myfile.parquet', or 'myfile.json').
        - aws_region: The AWS region where the bucket is located (default: 'us-west-2').
        - use_polars: Whether to use Polars instead of Pandas (for DataFrames).
        - delete_local: Whether to delete the local file after upload (default: True).
        """
        s3_client = boto3.client('s3', region_name=aws_region)
        file_path = file_name

        # Save locally first
        self.save_file(data, file_path, use_polars, delete_local=False)

        # Upload the local file to S3
        s3_path = f"{s3_directory}/{file_name}" if s3_directory else file_name
        with open(file_path, 'rb') as data_file:
            s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=data_file)
        print(f"File saved to S3://{bucket_name}/{s3_path}")

        # Delete local file if required
        if delete_local:
            os.remove(file_path)
            print(f"Local file {file_path} has been deleted.")
        else:
            print(f"Local file {file_path} has been kept.")
    
    def save_local(self, data, 
                   data_folder: str, 
                   file_name: str, 
                   use_polars: bool = False, 
                   delete_local: bool = False):
        """
        Saves a DataFrame or dictionary to a local directory as a CSV, Parquet, or JSON file.

        Parameters:
        - data: The data to save (can be Pandas DataFrame, Polars DataFrame, or a Python dictionary).
        - data_folder: The local directory where the file should be saved.
        - file_name: The name of the file to save (e.g., 'myfile.csv', 'myfile.parquet', or 'myfile.json').
        - use_polars: Whether to use Polars instead of Pandas (for DataFrames).
        - delete_local: Whether to delete the local file after saving (default: False).
        """
        file_path = os.path.join(data_folder, file_name)
        self.save_file(data, file_path, use_polars, delete_local)
        print(f"File saved to {file_path}")

    # df = pd.DataFrame({'date': [1, 2], 'value': [3, 4]})
    # save_s3(df, 'jtrade1-dir', 'data', 'testtest.parquet',use_polars=False, delete_local=True)
    # save_local(df, 'data','aaaaaa.csv', delete_local=False)

    # polars tables are stored in dictionary
    def polars_run_query(tables: dict, 
                         query: str) -> pl.DataFrame:
        """
        Runs a SQL query on the provided tables using Polars.

        Parameters:
        - tables: A dictionary of Polars DataFrames with table names as keys.
        - query: A SQL query string to be executed.

        Returns:
        - A Polars DataFrame containing the result of the query.
        """
        pl_sql_context = pl.SQLContext()  # Register each table in Polars SQL context
        
        for table_name, df in tables.items(): # Remove file extensions from table names for simpler queries
            table_name_no_ext = os.path.splitext(table_name)[0]
            pl_sql_context.register(table_name_no_ext, df)
        # Execute the query and collect the result into a DataFrame
        result = pl_sql_context.execute(query).collect()
        return result

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

if __name__ == "__main__":
    # Initialize the DataMaster class
    dm = DataMaster()

    # Test Redshift connection and SQL execution
    try:
        redshift = Redshift()
        df = redshift.run_sql("SELECT * FROM some_table LIMIT 5;")
        print("Redshift SQL Execution Successful:")
        print(df)
    except Exception as e:
        print(f"Redshift SQL Execution Failed: {e}")

    # Test run_sql_cache
    try:
        csv_path = "test.csv"
        query = "SELECT * FROM some_table LIMIT 5;"
        dm.run_sql_cache(csv_path, query, refresh=True)
        print("run_sql_cache execution successful.")
    except Exception as e:
        print(f"run_sql_cache execution failed: {e}")

    # Test get_current_dir
    current_dir = dm.get_current_dir()
    print(f"Current directory: {current_dir}")

    # Test create_dir (local and S3)
    try:
        dm.create_dir(method='data', parent_dir='.')
        dm.create_dir(method='calendar', parent_dir='.')
        dm.create_dir(method='data', bucket_name='your-s3-bucket', s3_directory='your/s3/folder', is_s3=True)
        dm.create_dir(method='calendar', bucket_name='your-s3-bucket', s3_directory='your/s3/folder', is_s3=True)
        print("create_dir execution successful.")
    except Exception as e:
        print(f"create_dir execution failed: {e}")

    # Test dir_locator (local and S3)
    try:
        local_path = dm.dir_locator(method='data', parent_dir='.')
        s3_path = dm.dir_locator(method='calendar', bucket_name='your-s3-bucket', s3_directory='your/s3/folder', is_s3=True)
        print(f"Local directory located at: {local_path}")
        print(f"S3 directory located at: S3://{s3_path}")
    except Exception as e:
        print(f"dir_locator execution failed: {e}")

    # Test list_files (local and S3)
    try:
        local_files = dm.list_files('your/local/directory', from_s3=False)
        s3_files = dm.list_files('your-s3-bucket/your/s3/folder', from_s3=True)
        print(f"Local files: {local_files}")
        print(f"S3 files: {s3_files}")
    except Exception as e:
        print(f"list_files execution failed: {e}")

    # Test load_file (local and S3)
    try:
        df_local = dm.load_local('your/local/directory', 'yourfile.csv', use_polars=False)
        df_s3 = dm.load_s3('your-s3-bucket', 'your/s3/folder', 'yourfile.csv', use_polars=False)
        print("load_file execution successful.")
        print(f"Local file loaded: {df_local.head()}")
        print(f"S3 file loaded: {df_s3.head()}")
    except Exception as e:
        print(f"load_file execution failed: {e}")

    # Test save_file (local and S3)
    try:
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        dm.save_local(df, 'your/local/directory', 'test_save_local.csv', use_polars=False)
        dm.save_s3(df, 'your-s3-bucket', 'your/s3/folder', 'test_save_s3.csv', use_polars=False)
        print("save_file execution successful.")
    except Exception as e:
        print(f"save_file execution failed: {e}")

    # Test polars_run_query
    try:
        tables = {
            'table1.csv': pl.DataFrame({'col1': [1, 2], 'col2': [3, 4]}),
            'table2.csv': pl.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        }
        query = "SELECT * FROM table1 WHERE col1 = 1"
        result_df = dm.polars_run_query(tables, query)
        print(f"Polars SQL Query Result: {result_df}")
    except Exception as e:
        print(f"polars_run_query execution failed: {e}")

# DataMaster
# Provides utility functions for managing data files locally and on S3, and performing SQL queries using Polars.

# Methods:
# run_sql_cache(csv_path, query, refresh=False): Caches the result of a SQL query as a CSV file.
# get_current_dir(): Returns the directory of the running script.
# create_dir(method='data', parent_dir='.', bucket_name=None, s3_directory='', aws_region='us-west-2', is_s3=False): Creates a directory either locally or in S3.
# dir_locator(method='data', parent_dir='.', bucket_name=None, s3_directory='', aws_region='us-west-2', is_s3=False): Locates a directory created by create_dir.
# list_files(data_folder, from_s3=False, aws_region='us-west-2'): Lists all CSV and Parquet files in a local or S3 directory.
# load_file(path, use_polars, is_s3=False, s3_client=None): Helper function to load a single file, either locally or from S3.
# load_s3(bucket_name, s3_directory='', file_name='', aws_region='us-west-2', use_polars=False, load_all=False, selected_files=None): Loads files from S3 into DataFrames.
# load_local(data_folder, file_name='', use_polars=False, load_all=False, selected_files=None): Loads files from a local directory into DataFrames.
# save_file(df, file_path, use_polars, delete_local=True): Helper function to save a DataFrame to a local file.
# save_s3(df, bucket_name, s3_directory, file_name, aws_region='us-west-2', use_polars=False, delete_local=True): Saves a DataFrame to S3.
# save_local(df, data_folder, file_name, use_polars=False, delete_local=False): Saves a DataFrame to a local directory.
# polars_run_query(tables, query): Runs a SQL query on Polars DataFrames.
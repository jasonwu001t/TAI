# data_master.py
from TAI.utils import ConfigLoader

import json, os, sys, inspect, logging
from datetime import datetime
from io import StringIO, BytesIO

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

        if from_s3:
            # Parse the bucket name and S3 directory from data_folder
            bucket_name, s3_directory = data_folder.split('/', 1)
            
            # Initialize S3 client
            s3_client = boto3.client('s3', region_name=aws_region)
            list_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)
            
            if 'Contents' in list_objects:
                for obj in list_objects['Contents']:
                    s3_file = obj['Key']
                    if s3_file.endswith('.csv') or s3_file.endswith('.parquet'):
                        file_list.append(os.path.basename(s3_file))  # Append only the file name to the list
        else:
            # List files from the local directory
            for root, dirs, files in os.walk(data_folder):
                for file in files:
                    if file.endswith('.csv') or file.endswith('.parquet'):
                        file_list.append(file)  # Append only the file name to the list

        return file_list
    # ls = list_files('jtrade1-dir/data/dd', from_s3=True)
    # print(ls)

    def load_file(self, path, use_polars, is_s3=False, s3_client=None):
        """
        Helper function to load a single file, either from a local path or from S3.

        Parameters:
        - path: The local path or S3 key to the file.
        - use_polars: Whether to use Polars instead of Pandas.
        - is_s3: If True, load the file from S3.
        - s3_client: The Boto3 S3 client, required if is_s3 is True.

        Returns:
        - A DataFrame (Polars or Pandas) loaded from the file.
        """
        if is_s3 and s3_client:
            response = s3_client.get_object(Bucket=path[0], Key=path[1])
            if path[1].endswith('.csv'):
                data = response['Body'].read().decode('utf-8')
                if use_polars:
                    return pl.read_csv(BytesIO(data.encode()))
                else:
                    return pd.read_csv(StringIO(data))
            elif path[1].endswith('.parquet'):
                data = response['Body'].read()
                if use_polars:
                    return pl.read_parquet(BytesIO(data))
                else:
                    return pd.read_parquet(BytesIO(data))
        else:
            if path.endswith('.csv'):
                return pl.read_csv(path) if use_polars else pd.read_csv(path)
            elif path.endswith('.parquet'):
                return pl.read_parquet(path) if use_polars else pd.read_parquet(path)
        raise ValueError("File extension not supported. Please use .csv or .parquet.")

    def load_s3(self, bucket_name: str, 
                s3_directory: str = '',
                file_name: str = '', 
                aws_region: str = 'us-west-2', 
                use_polars: bool = False, 
                load_all: bool = False, 
                selected_files: list = None):
        """
        Loads files from an S3 directory into Pandas or Polars DataFrames. Supports CSV and Parquet formats.
        """
        s3_client = boto3.client('s3', region_name=aws_region)
        
        if not load_all:
            s3_path = f"{s3_directory}/{file_name}" if s3_directory else file_name
            df = self.load_file((bucket_name, s3_path), use_polars, is_s3=True, s3_client=s3_client)
            print(f"DataFrame loaded from S3://{bucket_name}/{s3_path}")
            return df
        
        list_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)
        all_files = [obj['Key'] for obj in list_objects.get('Contents', []) if obj['Key'].endswith(('.csv', '.parquet'))]
        
        if selected_files:
            files_to_load = [file for file in all_files if any(file.endswith(selected_file) for selected_file in selected_files)]
        else:
            files_to_load = all_files

        dataframes = {}
        for s3_path in files_to_load:
            df = self.load_file((bucket_name, s3_path), use_polars, is_s3=True, s3_client=s3_client)
            file_key = os.path.basename(s3_path)
            dataframes[file_key] = df
            print(f"DataFrame loaded from S3://{bucket_name}/{s3_path}")
        return dataframes

    def load_local(self, data_folder: str, 
                file_name: str = '', 
                use_polars: bool = False, 
                load_all: bool = False, 
                selected_files: list = None):
        """
        Loads files from a local directory into Pandas or Polars DataFrames. Supports CSV and Parquet formats.
        """
        if not load_all:
            local_path = os.path.join(data_folder, file_name)
            df = self.load_file(local_path, use_polars)
            print(f"DataFrame loaded from {local_path}")
            return df

        all_files = [f for f in os.listdir(data_folder) if f.endswith(('.csv', '.parquet'))]

        if selected_files:
            files_to_load = [file for file in all_files if file in selected_files]
        else:
            files_to_load = all_files

        dataframes = {}
        for file in files_to_load:
            local_path = os.path.join(data_folder, file)
            df = self.load_file(local_path, use_polars)
            dataframes[file] = df
            print(f"DataFrame loaded from {local_path}")
        return dataframes

    # df = load_s3('jtrade1-dir', 'data', 'hhh.csv', use_polars=False)# Single file loading
    # df_dict = load_s3('jtrade1-dir', 'data',use_polars=True, load_all=True) # Massive loading: all files in the directory
    # df_dict = load_s3('jtrade1-dir', 'data', use_polars=False,load_all=True, selected_files=['hhh.csv', 'eeeee.parquet'])

    # load_local(data_folder='data', 
    #                file_name = 'orders.csv', 
    #                use_polars = True, 
    #                load_all = True, 
    #                selected_files = ['products.csv','users.csv'])
            
    def polars_load_dfs(self, data_folder, list_tables=None, load_all=True): #if list_tables have value, load_all has to be False
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

    def to_s3(self, df: pd.DataFrame, 
                    bucket_name: str, 
                    s3_directory: str, 
                    file_name: str, 
                    aws_region: str = 'us-east-1', 
                    delete_local: bool = True):
        """
        Saves a pandas DataFrame to an S3 directory as a CSV or Parquet file based on the file extension.
        Parameters:
        - df: pandas DataFrame to save
        - bucket_name: The S3 bucket name
        - s3_directory: The directory in the S3 bucket where the file should be saved
        - file_name: The name of the file to save in the S3 bucket (e.g., 'myfile.csv' or 'myfile.parquet')
        - aws_region: The AWS region where the bucket is located (default: 'us-east-1')
        - delete_local: Whether to delete the local file after upload (default: False)
        """
        # Determine the file path
        file_path = f"{s3_directory}/{file_name}" if s3_directory else file_name
        # Save the DataFrame locally based on the file extension
        if file_name.endswith('.csv'):
            df.to_csv(file_name, index=False)
        elif file_name.endswith('.parquet'):
            df.to_parquet(file_name, index=False)
        else:
            raise ValueError("File extension not supported. Please use .csv or .parquet.")
        
        s3_client = boto3.client('s3', region_name=aws_region)        # Initialize S3 client
        # Upload the local file to S3
        with open(file_name, 'rb') as data:
            s3_client.put_object(Bucket=bucket_name, Key=file_path, Body=data)
        print(f"DataFrame saved to S3://{bucket_name}/{file_path}")
        if delete_local:   # Delete the local file if required
            os.remove(file_name)
            print(f"Local file {file_name} has been deleted.")
        else:
            print(f"Local file {file_name} has been kept.")
    # # Example usage:
    # df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    # to_s3(df, 'jtrade1-dir', 'data', 'wwww.csv', delete_local=True)
    # to_s3(df, 'jtrade1-dir', 'data', 'eeeee.parquet', delete_local=True)

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
    data_master.to_s3('your-bucket-name', 'example.csv', df_example.to_csv(index=False))

    # Load data from S3
    df_loaded_s3 = data_master.load_from_s3('your-bucket-name', 'example.csv', 'csv')
    print(df_loaded_s3)

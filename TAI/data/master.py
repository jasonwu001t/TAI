# data_master.py
from TAI.utils import ConfigLoader
import os, sys, inspect
import pandas as pd
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
import psycopg2
import logging

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
                how = "in_dir"):
        # locate the data folder
        default_path = os.path.join(self.get_current_dir(), dir_name)

        if how =='calendar':
            today = datetime.now()
            year, month, day = today.year, today.month, today.day
            dir_path = os.path.join(dir_name, f"{year}", f"{month:02d}", f"{day:02d}")
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

    def load_from_local(self, file_path, file_format):
        if file_format == 'csv':
            return pd.read_csv(file_path)
        elif file_format == 'parquet':
            return pd.read_parquet(file_path)

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

# data_master.py
from TAI.utils import ConfigLoader
import os
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

    def run_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        if not self.use_connection_pooling:
            self.conn.close()
        return pd.DataFrame(rows, columns=columns)

    def read_query(self, query):
        return self.run_query(query)

    def write_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()
        if not self.use_connection_pooling:
            self.conn.close()

class DataMaster:
    def __init__(self, base_dir="data", aws_session=None):
        self.base_dir = base_dir
        self.s3_client = aws_session.client('s3') if aws_session else None

    def save_data(self, dataframes, file_format="parquet"):
        today = datetime.now()
        year, month, day = today.year, today.month, today.day
        dir_path = os.path.join(self.base_dir, f"{year}", f"{month:02d}", f"{day:02d}")
        os.makedirs(dir_path, exist_ok=True)

        for df_name, df in dataframes.items():
            file_path = os.path.join(dir_path, f"{df_name}.{file_format}")
            if file_format == "parquet":
                df.to_parquet(file_path)
            elif file_format == "csv":
                df.to_csv(file_path, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            logging.info(f"Saved {df_name} to {file_path}")

    def load_from_local(self, file_path, file_format):
        if file_format == 'csv':
            return pd.read_csv(file_path)
        elif file_format == 'parquet':
            return pd.read_parquet(file_path)

    def save_to_s3(self, bucket_name, object_name, data):
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
            logging.info(f"File {object_name} uploaded to {bucket_name}")
        except NoCredentialsError:
            logging.error("Credentials not available")

    def load_from_s3(self, bucket_name, file_key, file_format):
        try:
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
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

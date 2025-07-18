# athena.py
from TAI.utils import ConfigLoader

import json, os, sys, logging
import time
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from typing import Dict, List, Optional, Union, Any, Tuple

import pandas as pd
import numpy as np

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

logging.basicConfig(level=logging.INFO)

class AthenaAuth:
    """Authentication handler for AWS Athena"""
    
    def __init__(self, aws_profile: str = None, region: str = 'us-east-1'):
        self.aws_profile = aws_profile
        self.region = region
        config_loader = ConfigLoader()
        
        # Initialize AWS session
        self.session = boto3.Session(
            profile_name=aws_profile, 
            region_name=region
        ) if aws_profile else boto3.Session(region_name=region)
        
        # Initialize AWS clients
        self.athena_client = self.session.client('athena', region_name=region)
        self.s3_client = self.session.client('s3', region_name=region)
        self.sts_client = self.session.client('sts', region_name=region)
        
        # Get account ID for default configurations
        self.account_id = self._get_account_id()
        
    def _get_account_id(self) -> str:
        """Get current AWS account ID"""
        try:
            return self.sts_client.get_caller_identity()['Account']
        except Exception as e:
            logging.warning(f"Could not determine account ID: {e}")
            return "unknown"

class AthenaSparkSQL:
    """
    Simplified AWS Athena handler for basic S3 parquet file querying.
    Provides core query execution and S3 parquet table creation.
    """
    
    def __init__(self, aws_profile: str = None, region: str = 'us-east-1'):
        self.auth = AthenaAuth(aws_profile, region)
        self.athena_client = self.auth.athena_client
        self.s3_client = self.auth.s3_client
        self.account_id = self.auth.account_id
        self.region = region
        
        # Default configurations
        self.default_database = 'default'
        self.default_workgroup = 'primary'
        self.default_output_location = f's3://aws-athena-query-results-{region}-{self.account_id}'
    
    # =========================================================================
    # CORE ATHENA OPERATIONS
    # =========================================================================
    
    def execute_query(self, 
                     query: str,
                     database: str = None,
                     workgroup: str = None,
                     output_location: str = None,
                     return_as_dataframe: bool = True,
                     query_timeout: int = 300) -> Union[pd.DataFrame, Dict[str, Any]]:
        """
        Execute SQL query against Athena tables.
        
        Parameters:
        - query: SQL query to execute
        - database: Athena database name
        - workgroup: Athena workgroup 
        - output_location: S3 location for query results
        - return_as_dataframe: Return results as DataFrame if True
        - query_timeout: Maximum time to wait for query completion
        
        Returns:
        - Query results as DataFrame or raw response
        """
        try:
            # Set defaults
            database = database or self.default_database
            workgroup = workgroup or self.default_workgroup
            output_location = output_location or self.default_output_location
            
            # Build execution context
            execution_context = {'Database': database}
            
            # Build result configuration
            result_config = {
                'OutputLocation': output_location,
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
            
            # Execute query
            logging.info(f"Executing Athena query in database: {database}")
            logging.info(f"Query: {query[:200]}...")
            
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext=execution_context,
                ResultConfiguration=result_config,
                WorkGroup=workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            logging.info(f"Query execution ID: {query_execution_id}")
            
            # Wait for completion with timeout
            final_status = self._wait_for_query_completion(
                query_execution_id, 
                timeout=query_timeout
            )
            
            if final_status != 'SUCCEEDED':
                error_details = self._get_query_error_details(query_execution_id)
                raise Exception(f"Query failed with status: {final_status}. Error: {error_details}")
            
            # Get execution statistics
            stats = self._get_query_statistics(query_execution_id)
            logging.info(f"Query completed in {stats.get('execution_time_ms', 0)}ms")
            logging.info(f"Data scanned: {stats.get('data_scanned_mb', 0):.2f} MB")
            
            # Return results
            if return_as_dataframe:
                return self._get_results_as_dataframe(query_execution_id)
            else:
                return self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
                
        except Exception as e:
            logging.error(f"Failed to execute Athena query: {e}")
            raise
    
    def create_database(self, 
                       database_name: str,
                       description: str = None,
                       location: str = None) -> Dict[str, str]:
        """
        Create Athena database with optional S3 location.
        
        Parameters:
        - database_name: Name of the database to create
        - description: Database description
        - location: S3 location for database
        
        Returns:
        - Creation status and details
        """
        try:
            # Build CREATE DATABASE query
            query_parts = [f"CREATE DATABASE IF NOT EXISTS {database_name}"]
            
            if description:
                query_parts.append(f"COMMENT '{description}'")
            
            if location:
                query_parts.append(f"LOCATION '{location}'")
            
            create_query = " ".join(query_parts)
            
            # Execute database creation
            self.execute_query(
                query=create_query,
                return_as_dataframe=False
            )
            
            logging.info(f"Database '{database_name}' created successfully")
            
            return {
                'database_name': database_name,
                'status': 'created',
                'location': location or 'default'
            }
            
        except Exception as e:
            logging.error(f"Failed to create database '{database_name}': {e}")
            raise
    
    # =========================================================================
    # S3 PARQUET FILE OPERATIONS
    # =========================================================================
    
    def list_s3_parquet_files(self, s3_path: str) -> List[Dict[str, Any]]:
        """
        List all parquet files in the given S3 path.
        
        Parameters:
        - s3_path: S3 path (e.g., 's3://bucket/path/')
        
        Returns:
        - List of parquet file details
        """
        try:
            # Parse S3 path
            if not s3_path.startswith('s3://'):
                raise ValueError("S3 path must start with 's3://'")
            
            path_parts = s3_path[5:].split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''
            
            # List objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            parquet_files = []
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].lower().endswith('.parquet'):
                            parquet_files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'],
                                's3_path': f"s3://{bucket}/{obj['Key']}"
                            })
            
            logging.info(f"Found {len(parquet_files)} parquet files in {s3_path}")
            return parquet_files
            
        except Exception as e:
            logging.error(f"Failed to list parquet files in {s3_path}: {e}")
            raise
    
    def create_table_from_s3_parquet(self,
                                    table_name: str,
                                    database: str,
                                    s3_path: str,
                                    specific_file: str = None,
                                    auto_detect_schema: bool = True) -> Dict[str, str]:
        """
        Create Athena table from S3 parquet files with automatic schema detection.
        
        Parameters:
        - table_name: Name of the table to create
        - database: Target database
        - s3_path: S3 path containing parquet files
        - specific_file: Specific parquet file to use (optional)
        - auto_detect_schema: Automatically detect schema from parquet metadata
        
        Returns:
        - Table creation status
        """
        try:
            # Determine the actual S3 location
            if specific_file:
                # Use specific file
                if not specific_file.startswith('s3://'):
                    # Assume it's a key within the s3_path
                    table_location = f"{s3_path.rstrip('/')}/{specific_file}"
                else:
                    table_location = specific_file
                logging.info(f"Creating table for specific file: {table_location}")
            else:
                # Use entire directory
                table_location = s3_path.rstrip('/') + '/'
                logging.info(f"Creating table for all parquet files in: {table_location}")
            
            if auto_detect_schema:
                # Try multiple approaches for schema detection
                table_created = False
                
                # Method 1: Use simple CREATE TABLE with ParquetSerDe (most compatible)
                try:
                    logging.info("Trying simple ParquetSerDe table creation...")
                    create_query = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name}
                    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                    SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    LOCATION '{table_location}'
                    TBLPROPERTIES (
                        'has_encrypted_data'='false',
                        'classification'='parquet'
                    )
                    """
                    
                    self.execute_query(
                        query=create_query,
                        database=database,
                        return_as_dataframe=False
                    )
                    table_created = True
                    logging.info("✓ Simple ParquetSerDe table creation successful")
                    
                except Exception as e:
                    logging.info(f"Simple ParquetSerDe method failed: {e}")
                
                # Method 2: Use LazySimpleSerDe (fallback)
                if not table_created:
                    try:
                        logging.info("Trying LazySimpleSerDe fallback...")
                        create_query = f"""
                        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name}
                        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                        LOCATION '{table_location}'
                        TBLPROPERTIES (
                            'has_encrypted_data'='false'
                        )
                        """
                        
                        self.execute_query(
                            query=create_query,
                            database=database,
                            return_as_dataframe=False
                        )
                        table_created = True
                        logging.info("✓ LazySimpleSerDe table creation successful")
                        
                    except Exception as e:
                        logging.info(f"LazySimpleSerDe method failed: {e}")
                
                # If all methods failed, provide helpful error message
                if not table_created:
                    error_msg = f"""
                    Automatic table creation failed for {database}.{table_name}.
                    
                    This may happen if:
                    1. The parquet files have complex schema
                    2. AWS Glue crawler is needed for schema discovery
                    3. Manual column definition is required
                    
                    Suggestion: Use AWS Glue crawler to create the table, then use athena.execute_query() directly.
                    """
                    raise Exception(error_msg)
            else:
                # Basic external table without schema detection - use parquet serde
                create_query = f"""
                CREATE EXTERNAL TABLE {database}.{table_name}
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                LOCATION '{table_location}'
                TBLPROPERTIES (
                    'has_encrypted_data'='false',
                    'classification'='parquet'
                )
                """
                
                self.execute_query(
                    query=create_query,
                    database=database,
                    return_as_dataframe=False
                )
            
            logging.info(f"Table '{database}.{table_name}' created successfully")
            
            return {
                'table_name': f"{database}.{table_name}",
                'status': 'created',
                's3_location': table_location,
                'schema_detection': auto_detect_schema
            }
            
        except Exception as e:
            logging.error(f"Failed to create table '{database}.{table_name}': {e}")
            raise
    
    def query_s3_parquet_data(self,
                             s3_path: str,
                             query: str,
                             table_name: str = None,
                             database: str = None,
                             specific_file: str = None,
                             create_temp_table: bool = True) -> pd.DataFrame:
        """
        Query S3 parquet data through a temporary table.
        
        Parameters:
        - s3_path: S3 path containing parquet files
        - query: SQL query to execute (use {table} placeholder for table name)
        - table_name: Table name to create (if create_temp_table=True)
        - database: Target database
        - specific_file: Query specific parquet file
        - create_temp_table: Create temporary table for querying
        
        Returns:
        - DataFrame with query results
        """
        try:
            database = database or self.default_database
            
            if create_temp_table:
                # Create temporary table
                temp_table_name = table_name or f"temp_table_{int(time.time())}"
                
                # Check if table already exists
                try:
                    existing_tables = self.list_tables(database)
                    if temp_table_name not in existing_tables:
                        self.create_table_from_s3_parquet(
                            table_name=temp_table_name,
                            database=database,
                            s3_path=s3_path,
                            specific_file=specific_file
                        )
                        logging.info(f"Created temporary table: {temp_table_name}")
                    else:
                        logging.info(f"Using existing table: {temp_table_name}")
                except Exception as e:
                    logging.warning(f"Could not check existing tables: {e}")
                    # Proceed with table creation
                    self.create_table_from_s3_parquet(
                        table_name=temp_table_name,
                        database=database,
                        s3_path=s3_path,
                        specific_file=specific_file
                    )
                
                # Execute query against the table
                # Replace any table references with the actual table name
                final_query = query.replace("{table}", f"{database}.{temp_table_name}")
                
                return self.execute_query(
                    query=final_query,
                    database=database
                )
            
            else:
                # Direct query without table creation
                return self.execute_query(query=query, database=database)
                    
        except Exception as e:
            logging.error(f"Failed to query S3 parquet data: {e}")
            raise
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _wait_for_query_completion(self, query_execution_id: str, timeout: int = 300) -> str:
        """Wait for Athena query to complete"""
        
        start_time = time.time()
        last_status = None
        
        while time.time() - start_time < timeout:
            try:
                response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = response['QueryExecution']['Status']['State']
                
                # Log status changes
                if status != last_status:
                    logging.info(f"Query {query_execution_id} status: {status}")
                    last_status = status
                
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    return status
                
                time.sleep(3)  # Check every 3 seconds
                
            except Exception as e:
                logging.warning(f"Error checking query status: {e}")
                time.sleep(5)
        
        raise TimeoutError(f"Query {query_execution_id} did not complete within {timeout} seconds")
    
    def _get_query_error_details(self, query_execution_id: str) -> str:
        """Get detailed error information for failed queries"""
        
        try:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status_info = response['QueryExecution']['Status']
            
            error_details = {
                'reason': status_info.get('StateChangeReason', 'Unknown'),
                'error_type': status_info.get('AthenaError', {}).get('ErrorType', 'Unknown'),
                'error_message': status_info.get('AthenaError', {}).get('ErrorMessage', 'Unknown')
            }
            
            return f"Type: {error_details['error_type']}, Message: {error_details['error_message']}, Reason: {error_details['reason']}"
            
        except Exception as e:
            return f"Could not retrieve error details: {e}"
    
    def _get_query_statistics(self, query_execution_id: str) -> Dict[str, Any]:
        """Get query execution statistics"""
        
        try:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            stats = response['QueryExecution']['Statistics']
            
            return {
                'execution_time_ms': stats.get('EngineExecutionTimeInMillis', 0),
                'data_scanned_bytes': stats.get('DataScannedInBytes', 0),
                'data_scanned_mb': stats.get('DataScannedInBytes', 0) / (1024 * 1024),
                'result_reused': stats.get('ResultReuseInformation', {}).get('ReusedPreviousResult', False)
            }
            
        except Exception as e:
            logging.warning(f"Could not retrieve query statistics: {e}")
            return {}
    
    def _get_results_as_dataframe(self, query_execution_id: str) -> pd.DataFrame:
        """Convert Athena results to DataFrame"""
        
        try:
            # Get results with pagination support
            results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Extract column metadata
            columns_info = results['ResultSet']['ResultSetMetadata']['ColumnInfo']
            columns = [col['Label'] for col in columns_info]
            column_types = {col['Label']: col['Type'] for col in columns_info}
            
            # Extract data rows
            rows = []
            for row in results['ResultSet']['Rows'][1:]:  # Skip header
                row_data = []
                for field in row['Data']:
                    value = field.get('VarCharValue')
                    if value == '':
                        value = None
                    row_data.append(value)
                rows.append(row_data)
            
            # Handle pagination for large result sets
            while 'NextToken' in results:
                results = self.athena_client.get_query_results(
                    QueryExecutionId=query_execution_id,
                    NextToken=results['NextToken']
                )
                for row in results['ResultSet']['Rows']:
                    row_data = []
                    for field in row['Data']:
                        value = field.get('VarCharValue')
                        if value == '':
                            value = None
                        row_data.append(value)
                    rows.append(row_data)
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # Convert data types
            for col, athena_type in column_types.items():
                if col in df.columns:
                    df[col] = self._convert_column_type(df[col], athena_type)
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to convert results to DataFrame: {e}")
            raise
    
    def _convert_column_type(self, series: pd.Series, athena_type: str) -> pd.Series:
        """Convert DataFrame column to appropriate type"""
        
        try:
            athena_type = athena_type.lower()
            
            if athena_type in ['bigint', 'integer', 'int']:
                return pd.to_numeric(series, errors='coerce').astype('Int64')
            elif athena_type in ['double', 'float', 'decimal']:
                return pd.to_numeric(series, errors='coerce')
            elif athena_type in ['boolean']:
                return series.map({'true': True, 'false': False, True: True, False: False})
            elif athena_type in ['date']:
                return pd.to_datetime(series, errors='coerce').dt.date
            elif athena_type in ['timestamp']:
                return pd.to_datetime(series, errors='coerce')
            else:
                return series  # Keep as string for other types
                
        except Exception as e:
            logging.warning(f"Could not convert column type {athena_type}: {e}")
            return series
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def list_databases(self) -> List[str]:
        """List all available Athena databases"""
        
        try:
            response = self.athena_client.list_databases(CatalogName='AwsDataCatalog')
            return [db['Name'] for db in response['DatabaseList']]
        except Exception as e:
            logging.error(f"Failed to list databases: {e}")
            raise
    
    def list_tables(self, database: str) -> List[str]:
        """List all tables in a database"""
        
        try:
            response = self.athena_client.list_table_metadata(
                CatalogName='AwsDataCatalog',
                DatabaseName=database
            )
            return [table['Name'] for table in response['TableMetadataList']]
        except Exception as e:
            logging.error(f"Failed to list tables in database '{database}': {e}")
            raise
    
    def get_table_schema(self, database: str, table: str) -> Dict[str, str]:
        """Get table schema information"""
        
        try:
            response = self.athena_client.get_table_metadata(
                CatalogName='AwsDataCatalog',
                DatabaseName=database,
                TableName=table
            )
            
            schema = {}
            for column in response['TableMetadata']['Columns']:
                schema[column['Name']] = column['Type']
            
            return schema
        except Exception as e:
            logging.error(f"Failed to get schema for table '{database}.{table}': {e}")
            raise
    
    def drop_table(self, database: str, table: str) -> Dict[str, str]:
        """Drop a table from the database"""
        
        try:
            drop_query = f"DROP TABLE IF EXISTS {database}.{table}"
            
            self.execute_query(
                query=drop_query,
                database=database,
                return_as_dataframe=False
            )
            
            logging.info(f"Table '{database}.{table}' dropped successfully")
            
            return {
                'table_name': f"{database}.{table}",
                'status': 'dropped'
            }
            
        except Exception as e:
            logging.error(f"Failed to drop table '{database}.{table}': {e}")
            raise

if __name__ == "__main__":
    # Example usage
    athena = AthenaSparkSQL()
    
    # Test basic connectivity
    try:
        databases = athena.list_databases()
        print(f"Available databases: {databases}")
    except Exception as e:
        print(f"Connection test failed: {e}") 
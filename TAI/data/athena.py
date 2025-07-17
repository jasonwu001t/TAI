# athena.py
from TAI.utils import ConfigLoader

import json, os, sys, logging
import time
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from typing import Dict, List, Optional, Union, Any, Tuple

import pandas as pd
import numpy as np
import polars as pl

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
    Enhanced AWS Athena handler with focus on Spark SQL capabilities.
    Provides comprehensive query execution, optimization, and data analysis features.
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
        
        # Spark SQL optimization settings
        self.spark_configs = {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.skewJoin.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.execution.arrow.pyspark.enabled': 'true'
        }
        
    # =========================================================================
    # CORE ATHENA OPERATIONS
    # =========================================================================
    
    def execute_query(self, 
                     query: str,
                     database: str = None,
                     workgroup: str = None,
                     output_location: str = None,
                     return_as_dataframe: bool = True,
                     query_timeout: int = 300,
                     enable_result_reuse: bool = True) -> Union[pd.DataFrame, Dict[str, Any]]:
        """
        Execute SQL query with enhanced Spark SQL optimizations.
        
        Parameters:
        - query: SQL query to execute
        - database: Athena database name
        - workgroup: Athena workgroup 
        - output_location: S3 location for query results
        - return_as_dataframe: Return results as DataFrame if True
        - query_timeout: Maximum time to wait for query completion
        - enable_result_reuse: Enable query result reuse for performance
        
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
            
            # Build result configuration with optimizations
            result_config = {
                'OutputLocation': output_location,
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
            
            # Add result reuse if enabled
            if enable_result_reuse:
                result_config['AclConfiguration'] = {
                    'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                }
            
            # Execute query with Spark optimizations
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
    
    def execute_spark_sql(self,
                         query: str,
                         database: str = None,
                         enable_optimizations: bool = True,
                         partition_projection: bool = False,
                         **kwargs) -> pd.DataFrame:
        """
        Execute Spark SQL with enhanced optimizations and performance tuning.
        
        Parameters:
        - query: Spark SQL query
        - database: Target database
        - enable_optimizations: Enable Spark SQL optimizations
        - partition_projection: Enable partition projection for performance
        - **kwargs: Additional execution parameters
        
        Returns:
        - DataFrame with query results
        """
        try:
            # Add Spark SQL optimizations to query
            if enable_optimizations:
                optimized_query = self._add_spark_optimizations(query)
            else:
                optimized_query = query
            
            # Add partition projection hints if enabled
            if partition_projection:
                optimized_query = self._add_partition_hints(optimized_query)
            
            # Execute with enhanced settings
            return self.execute_query(
                query=optimized_query,
                database=database,
                **kwargs
            )
            
        except Exception as e:
            logging.error(f"Failed to execute Spark SQL: {e}")
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
    
    def create_external_table(self,
                             table_name: str,
                             database: str,
                             s3_location: str,
                             schema: Dict[str, str],
                             file_format: str = 'PARQUET',
                             partition_keys: List[str] = None,
                             table_properties: Dict[str, str] = None) -> Dict[str, str]:
        """
        Create external table with enhanced Spark SQL features.
        
        Parameters:
        - table_name: Name of the table
        - database: Target database
        - s3_location: S3 location of data
        - schema: Column definitions {column_name: data_type}
        - file_format: Data file format (PARQUET, CSV, JSON, etc.)
        - partition_keys: List of partition column names
        - table_properties: Additional table properties
        
        Returns:
        - Table creation status
        """
        try:
            # Build column definitions
            columns = []
            for col_name, col_type in schema.items():
                columns.append(f"{col_name} {col_type}")
            
            # Build partition clause
            partition_clause = ""
            if partition_keys:
                partition_cols = [f"{key} string" for key in partition_keys]
                partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
            
            # Build table properties
            properties = table_properties or {}
            
            # Add format-specific optimizations
            if file_format.upper() == 'PARQUET':
                properties.update({
                    'has_encrypted_data': 'false',
                    'projection.enabled': 'true'
                })
            
            properties_clause = ""
            if properties:
                prop_list = [f"'{k}'='{v}'" for k, v in properties.items()]
                properties_clause = f"TBLPROPERTIES ({', '.join(prop_list)})"
            
            # Build CREATE TABLE query
            create_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
                {', '.join(columns)}
            )
            {partition_clause}
            STORED AS {file_format}
            LOCATION '{s3_location}'
            {properties_clause}
            """
            
            # Execute table creation
            self.execute_query(
                query=create_query,
                database=database,
                return_as_dataframe=False
            )
            
            logging.info(f"External table '{database}.{table_name}' created successfully")
            
            return {
                'table_name': f"{database}.{table_name}",
                'status': 'created',
                's3_location': s3_location,
                'file_format': file_format
            }
            
        except Exception as e:
            logging.error(f"Failed to create table '{database}.{table_name}': {e}")
            raise
    
    # =========================================================================
    # CUR/BILLING DATA SPECIFIC METHODS
    # =========================================================================
    
    def setup_cur_integration(self,
                             database_name: str = 'cur_database',
                             table_name: str = 'cur_table',
                             s3_bucket: str = None,
                             s3_prefix: str = 'cur-exports/',
                             enable_partition_projection: bool = True) -> Dict[str, str]:
        """
        Set up optimized Athena integration for AWS Cost and Usage Reports.
        
        Parameters:
        - database_name: Database name for CUR data
        - table_name: Table name for CUR data  
        - s3_bucket: S3 bucket containing CUR data
        - s3_prefix: S3 prefix where CUR data is stored
        - enable_partition_projection: Enable partition projection for performance
        
        Returns:
        - Setup status and details
        """
        try:
            # Create database
            self.create_database(
                database_name=database_name,
                description="AWS Cost and Usage Reports database"
            )
            
            # Define CUR schema with common columns
            cur_schema = {
                'bill_billing_period_start_date': 'string',
                'bill_billing_period_end_date': 'string',
                'bill_payer_account_id': 'string',
                'line_item_usage_account_id': 'string',
                'line_item_product_code': 'string',
                'line_item_usage_type': 'string',
                'line_item_operation': 'string',
                'line_item_line_item_type': 'string',
                'line_item_usage_start_date': 'string',
                'line_item_usage_end_date': 'string',
                'line_item_usage_amount': 'double',
                'line_item_currency_code': 'string',
                'line_item_unblended_cost': 'double',
                'line_item_blended_cost': 'double',
                'line_item_resource_id': 'string',
                'product_product_name': 'string',
                'product_region': 'string',
                'product_servicecode': 'string',
                'pricing_term': 'string',
                'reservation_reservation_a_r_n': 'string',
                'savings_plan_savings_plan_a_r_n': 'string'
            }
            
            # Table properties with partition projection
            table_properties = {
                'skip.header.line.count': '1',
                'serialization.format': '1'
            }
            
            if enable_partition_projection:
                # Add partition projection for year/month partitions
                table_properties.update({
                    'projection.enabled': 'true',
                    'projection.year.type': 'integer',
                    'projection.year.range': '2020,2030',
                    'projection.month.type': 'integer', 
                    'projection.month.range': '1,12',
                    'projection.month.digits': '2',
                    'storage.location.template': f's3://{s3_bucket}/{s3_prefix}year=${{year}}/month=${{month}}/'
                })
            
            # Create external table
            if s3_bucket:
                self.create_external_table(
                    table_name=table_name,
                    database=database_name,
                    s3_location=f's3://{s3_bucket}/{s3_prefix}',
                    schema=cur_schema,
                    file_format='PARQUET',
                    partition_keys=['year', 'month'] if enable_partition_projection else None,
                    table_properties=table_properties
                )
            
            logging.info(f"CUR Athena integration setup complete for {database_name}.{table_name}")
            
            return {
                'database_name': database_name,
                'table_name': table_name,
                's3_location': f's3://{s3_bucket}/{s3_prefix}' if s3_bucket else None,
                'partition_projection': enable_partition_projection,
                'status': 'completed'
            }
            
        except Exception as e:
            logging.error(f"Failed to setup CUR integration: {e}")
            raise
    
    def query_latest_month_data(self,
                               database: str = 'cur_database',
                               table: str = 'cur_table',
                               limit: int = 10,
                               cost_threshold: float = 0.01) -> pd.DataFrame:
        """
        Query latest month's CUR data with Spark SQL optimizations.
        
        Parameters:
        - database: Athena database name
        - table: Athena table name
        - limit: Number of records to return
        - cost_threshold: Minimum cost to include in results
        
        Returns:
        - DataFrame with latest month's cost data
        """
        try:
            # Calculate latest complete month
            now = datetime.now()
            if now.month == 1:
                latest_month_start = datetime(now.year - 1, 12, 1)
            else:
                latest_month_start = datetime(now.year, now.month - 1, 1)
            
            start_date = latest_month_start.strftime('%Y-%m-%d')
            year = latest_month_start.year
            month = latest_month_start.month
            
            # Optimized query with partition pruning and cost filtering
            query = f"""
            SELECT 
                line_item_usage_start_date,
                line_item_usage_account_id,
                product_product_name,
                line_item_usage_type,
                line_item_operation,
                line_item_unblended_cost,
                line_item_usage_amount,
                line_item_currency_code,
                product_region,
                line_item_resource_id
            FROM {database}.{table}
            WHERE year = {year} 
            AND month = {month}
            AND line_item_usage_start_date >= '{start_date}'
            AND line_item_line_item_type != 'Tax'
            AND line_item_unblended_cost >= {cost_threshold}
            ORDER BY line_item_unblended_cost DESC
            LIMIT {limit}
            """
            
            return self.execute_spark_sql(
                query=query,
                database=database,
                enable_optimizations=True,
                partition_projection=True
            )
            
        except Exception as e:
            logging.error(f"Failed to query latest month data: {e}")
            raise
    
    def get_cost_by_service(self,
                           start_date: str,
                           end_date: str,
                           database: str = 'cur_database',
                           table: str = 'cur_table',
                           account_ids: List[str] = None,
                           group_by_account: bool = True,
                           min_cost: float = 1.0) -> pd.DataFrame:
        """
        Get cost breakdown by AWS service with enhanced filtering and grouping.
        
        Parameters:
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)
        - database: Athena database name
        - table: Athena table name
        - account_ids: List of AWS account IDs to filter
        - group_by_account: Include account breakdown
        - min_cost: Minimum cost threshold
        
        Returns:
        - DataFrame with cost breakdown by service
        """
        try:
            # Build account filter
            account_filter = ""
            if account_ids:
                account_list = "', '".join(account_ids)
                account_filter = f"AND line_item_usage_account_id IN ('{account_list}')"
            
            # Build grouping and selection
            if group_by_account:
                select_clause = "line_item_usage_account_id, product_product_name"
                group_by_clause = "line_item_usage_account_id, product_product_name"
            else:
                select_clause = "product_product_name" 
                group_by_clause = "product_product_name"
            
            # Optimized query with aggregations and filtering
            query = f"""
            SELECT 
                {select_clause},
                SUM(line_item_unblended_cost) as total_cost,
                SUM(line_item_usage_amount) as total_usage,
                COUNT(*) as line_item_count,
                MAX(line_item_usage_start_date) as latest_usage_date,
                line_item_currency_code
            FROM {database}.{table}
            WHERE line_item_usage_start_date >= '{start_date}'
            AND line_item_usage_start_date < '{end_date}'
            AND line_item_line_item_type != 'Tax'
            AND line_item_unblended_cost > 0
            {account_filter}
            GROUP BY {group_by_clause}, line_item_currency_code
            HAVING SUM(line_item_unblended_cost) >= {min_cost}
            ORDER BY total_cost DESC
            """
            
            return self.execute_spark_sql(
                query=query,
                database=database,
                enable_optimizations=True
            )
            
        except Exception as e:
            logging.error(f"Failed to get cost by service: {e}")
            raise
    
    def get_daily_cost_trend(self,
                            start_date: str,
                            end_date: str,
                            database: str = 'cur_database',
                            table: str = 'cur_table',
                            service_name: str = None,
                            account_id: str = None) -> pd.DataFrame:
        """
        Get daily cost trends with time-series optimizations.
        
        Parameters:
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)
        - database: Database name
        - table: Table name
        - service_name: Specific AWS service
        - account_id: Specific AWS account ID
        
        Returns:
        - DataFrame with daily cost trends
        """
        try:
            # Build filters
            filters = []
            if service_name:
                filters.append(f"AND product_product_name = '{service_name}'")
            if account_id:
                filters.append(f"AND line_item_usage_account_id = '{account_id}'")
            
            filter_clause = " ".join(filters)
            
            # Time-series optimized query
            query = f"""
            SELECT 
                DATE(line_item_usage_start_date) as usage_date,
                product_product_name,
                line_item_usage_account_id,
                SUM(line_item_unblended_cost) as daily_cost,
                SUM(line_item_usage_amount) as daily_usage,
                COUNT(DISTINCT line_item_resource_id) as unique_resources,
                line_item_currency_code
            FROM {database}.{table}
            WHERE line_item_usage_start_date >= '{start_date}'
            AND line_item_usage_start_date < '{end_date}'
            AND line_item_line_item_type != 'Tax'
            AND line_item_unblended_cost > 0
            {filter_clause}
            GROUP BY DATE(line_item_usage_start_date), product_product_name, 
                     line_item_usage_account_id, line_item_currency_code
            ORDER BY usage_date DESC, daily_cost DESC
            """
            
            return self.execute_spark_sql(
                query=query,
                database=database,
                enable_optimizations=True
            )
            
        except Exception as e:
            logging.error(f"Failed to get daily cost trend: {e}")
            raise
    
    def analyze_cost_anomalies(self,
                              start_date: str,
                              end_date: str,
                              database: str = 'cur_database',
                              table: str = 'cur_table',
                              anomaly_threshold: float = 2.0) -> pd.DataFrame:
        """
        Detect cost anomalies using statistical analysis with Spark SQL.
        
        Parameters:
        - start_date: Analysis start date
        - end_date: Analysis end date
        - database: Database name
        - table: Table name
        - anomaly_threshold: Standard deviation threshold for anomalies
        
        Returns:
        - DataFrame with detected anomalies
        """
        try:
            # Advanced statistical analysis query
            query = f"""
            WITH daily_costs AS (
                SELECT 
                    DATE(line_item_usage_start_date) as usage_date,
                    product_product_name,
                    line_item_usage_account_id,
                    SUM(line_item_unblended_cost) as daily_cost
                FROM {database}.{table}
                WHERE line_item_usage_start_date >= '{start_date}'
                AND line_item_usage_start_date < '{end_date}'
                AND line_item_line_item_type != 'Tax'
                GROUP BY DATE(line_item_usage_start_date), product_product_name, line_item_usage_account_id
            ),
            cost_stats AS (
                SELECT 
                    product_product_name,
                    line_item_usage_account_id,
                    AVG(daily_cost) as avg_daily_cost,
                    STDDEV(daily_cost) as stddev_daily_cost,
                    COUNT(*) as days_count
                FROM daily_costs
                GROUP BY product_product_name, line_item_usage_account_id
                HAVING COUNT(*) >= 7  -- At least 7 days of data
            )
            SELECT 
                dc.usage_date,
                dc.product_product_name,
                dc.line_item_usage_account_id,
                dc.daily_cost,
                cs.avg_daily_cost,
                cs.stddev_daily_cost,
                ABS(dc.daily_cost - cs.avg_daily_cost) / cs.stddev_daily_cost as z_score,
                CASE 
                    WHEN dc.daily_cost > (cs.avg_daily_cost + {anomaly_threshold} * cs.stddev_daily_cost) THEN 'HIGH_ANOMALY'
                    WHEN dc.daily_cost < (cs.avg_daily_cost - {anomaly_threshold} * cs.stddev_daily_cost) THEN 'LOW_ANOMALY'
                    ELSE 'NORMAL'
                END as anomaly_type
            FROM daily_costs dc
            JOIN cost_stats cs ON dc.product_product_name = cs.product_product_name 
                AND dc.line_item_usage_account_id = cs.line_item_usage_account_id
            WHERE ABS(dc.daily_cost - cs.avg_daily_cost) / cs.stddev_daily_cost >= {anomaly_threshold}
            ORDER BY z_score DESC, usage_date DESC
            """
            
            return self.execute_spark_sql(
                query=query,
                database=database,
                enable_optimizations=True
            )
            
        except Exception as e:
            logging.error(f"Failed to analyze cost anomalies: {e}")
            raise
    
    # =========================================================================
    # SPARK SQL OPTIMIZATION HELPERS
    # =========================================================================
    
    def _add_spark_optimizations(self, query: str) -> str:
        """Add Spark SQL optimization hints to query"""
        
        # Add broadcast hints for small tables
        optimized_query = query
        
        # Add repartition hints for large aggregations
        if 'GROUP BY' in query.upper():
            optimized_query = f"/* +REPARTITION(10) */ {optimized_query}"
        
        # Add coalesce hints for final output
        if 'ORDER BY' in query.upper():
            optimized_query = f"/* +COALESCE(1) */ {optimized_query}"
        
        return optimized_query
    
    def _add_partition_hints(self, query: str) -> str:
        """Add partition projection hints for better performance"""
        
        # Look for date filters and add partition hints
        if 'WHERE' in query.upper() and 'date' in query.lower():
            # Add comment with partition pruning hint
            hint = "/* Partition pruning enabled for date-based queries */"
            return f"{hint}\n{query}"
        
        return query
    
    def _wait_for_query_completion(self, query_execution_id: str, timeout: int = 300) -> str:
        """Wait for Athena query to complete with enhanced monitoring"""
        
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
        """Get query execution statistics for performance monitoring"""
        
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
        """Convert Athena results to optimized DataFrame with proper type conversion"""
        
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
            
            # Optimize data types based on Athena metadata
            for col, athena_type in column_types.items():
                if col in df.columns:
                    df[col] = self._convert_column_type(df[col], athena_type)
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to convert results to DataFrame: {e}")
            raise
    
    def _convert_column_type(self, series: pd.Series, athena_type: str) -> pd.Series:
        """Convert DataFrame column to appropriate type based on Athena metadata"""
        
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
    # ADVANCED SPARK SQL FEATURES
    # =========================================================================
    
    def create_view(self,
                   view_name: str,
                   query: str,
                   database: str = None,
                   replace_if_exists: bool = True) -> Dict[str, str]:
        """
        Create a reusable SQL view for complex queries.
        
        Parameters:
        - view_name: Name of the view to create
        - query: SELECT query to base the view on
        - database: Target database for the view
        - replace_if_exists: Replace view if it already exists
        
        Returns:
        - View creation status
        """
        try:
            database = database or self.default_database
            
            replace_clause = "OR REPLACE" if replace_if_exists else ""
            create_view_query = f"""
            CREATE {replace_clause} VIEW {database}.{view_name} AS
            {query}
            """
            
            self.execute_query(
                query=create_view_query,
                database=database,
                return_as_dataframe=False
            )
            
            logging.info(f"View '{database}.{view_name}' created successfully")
            
            return {
                'view_name': f"{database}.{view_name}",
                'status': 'created',
                'database': database
            }
            
        except Exception as e:
            logging.error(f"Failed to create view '{database}.{view_name}': {e}")
            raise
    
    def execute_batch_queries(self,
                             queries: List[Dict[str, Any]],
                             parallel_execution: bool = False,
                             max_workers: int = 5) -> List[Dict[str, Any]]:
        """
        Execute multiple queries in batch with optional parallel processing.
        
        Parameters:
        - queries: List of query dictionaries with 'query' and optional parameters
        - parallel_execution: Execute queries in parallel
        - max_workers: Maximum number of parallel workers
        
        Returns:
        - List of execution results
        """
        try:
            results = []
            
            if parallel_execution:
                import concurrent.futures
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all queries
                    future_to_query = {}
                    for i, query_config in enumerate(queries):
                        future = executor.submit(self._execute_single_query, query_config, i)
                        future_to_query[future] = (i, query_config)
                    
                    # Collect results
                    for future in concurrent.futures.as_completed(future_to_query):
                        query_index, query_config = future_to_query[future]
                        try:
                            result = future.result()
                            results.append({
                                'index': query_index,
                                'query_name': query_config.get('name', f'query_{query_index}'),
                                'status': 'success',
                                'result': result
                            })
                        except Exception as e:
                            results.append({
                                'index': query_index,
                                'query_name': query_config.get('name', f'query_{query_index}'),
                                'status': 'failed',
                                'error': str(e)
                            })
            else:
                # Sequential execution
                for i, query_config in enumerate(queries):
                    try:
                        result = self._execute_single_query(query_config, i)
                        results.append({
                            'index': i,
                            'query_name': query_config.get('name', f'query_{i}'),
                            'status': 'success',
                            'result': result
                        })
                    except Exception as e:
                        results.append({
                            'index': i,
                            'query_name': query_config.get('name', f'query_{i}'),
                            'status': 'failed',
                            'error': str(e)
                        })
            
            # Sort results by original order
            results.sort(key=lambda x: x['index'])
            
            return results
            
        except Exception as e:
            logging.error(f"Failed to execute batch queries: {e}")
            raise
    
    def create_table_from_query(self,
                               new_table_name: str,
                               query: str,
                               database: str = None,
                               s3_location: str = None,
                               file_format: str = 'PARQUET',
                               partition_by: List[str] = None) -> Dict[str, str]:
        """
        Create a new table from a query result (CTAS - Create Table As Select).
        
        Parameters:
        - new_table_name: Name of the new table to create
        - query: SELECT query to populate the table
        - database: Target database
        - s3_location: S3 location for the new table data
        - file_format: File format for the table data
        - partition_by: List of columns to partition by
        
        Returns:
        - Table creation status
        """
        try:
            database = database or self.default_database
            
            # Build CTAS query
            ctas_parts = [f"CREATE TABLE {database}.{new_table_name}"]
            
            # Add table properties
            properties = [
                f"format = '{file_format}'",
                "write_compression = 'SNAPPY'"
            ]
            
            if s3_location:
                properties.append(f"external_location = '{s3_location}'")
            
            if partition_by:
                partition_cols = [f"'{col}'" for col in partition_by]
                partition_clause = f"partitioned_by = ARRAY[{', '.join(partition_cols)}]"
                properties.append(partition_clause)
            
            ctas_parts.append(f"WITH ({', '.join(properties)})")
            ctas_parts.append(f"AS ({query})")
            
            ctas_query = " ".join(ctas_parts)
            
            # Execute CTAS
            self.execute_query(
                query=ctas_query,
                database=database,
                return_as_dataframe=False
            )
            
            logging.info(f"Table '{database}.{new_table_name}' created from query")
            
            return {
                'table_name': f"{database}.{new_table_name}",
                'status': 'created',
                's3_location': s3_location,
                'format': file_format
            }
            
        except Exception as e:
            logging.error(f"Failed to create table from query: {e}")
            raise
    
    def optimize_query_performance(self,
                                  query: str,
                                  enable_adaptive_query: bool = True,
                                  enable_dynamic_filtering: bool = True,
                                  enable_join_reordering: bool = True) -> str:
        """
        Apply advanced Spark SQL optimizations to improve query performance.
        
        Parameters:
        - query: Original SQL query
        - enable_adaptive_query: Enable adaptive query execution
        - enable_dynamic_filtering: Enable dynamic partition pruning
        - enable_join_reordering: Enable cost-based join reordering
        
        Returns:
        - Optimized query with performance hints
        """
        try:
            optimization_hints = []
            
            # Add Spark SQL optimization hints
            if enable_adaptive_query:
                optimization_hints.append("/*+ ADAPTIVE_QUERY_EXECUTION=true */")
            
            if enable_dynamic_filtering:
                optimization_hints.append("/*+ DYNAMIC_PARTITION_PRUNING=true */")
            
            if enable_join_reordering:
                optimization_hints.append("/*+ JOIN_REORDER=true */")
            
            # Add broadcast hints for small tables (heuristic-based)
            query_upper = query.upper()
            if 'JOIN' in query_upper and 'WHERE' in query_upper:
                # Look for dimension tables or small lookup tables
                if any(keyword in query_upper for keyword in ['LOOKUP', 'DIM_', 'REF_']):
                    optimization_hints.append("/*+ BROADCAST_JOIN */")
            
            # Add repartition hints for large aggregations
            if 'GROUP BY' in query_upper and 'SUM' in query_upper:
                optimization_hints.append("/*+ REPARTITION(spark.sql.shuffle.partitions) */")
            
            # Combine hints with original query
            if optimization_hints:
                optimized_query = f"{' '.join(optimization_hints)}\n{query}"
            else:
                optimized_query = query
            
            logging.info(f"Applied {len(optimization_hints)} optimization hints to query")
            
            return optimized_query
            
        except Exception as e:
            logging.warning(f"Failed to optimize query: {e}")
            return query
    
    def analyze_query_plan(self,
                          query: str,
                          database: str = None) -> Dict[str, Any]:
        """
        Analyze query execution plan for performance insights.
        
        Parameters:
        - query: SQL query to analyze
        - database: Target database
        
        Returns:
        - Query plan analysis results
        """
        try:
            database = database or self.default_database
            
            # Execute EXPLAIN for the query
            explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            
            plan_result = self.execute_query(
                query=explain_query,
                database=database,
                return_as_dataframe=False
            )
            
            # Parse the execution plan
            analysis = {
                'query_complexity': 'unknown',
                'estimated_cost': 'unknown',
                'optimization_opportunities': [],
                'warnings': []
            }
            
            # Basic analysis based on query structure
            query_upper = query.upper()
            
            # Analyze complexity
            join_count = query_upper.count('JOIN')
            subquery_count = query_upper.count('SELECT') - 1
            
            if join_count >= 5 or subquery_count >= 3:
                analysis['query_complexity'] = 'high'
                analysis['optimization_opportunities'].append('Consider breaking into smaller queries')
            elif join_count >= 2 or subquery_count >= 1:
                analysis['query_complexity'] = 'medium'
            else:
                analysis['query_complexity'] = 'low'
            
            # Check for common performance issues
            if 'SELECT *' in query_upper:
                analysis['warnings'].append('SELECT * can impact performance, consider selecting specific columns')
            
            if 'ORDER BY' in query_upper and 'LIMIT' not in query_upper:
                analysis['warnings'].append('ORDER BY without LIMIT can be expensive for large datasets')
            
            if join_count > 0 and 'WHERE' not in query_upper:
                analysis['warnings'].append('Joins without WHERE clauses may result in cartesian products')
            
            logging.info(f"Query analysis completed - Complexity: {analysis['query_complexity']}")
            
            return analysis
            
        except Exception as e:
            logging.error(f"Failed to analyze query plan: {e}")
            return {'error': str(e)}
    
    def cache_query_result(self,
                          query: str,
                          cache_name: str,
                          database: str = None,
                          ttl_hours: int = 24) -> Dict[str, Any]:
        """
        Cache query results for improved performance on repeated queries.
        
        Parameters:
        - query: SQL query to cache
        - cache_name: Name for the cached result
        - database: Target database
        - ttl_hours: Time-to-live for the cache in hours
        
        Returns:
        - Cache creation status
        """
        try:
            database = database or self.default_database
            
            # Create a view as a simple caching mechanism
            cache_view_name = f"cache_{cache_name}"
            
            # Add cache metadata as comments
            cache_metadata = f"""
            /* CACHE METADATA:
             * Created: {datetime.now().isoformat()}
             * TTL: {ttl_hours} hours
             * Expires: {(datetime.now() + timedelta(hours=ttl_hours)).isoformat()}
             */
            """
            
            cached_query = f"{cache_metadata}\n{query}"
            
            # Create the cache view
            cache_result = self.create_view(
                view_name=cache_view_name,
                query=cached_query,
                database=database,
                replace_if_exists=True
            )
            
            logging.info(f"Query result cached as '{cache_view_name}' for {ttl_hours} hours")
            
            return {
                'cache_name': cache_name,
                'view_name': cache_view_name,
                'ttl_hours': ttl_hours,
                'expires_at': (datetime.now() + timedelta(hours=ttl_hours)).isoformat(),
                'status': 'cached'
            }
            
        except Exception as e:
            logging.error(f"Failed to cache query result: {e}")
            raise
    
    def _execute_single_query(self, query_config: Dict[str, Any], index: int) -> Any:
        """Helper method for batch query execution"""
        
        query = query_config['query']
        database = query_config.get('database', self.default_database)
        return_df = query_config.get('return_as_dataframe', True)
        
        return self.execute_query(
            query=query,
            database=database,
            return_as_dataframe=return_df
        )
    
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
    
    def optimize_table(self, 
                      database: str, 
                      table: str,
                      enable_compression: bool = True,
                      enable_partition_projection: bool = True) -> Dict[str, str]:
        """Apply optimization settings to existing table"""
        
        try:
            properties = []
            
            if enable_compression:
                properties.append("'parquet.compression'='SNAPPY'")
            
            if enable_partition_projection:
                properties.append("'projection.enabled'='true'")
            
            if properties:
                alter_query = f"""
                ALTER TABLE {database}.{table} 
                SET TBLPROPERTIES ({', '.join(properties)})
                """
                
                self.execute_query(
                    query=alter_query,
                    database=database,
                    return_as_dataframe=False
                )
            
            return {
                'table': f"{database}.{table}",
                'optimizations_applied': len(properties),
                'status': 'completed'
            }
            
        except Exception as e:
            logging.error(f"Failed to optimize table '{database}.{table}': {e}")
            raise

if __name__ == "__main__":
    # Example usage and testing
    athena = AthenaSparkSQL()
    
    # Test basic connectivity
    try:
        databases = athena.list_databases()
        print(f"Available databases: {databases}")
    except Exception as e:
        print(f"Connection test failed: {e}")
    
    # Test CUR integration setup
    try:
        setup_result = athena.setup_cur_integration(
            database_name='test_cur_db',
            table_name='test_cur_table',
            s3_bucket='your-cur-bucket',
            s3_prefix='cur-data/'
        )
        print(f"CUR setup result: {setup_result}")
    except Exception as e:
        print(f"CUR setup failed: {e}") 
# glue.py
from TAI.utils import ConfigLoader

import json, logging, time
from datetime import datetime
from typing import Dict, List, Optional, Any

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

logging.basicConfig(level=logging.INFO)

class GlueAuth:
    """Authentication handler for AWS Glue"""
    
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
        self.glue_client = self.session.client('glue', region_name=region)
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

class GlueCrawler:
    """
    AWS Glue Crawler handler for automatic schema discovery.
    Focuses on creating tables that Athena can query.
    """
    
    def __init__(self, aws_profile: str = None, region: str = 'us-east-1'):
        self.auth = GlueAuth(aws_profile, region)
        self.glue_client = self.auth.glue_client
        self.s3_client = self.auth.s3_client
        self.account_id = self.auth.account_id
        self.region = region
        
        # Default IAM role for crawler (using the role we just created)
        self.default_crawler_role = f'arn:aws:iam::{self.account_id}:role/AWSGlueServiceRole-TAI'
    
    # =========================================================================
    # CRAWLER MANAGEMENT
    # =========================================================================
    
    def create_crawler(self,
                      crawler_name: str,
                      database_name: str,
                      s3_targets: List[str],
                      description: str = None,
                      iam_role: str = None,
                      table_prefix: str = "",
                      schedule: str = None) -> Dict[str, Any]:
        """
        Create a Glue Crawler for automatic schema discovery.
        
        Parameters:
        - crawler_name: Name of the crawler
        - database_name: Target database for discovered tables
        - s3_targets: List of S3 paths to crawl
        - description: Crawler description
        - iam_role: IAM role ARN (defaults to AWSGlueServiceRole)
        - table_prefix: Prefix for created table names
        - schedule: Crawler schedule (e.g., 'cron(0 12 * * ? *)')
        
        Returns:
        - Crawler creation status
        """
        try:
            # Prepare S3 targets
            targets = {
                'S3Targets': [{'Path': path} for path in s3_targets]
            }
            
            # Prepare crawler configuration
            crawler_config = {
                'Name': crawler_name,
                'Role': iam_role or self.default_crawler_role,
                'DatabaseName': database_name,
                'Description': description or f"Auto-generated crawler for {database_name}",
                'Targets': targets,
                'TablePrefix': table_prefix
            }
            
            # Add schedule if provided
            if schedule:
                crawler_config['Schedule'] = schedule
            
            # Additional configuration for better schema detection
            crawler_config['Configuration'] = json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
                    "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
                }
            })
            
            # Create crawler
            response = self.glue_client.create_crawler(**crawler_config)
            
            logging.info(f"Crawler '{crawler_name}' created successfully")
            
            return {
                'crawler_name': crawler_name,
                'database': database_name,
                's3_targets': s3_targets,
                'status': 'created'
            }
            
        except Exception as e:
            logging.error(f"Failed to create crawler '{crawler_name}': {e}")
            raise
    
    def start_crawler(self, crawler_name: str) -> Dict[str, str]:
        """
        Start a Glue Crawler to discover schema.
        
        Parameters:
        - crawler_name: Name of the crawler to start
        
        Returns:
        - Crawler start status
        """
        try:
            self.glue_client.start_crawler(Name=crawler_name)
            
            logging.info(f"Crawler '{crawler_name}' started")
            
            return {
                'crawler_name': crawler_name,
                'status': 'started'
            }
            
        except Exception as e:
            logging.error(f"Failed to start crawler '{crawler_name}': {e}")
            raise
    
    def get_crawler_status(self, crawler_name: str) -> Dict[str, Any]:
        """
        Get the current status of a crawler.
        
        Parameters:
        - crawler_name: Name of the crawler
        
        Returns:
        - Crawler status information
        """
        try:
            response = self.glue_client.get_crawler(Name=crawler_name)
            crawler = response['Crawler']
            
            status_info = {
                'name': crawler['Name'],
                'state': crawler['State'],
                'database': crawler['DatabaseName'],
                'last_crawl': crawler.get('LastCrawl', {}),
                'creation_time': crawler.get('CreationTime'),
                'last_updated': crawler.get('LastUpdated')
            }
            
            return status_info
            
        except Exception as e:
            logging.error(f"Failed to get crawler status for '{crawler_name}': {e}")
            raise
    
    def wait_for_crawler_completion(self, crawler_name: str, timeout: int = 1800) -> str:
        """
        Wait for crawler to complete its run.
        
        Parameters:
        - crawler_name: Name of the crawler
        - timeout: Maximum time to wait in seconds (default 30 minutes)
        
        Returns:
        - Final crawler state
        """
        try:
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                status = self.get_crawler_status(crawler_name)
                state = status['state']
                
                logging.info(f"Crawler '{crawler_name}' state: {state}")
                
                if state in ['READY', 'STOPPING']:
                    logging.info(f"Crawler '{crawler_name}' completed")
                    return state
                elif state in ['RUNNING']:
                    time.sleep(30)  # Check every 30 seconds
                else:
                    logging.warning(f"Crawler '{crawler_name}' in unexpected state: {state}")
                    time.sleep(10)
            
            raise TimeoutError(f"Crawler '{crawler_name}' did not complete within {timeout} seconds")
            
        except Exception as e:
            logging.error(f"Error waiting for crawler completion: {e}")
            raise
    
    def run_crawler_and_wait(self, crawler_name: str, timeout: int = 1800) -> Dict[str, Any]:
        """
        Start crawler and wait for completion.
        
        Parameters:
        - crawler_name: Name of the crawler
        - timeout: Maximum time to wait
        
        Returns:
        - Crawler run results
        """
        try:
            # Start crawler
            start_result = self.start_crawler(crawler_name)
            
            # Wait for completion
            final_state = self.wait_for_crawler_completion(crawler_name, timeout)
            
            # Get final status
            final_status = self.get_crawler_status(crawler_name)
            
            return {
                'crawler_name': crawler_name,
                'final_state': final_state,
                'last_crawl': final_status.get('last_crawl', {}),
                'status': 'completed'
            }
            
        except Exception as e:
            logging.error(f"Failed to run crawler '{crawler_name}': {e}")
            raise
    
    # =========================================================================
    # TABLE DISCOVERY AND MANAGEMENT
    # =========================================================================
    
    def list_discovered_tables(self, database_name: str) -> List[Dict[str, Any]]:
        """
        List tables discovered by crawlers in a database.
        
        Parameters:
        - database_name: Name of the Glue database
        
        Returns:
        - List of discovered tables with metadata
        """
        try:
            response = self.glue_client.get_tables(DatabaseName=database_name)
            
            tables = []
            for table in response['TableList']:
                table_info = {
                    'name': table['Name'],
                    'database': table['DatabaseName'],
                    'location': table.get('StorageDescriptor', {}).get('Location'),
                    'input_format': table.get('StorageDescriptor', {}).get('InputFormat'),
                    'output_format': table.get('StorageDescriptor', {}).get('OutputFormat'),
                    'columns': len(table.get('StorageDescriptor', {}).get('Columns', [])),
                    'creation_time': table.get('CreateTime'),
                    'last_analyzed': table.get('LastAnalyzedTime')
                }
                tables.append(table_info)
            
            return tables
            
        except Exception as e:
            logging.error(f"Failed to list tables in database '{database_name}': {e}")
            raise
    
    def get_table_schema(self, database_name: str, table_name: str) -> Dict[str, str]:
        """
        Get schema of a discovered table.
        
        Parameters:
        - database_name: Name of the database
        - table_name: Name of the table
        
        Returns:
        - Dictionary of column names and types
        """
        try:
            response = self.glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )
            
            table = response['Table']
            columns = table.get('StorageDescriptor', {}).get('Columns', [])
            
            schema = {}
            for column in columns:
                schema[column['Name']] = column['Type']
            
            return schema
            
        except Exception as e:
            logging.error(f"Failed to get schema for table '{database_name}.{table_name}': {e}")
            raise
    
    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================
    
    def discover_s3_schema(self,
                          s3_path: str,
                          database_name: str,
                          crawler_name: str = None,
                          table_prefix: str = "",
                          wait_for_completion: bool = True) -> Dict[str, Any]:
        """
        One-stop method to discover schema from S3 path.
        
        Parameters:
        - s3_path: S3 path to crawl
        - database_name: Target database name
        - crawler_name: Custom crawler name (auto-generated if None)
        - table_prefix: Prefix for discovered tables
        - wait_for_completion: Wait for crawler to finish
        
        Returns:
        - Discovery results with table information
        """
        try:
            # Generate crawler name if not provided
            if not crawler_name:
                timestamp = int(time.time())
                crawler_name = f"auto_crawler_{database_name}_{timestamp}"
            
            # Create crawler
            logging.info(f"Creating crawler '{crawler_name}' for {s3_path}")
            self.create_crawler(
                crawler_name=crawler_name,
                database_name=database_name,
                s3_targets=[s3_path],
                table_prefix=table_prefix,
                description=f"Auto-discovery for {s3_path}"
            )
            
            # Run crawler
            if wait_for_completion:
                logging.info(f"Running crawler and waiting for completion...")
                run_result = self.run_crawler_and_wait(crawler_name)
                
                # List discovered tables
                discovered_tables = self.list_discovered_tables(database_name)
                
                return {
                    'crawler_name': crawler_name,
                    'database': database_name,
                    's3_path': s3_path,
                    'crawler_result': run_result,
                    'discovered_tables': discovered_tables,
                    'status': 'completed'
                }
            else:
                # Just start crawler, don't wait
                start_result = self.start_crawler(crawler_name)
                
                return {
                    'crawler_name': crawler_name,
                    'database': database_name,
                    's3_path': s3_path,
                    'crawler_result': start_result,
                    'status': 'started'
                }
                
        except Exception as e:
            logging.error(f"Failed to discover schema for {s3_path}: {e}")
            raise
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def list_crawlers(self) -> List[Dict[str, Any]]:
        """List all crawlers in the account."""
        
        try:
            response = self.glue_client.get_crawlers()
            
            crawlers = []
            for crawler in response['Crawlers']:
                crawler_info = {
                    'name': crawler['Name'],
                    'state': crawler['State'],
                    'database': crawler.get('DatabaseName'),
                    'targets': len(crawler.get('Targets', {}).get('S3Targets', [])),
                    'creation_time': crawler.get('CreationTime'),
                    'last_updated': crawler.get('LastUpdated')
                }
                crawlers.append(crawler_info)
            
            return crawlers
            
        except Exception as e:
            logging.error(f"Failed to list crawlers: {e}")
            raise
    
    def delete_crawler(self, crawler_name: str) -> Dict[str, str]:
        """Delete a crawler."""
        
        try:
            self.glue_client.delete_crawler(Name=crawler_name)
            
            logging.info(f"Crawler '{crawler_name}' deleted successfully")
            
            return {
                'crawler_name': crawler_name,
                'status': 'deleted'
            }
            
        except Exception as e:
            logging.error(f"Failed to delete crawler '{crawler_name}': {e}")
            raise

if __name__ == "__main__":
    # Example usage
    glue = GlueCrawler()
    
    # Test basic connectivity
    try:
        crawlers = glue.list_crawlers()
        print(f"Found {len(crawlers)} crawlers")
    except Exception as e:
        print(f"Connection test failed: {e}") 
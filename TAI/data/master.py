# data_master.py
from TAI.utils import ConfigLoader

import json, os, sys, inspect, logging
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import time
from typing import Dict, List, Optional, Union, Any

import pandas as pd
import numpy as np
import polars as pl
import psycopg2

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

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
    
    # =========================================================================
    # AWS COST AND USAGE REPORTS (CUR) & DATA EXPORTS
    # =========================================================================
    
    def create_data_export(self, 
                              export_name: str,
                              s3_bucket: str,
                              s3_prefix: str = 'exports/',
                              export_type: str = 'CUR_2_0',
                              format_type: str = 'PARQUET',
                              compression: str = 'GZIP',
                              time_granularity: str = 'DAILY',
                              aws_profile: str = None,
                              region: str = 'us-east-1',
                              include_resources: bool = True,
                              include_split_cost_allocation: bool = True,
                              include_manual_discounts: bool = True) -> Dict[str, Any]:
        """
        Create AWS Data Export using the new Data Exports API (supports all export types).
        
        Based on: https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html
        
        Supported Export Types:
        - CUR_2_0: Cost and Usage Reports 2.0 with enhanced columns
        - FOCUS_1_0: FOCUS 1.0 billing data with AWS-specific columns
        - COST_OPTIMIZATION_RECOMMENDATIONS: AWS cost optimization insights
        - CARBON_EMISSIONS: Carbon footprint data for AWS services
        
        Parameters:
        - export_name: Name for the data export
        - s3_bucket: S3 bucket to store the export data
        - s3_prefix: S3 prefix for organizing exports
        - export_type: Type of export ('CUR_2_0', 'FOCUS_1_0', 'COST_OPTIMIZATION_RECOMMENDATIONS', 'CARBON_EMISSIONS')
        - format_type: Export format ('PARQUET', 'CSV')
        - compression: Compression type ('GZIP', 'BZIP2', 'ZIP')
        - time_granularity: Time granularity ('DAILY', 'MONTHLY', 'SYNCHRONOUS')
        - aws_profile: AWS profile to use (optional)
        - region: AWS region for the export
        - include_resources: Include resource-level details (for CUR/FOCUS)
        - include_split_cost_allocation: Include split cost allocation data
        - include_manual_discounts: Include manual discount compatibility (CUR only)
        
        Returns:
        - Dictionary containing export creation details
        """
        try:
            # Initialize AWS client
            session = boto3.Session(profile_name=aws_profile, region_name=region) if aws_profile else boto3.Session(region_name=region)
            
            # Create Data Exports client (using Cost Explorer for now as Data Exports API is newer)
            data_exports_client = session.client('ce', region_name=region)
            
            # Dynamic export configuration templates
            export_templates = {
                'CUR_2_0': {
                    'description': 'Cost and Usage Report 2.0',
                    'table_name': 'COST_AND_USAGE_REPORT',
                    'query_statement': 'SELECT bill_bill_type, bill_billing_entity, bill_billing_period_end_date, bill_billing_period_start_date, bill_invoice_id, bill_invoicing_entity, bill_payer_account_id, bill_payer_account_name, cost_category, discount, discount_bundled_discount, discount_total_discount, identity_line_item_id, identity_time_interval, line_item_availability_zone, line_item_blended_cost, line_item_blended_rate, line_item_currency_code, line_item_legal_entity, line_item_line_item_description, line_item_line_item_type, line_item_net_unblended_cost, line_item_net_unblended_rate, line_item_normalization_factor, line_item_normalized_usage_amount, line_item_operation, line_item_product_code, line_item_resource_id, line_item_tax_type, line_item_unblended_cost, line_item_unblended_rate, line_item_usage_account_id, line_item_usage_account_name, line_item_usage_amount, line_item_usage_end_date, line_item_usage_start_date, line_item_usage_type, pricing_currency, pricing_lease_contract_length, pricing_offering_class, pricing_public_on_demand_cost, pricing_public_on_demand_rate, pricing_purchase_option, pricing_rate_code, pricing_rate_id, pricing_term, pricing_unit, product, product_comment, product_fee_code, product_fee_description, product_from_location, product_from_location_type, product_from_region_code, product_instance_family, product_instance_type, product_instancesku, product_location, product_location_type, product_operation, product_pricing_unit, product_product_family, product_region_code, product_servicecode, product_sku, product_to_location, product_to_location_type, product_to_region_code, product_usagetype, reservation_amortized_upfront_cost_for_usage, reservation_amortized_upfront_fee_for_billing_period, reservation_availability_zone, reservation_effective_cost, reservation_end_time, reservation_modification_status, reservation_net_amortized_upfront_cost_for_usage, reservation_net_amortized_upfront_fee_for_billing_period, reservation_net_effective_cost, reservation_net_recurring_fee_for_usage, reservation_net_unused_amortized_upfront_fee_for_billing_period, reservation_net_unused_recurring_fee, reservation_net_upfront_value, reservation_normalized_units_per_reservation, reservation_number_of_reservations, reservation_recurring_fee_for_usage, reservation_reservation_a_r_n, reservation_start_time, reservation_subscription_id, reservation_total_reserved_normalized_units, reservation_total_reserved_units, reservation_units_per_reservation, reservation_unused_amortized_upfront_fee_for_billing_period, reservation_unused_normalized_unit_quantity, reservation_unused_quantity, reservation_unused_recurring_fee, reservation_upfront_value, resource_tags, savings_plan_amortized_upfront_commitment_for_billing_period, savings_plan_end_time, savings_plan_instance_type_family, savings_plan_net_amortized_upfront_commitment_for_billing_period, savings_plan_net_recurring_commitment_for_billing_period, savings_plan_net_savings_plan_effective_cost, savings_plan_offering_type, savings_plan_payment_option, savings_plan_purchase_term, savings_plan_recurring_commitment_for_billing_period, savings_plan_region, savings_plan_savings_plan_a_r_n, savings_plan_savings_plan_effective_cost, savings_plan_savings_plan_rate, savings_plan_start_time, savings_plan_total_commitment_to_date, savings_plan_used_commitment, split_line_item_actual_usage, split_line_item_net_split_cost, split_line_item_net_unused_cost, split_line_item_parent_resource_id, split_line_item_public_on_demand_split_cost, split_line_item_public_on_demand_unused_cost, split_line_item_reserved_usage, split_line_item_split_cost, split_line_item_split_usage, split_line_item_split_usage_ratio, split_line_item_unused_cost FROM COST_AND_USAGE_REPORT',
                    'default_frequency': 'SYNCHRONOUS',  # AWS only supports SYNCHRONOUS
                    'supports_manual_discounts': True,
                    'supported_formats': ['PARQUET', 'TEXT_OR_CSV']
                },
                'FOCUS_1_0': {
                    'description': 'FOCUS 1.0 with AWS columns',
                    'table_name': 'FOCUS_1_0',  # Simplified table name
                    'default_frequency': 'SYNCHRONOUS',
                    'supports_manual_discounts': False,
                    'supported_formats': ['PARQUET']
                },
                'COST_OPTIMIZATION_RECOMMENDATIONS': {
                    'description': 'Cost Optimization Recommendations',
                    'table_name': 'COST_OPTIMIZATION_RECOMMENDATIONS',
                    'default_frequency': 'SYNCHRONOUS',
                    'supports_manual_discounts': False,
                    'supported_formats': ['PARQUET', 'TEXT_OR_CSV']
                },
                'CARBON_EMISSIONS': {
                    'description': 'Carbon Emissions Data',
                    'table_name': 'CARBON_FOOTPRINT',  # Simplified table name
                    'default_frequency': 'SYNCHRONOUS',
                    'supports_manual_discounts': False,
                    'supported_formats': ['PARQUET']
                }
            }
            
            # Validate export type
            if export_type not in export_templates:
                raise ValueError(f"Unsupported export type: {export_type}. Supported types: {list(export_templates.keys())}")
            
            template = export_templates[export_type]
            
            # Build table configurations dynamically (AWS expects 'TRUE'/'FALSE')
            table_config = {}
            if include_resources:
                table_config['INCLUDE_RESOURCES'] = 'TRUE'
            if include_split_cost_allocation:
                table_config['INCLUDE_SPLIT_COST_ALLOCATION_DATA'] = 'TRUE'
            if include_manual_discounts and template['supports_manual_discounts']:
                table_config['INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY'] = 'TRUE'
            else:
                # Set to FALSE if not explicitly enabled
                table_config['INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY'] = 'FALSE'
            
            # Add TIME_GRANULARITY for CUR exports
            if export_type == 'CUR_2_0':
                table_config['TIME_GRANULARITY'] = 'HOURLY'
            
            # Determine frequency - all exports now use SYNCHRONOUS based on AWS validation
            frequency = template['default_frequency']
            
            # Validate and adjust format based on export type
            if format_type == 'CSV' and 'TEXT_OR_CSV' in template.get('supported_formats', []):
                validated_format = 'TEXT_OR_CSV'
            elif format_type in template.get('supported_formats', ['PARQUET']):
                validated_format = format_type
            else:
                # Default to PARQUET if requested format not supported
                validated_format = 'PARQUET'
                logging.warning(f"Format '{format_type}' not supported for {export_type}, using PARQUET")
            
            # Use specific query statement from template if available, otherwise use generic
            query_statement = template.get('query_statement', f"SELECT * FROM {template['table_name']}")
            
            # Set compression based on format (PARQUET format uses PARQUET compression)
            if validated_format == 'PARQUET':
                compression_setting = 'PARQUET'
            else:
                compression_setting = compression
            
            # Build dynamic export configuration with proper QueryStatement
            export_config = {
                'Name': export_name,
                'DataQuery': {
                    'QueryStatement': query_statement,
                    'TableConfigurations': {
                        template['table_name']: table_config
                    } if table_config else {
                        template['table_name']: {}
                    }
                },
                'DestinationConfigurations': {
                    'S3Destination': {
                        'S3Bucket': s3_bucket,
                        'S3Prefix': s3_prefix,
                        'S3Region': 'us-west-2',  # Match working export region
                        'S3OutputConfigurations': {
                            'OutputType': 'CUSTOM',
                            'Format': validated_format,
                            'Compression': compression_setting,
                            'Overwrite': 'OVERWRITE_REPORT'
                        }
                    }
                },
                'RefreshCadence': {
                    'Frequency': frequency
                }
            }
            
            # Call real AWS Data Exports API
            logging.info(f"Creating Data Export: {export_name}")
            logging.info(f"Export Type: {export_type}")
            logging.info(f"S3 Destination: s3://{s3_bucket}/{s3_prefix}")
            
            try:
                # Use bcm-data-exports service for creating exports
                bcm_client = session.client('bcm-data-exports', region_name=region)
                
                # Create the export using real API with correct parameter structure
                create_params = {
                    'Export': export_config
                }
                response = bcm_client.create_export(**create_params)
                
                logging.info(f"Successfully created export: {export_name}")
                
                return {
                    'ExportName': export_name,
                    'ExportArn': response.get('ExportArn', ''),
                    'Status': 'CREATED',
                    'S3Bucket': s3_bucket,
                    'S3Prefix': s3_prefix,
                    'ExportType': export_type,
                    'Format': format_type,
                    'Response': response
                }
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ValidationException':
                    logging.warning(f"Validation error creating export: {e}")
                elif error_code in ['AccessDenied', 'UnauthorizedOperation']:
                    logging.warning(f"Access denied creating export: {e}")
                else:
                    logging.error(f"AWS error creating export: {e}")
                
                # Return configuration info even if API call fails
                return {
                    'ExportName': export_name,
                    'Status': 'FAILED',
                    'S3Bucket': s3_bucket,
                    'S3Prefix': s3_prefix,
                    'ExportType': export_type,
                    'Format': format_type,
                    'Error': str(e),
                    'Configuration': export_config
                }
            except Exception as e:
                logging.error(f"Unexpected error creating export: {e}")
                raise
            
        except Exception as e:
            logging.error(f"Failed to create data export: {e}")
            raise

    def create_cur_data_export(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Backward compatibility method. Use create_data_export() instead.
        """
        import warnings
        warnings.warn(
            "create_cur_data_export() is deprecated. Use create_data_export() instead.", 
            DeprecationWarning, 
            stacklevel=2
        )
        return self.create_data_export(*args, **kwargs)

    def get_supported_export_types(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all supported Data Export types and their capabilities.
        
        Returns:
        - Dictionary with export type information including supported formats, 
          table configurations, and other capabilities
        """
        return {
            'CUR_2_0': {
                'description': 'Cost and Usage Report 2.0 with enhanced columns',
                'supported_formats': ['PARQUET', 'TEXT_OR_CSV'],
                'frequency': 'SYNCHRONOUS',
                'supports_resources': True,
                'supports_split_cost_allocation': True,
                'supports_manual_discounts': True,
                'table_name': 'COST_AND_USAGE_REPORT'
            },
            'FOCUS_1_0': {
                'description': 'FOCUS 1.0 billing data with AWS-specific columns',
                'supported_formats': ['PARQUET'],
                'frequency': 'SYNCHRONOUS',
                'supports_resources': True,
                'supports_split_cost_allocation': True,
                'supports_manual_discounts': False,
                'table_name': 'FOCUS_1_0'
            },
            'COST_OPTIMIZATION_RECOMMENDATIONS': {
                'description': 'AWS cost optimization insights and recommendations',
                'supported_formats': ['PARQUET', 'TEXT_OR_CSV'],
                'frequency': 'SYNCHRONOUS',
                'supports_resources': False,
                'supports_split_cost_allocation': False,
                'supports_manual_discounts': False,
                'table_name': 'COST_OPTIMIZATION_RECOMMENDATIONS'
            },
            'CARBON_EMISSIONS': {
                'description': 'Carbon footprint data for AWS services',
                'supported_formats': ['PARQUET'],
                'frequency': 'SYNCHRONOUS',
                'supports_resources': False,
                'supports_split_cost_allocation': False,
                'supports_manual_discounts': False,
                'table_name': 'CARBON_FOOTPRINT'
            }
        }

    def list_data_exports(self,
                         aws_profile: str = None,
                         region: str = 'us-east-1') -> List[Dict[str, Any]]:
        """
        List all existing AWS Data Exports.
        
        Parameters:
        - aws_profile: AWS profile to use (optional)
        - region: AWS region
        
        Returns:
        - List of dictionaries containing export details
        """
        try:
            # Initialize AWS client
            session = boto3.Session(profile_name=aws_profile, region_name=region) if aws_profile else boto3.Session(region_name=region)
            
            exports_list = []
            
            # Try AWS Data Exports service (bcm-data-exports)
            try:
                bcm_client = session.client('bcm-data-exports', region_name=region)
                response = bcm_client.list_exports()
                
                if 'Exports' in response:
                    for export in response['Exports']:
                        export_info = {
                            'ExportName': export.get('ExportName', 'Unknown'),
                            'ExportType': 'Data_Export_2_0',  # Default for new data exports
                            'Status': export.get('ExportStatus', {}).get('StatusCode', 'Unknown'),
                            'S3Bucket': '',
                            'S3Prefix': '',
                            'Format': '',
                            'CreatedDate': str(export.get('CreationTime', '')),
                            'LastModified': str(export.get('LastUpdatedTime', ''))
                        }
                        
                        # Need to call get_export() for detailed S3 information
                        # The list_exports() only returns basic info
                        try:
                            export_arn = export.get('ExportArn', '')
                            if export_arn:
                                detail_response = bcm_client.get_export(ExportArn=export_arn)
                                if 'Export' in detail_response:
                                    detail_export = detail_response['Export']
                                    dest_configs = detail_export.get('DestinationConfigurations', {})
                                    
                                    # Handle S3Destination structure
                                    if 'S3Destination' in dest_configs:
                                        s3_dest = dest_configs['S3Destination']
                                        export_info['S3Bucket'] = s3_dest.get('S3Bucket', '')
                                        export_info['S3Prefix'] = s3_dest.get('S3Prefix', '')
                                        s3_output_configs = s3_dest.get('S3OutputConfigurations', {})
                                        export_info['Format'] = s3_output_configs.get('Format', '')
                                    
                                    # Handle S3OutputConfigurations structure
                                    elif 'S3OutputConfigurations' in dest_configs:
                                        s3_configs = dest_configs['S3OutputConfigurations']
                                        export_info['S3Bucket'] = s3_configs.get('BucketName', '')
                                        export_info['S3Prefix'] = s3_configs.get('Prefix', '')
                                        export_info['Format'] = s3_configs.get('Format', '')
                        except Exception as e:
                            logging.warning(f"Could not get detailed info for export {export.get('ExportName', 'Unknown')}: {e}")
                        
                        exports_list.append(export_info)
                        
                logging.info(f"Found {len(exports_list)} data exports via bcm-data-exports")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['AccessDenied', 'UnauthorizedOperation']:
                    logging.warning("No access to bcm-data-exports service")
                else:
                    logging.warning(f"bcm-data-exports error: {error_code}")
            except Exception as e:
                logging.warning(f"bcm-data-exports service not available: {e}")
            
            # Try legacy Cost and Usage Reports (CUR) service
            try:
                cur_client = session.client('cur', region_name='us-east-1')  # CUR is only in us-east-1
                response = cur_client.describe_report_definitions()
                
                if 'ReportDefinitions' in response:
                    for report in response['ReportDefinitions']:
                        export_info = {
                            'ExportName': report.get('ReportName', 'Unknown'),
                            'ExportType': 'Legacy_CUR',
                            'Status': 'ACTIVE',
                            'S3Bucket': report.get('S3Bucket', ''),
                            'S3Prefix': report.get('S3Prefix', ''),
                            'Format': report.get('Format', ''),
                            'CreatedDate': '',
                            'LastModified': ''
                        }
                        exports_list.append(export_info)
                        
                logging.info(f"Found {len(response.get('ReportDefinitions', []))} legacy CUR reports")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['AccessDenied', 'UnauthorizedOperation']:
                    logging.warning("No access to CUR service")
                else:
                    logging.warning(f"CUR service error: {error_code}")
            except Exception as e:
                logging.warning(f"CUR service not available: {e}")
            
            # If no exports found, return empty list instead of mock data
            if not exports_list:
                logging.info("No data exports found in AWS account")
                
            logging.info(f"Total found {len(exports_list)} data exports")
            return exports_list
            
        except Exception as e:
            logging.error(f"Failed to list data exports: {e}")
            raise

    def get_export_details(self,
                          export_name: str,
                          aws_profile: str = None,
                          region: str = 'us-east-1') -> Dict[str, Any]:
        """
        Get detailed information about a specific data export.
        
        Parameters:
        - export_name: Name of the export to get details for
        - aws_profile: AWS profile to use (optional)
        - region: AWS region
        
        Returns:
        - Dictionary containing detailed export information
        """
        try:
            exports = self.list_data_exports(aws_profile=aws_profile, region=region)
            
            for export in exports:
                if export['ExportName'] == export_name:
                    logging.info(f"Found export: {export_name}")
                    return export
            
            raise ValueError(f"Export '{export_name}' not found")
            
        except Exception as e:
            logging.error(f"Failed to get export details for {export_name}: {e}")
            raise

    def find_export_by_name(self,
                           export_name: str,
                           aws_profile: str = None,
                           region: str = 'us-east-1') -> Optional[Dict[str, Any]]:
        """
        Find a specific data export by name and return its S3 location.
        
        Parameters:
        - export_name: Name of the export to find
        - aws_profile: AWS profile to use (optional)
        - region: AWS region
        
        Returns:
        - Dictionary with export details including S3 location, or None if not found
        """
        try:
            export_details = self.get_export_details(export_name, aws_profile, region)
            
            result = {
                'export_name': export_details['ExportName'],
                'export_type': export_details['ExportType'],
                's3_bucket': export_details['S3Bucket'],
                's3_prefix': export_details['S3Prefix'],
                's3_full_location': f"s3://{export_details['S3Bucket']}/{export_details['S3Prefix']}",
                'format': export_details['Format'],
                'status': export_details['Status']
            }
            
            logging.info(f"Export '{export_name}' found at: {result['s3_full_location']}")
            return result
            
        except ValueError:
            logging.warning(f"Export '{export_name}' not found")
            return None
        except Exception as e:
            logging.error(f"Failed to find export {export_name}: {e}")
            raise

    def create_focus_export_from_existing(self,
                                        source_export_name: str,
                                        new_export_name: str,
                                        aws_profile: str = None,
                                        region: str = 'us-east-1') -> Dict[str, Any]:
        """
        Create a new FOCUS export using the same S3 bucket as an existing export.
        
        Parameters:
        - source_export_name: Name of existing export to get S3 location from
        - new_export_name: Name for the new FOCUS export
        - aws_profile: AWS profile to use (optional)
        - region: AWS region
        
        Returns:
        - Dictionary containing new export creation details
        """
        try:
            # Find the source export
            source_export = self.find_export_by_name(source_export_name, aws_profile, region)
            
            if not source_export:
                raise ValueError(f"Source export '{source_export_name}' not found")
            
            # Create new FOCUS export using same S3 bucket
            s3_bucket = source_export['s3_bucket']
            s3_prefix = f"focus-exports/{new_export_name}/"
            
            logging.info(f"Creating FOCUS export '{new_export_name}' using S3 bucket from '{source_export_name}'")
            
            return self.create_data_export(
                export_name=new_export_name,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                export_type='FOCUS_1_0',
                format_type='PARQUET',
                time_granularity='DAILY',
                aws_profile=aws_profile,
                region=region
            )
            
        except Exception as e:
            logging.error(f"Failed to create FOCUS export from existing: {e}")
            raise

    def query_latest_month_data(self,
                               export_name: str = 'cur2',
                               database_name: str = 'cur_database',
                               table_name: str = 'cur_table',
                               limit: int = 10,
                               aws_profile: str = None,
                               region: str = 'us-east-1') -> pd.DataFrame:
        """
        Query the latest month's data from a CUR export using Athena.
        
        Parameters:
        - export_name: Name of the export to query
        - database_name: Athena database name
        - table_name: Athena table name
        - limit: Number of records to return
        - aws_profile: AWS profile to use (optional)
        - region: AWS region
        
        Returns:
        - DataFrame containing the latest month's data
        """
        try:
            # Calculate date range for the latest complete month
            now = datetime.now()
            if now.month == 1:
                latest_month_start = datetime(now.year - 1, 12, 1)
            else:
                latest_month_start = datetime(now.year, now.month - 1, 1)
            
            # Calculate end of month
            if latest_month_start.month == 12:
                latest_month_end = datetime(latest_month_start.year + 1, 1, 1)
            else:
                latest_month_end = datetime(latest_month_start.year, latest_month_start.month + 1, 1)
            
            start_date = latest_month_start.strftime('%Y-%m-%d')
            end_date = latest_month_end.strftime('%Y-%m-%d')
            
            logging.info(f"Querying latest month data for {export_name}: {start_date} to {end_date}")
            
            # Build query for latest month data
            query = f'''
            SELECT 
                line_item_usage_start_date,
                line_item_usage_account_id,
                product_product_name,
                line_item_usage_type,
                line_item_operation,
                line_item_unblended_cost,
                line_item_usage_amount,
                line_item_currency_code,
                product_region
            FROM {database_name}.{table_name}
            WHERE line_item_usage_start_date >= '{start_date}'
            AND line_item_usage_start_date < '{end_date}'
            AND line_item_line_item_type != 'Tax'
            ORDER BY line_item_unblended_cost DESC
            LIMIT {limit}
            '''
            
            return self.query_cur_with_athena(
                query=query,
                database_name=database_name,
                aws_profile=aws_profile,
                region=region
            )
            
        except Exception as e:
            logging.error(f"Failed to query latest month data: {e}")
            raise

    def setup_cur_athena_integration(self,
                                   database_name: str = 'cur_database',
                                   table_name: str = 'cur_table',
                                   s3_bucket: str = None,
                                   s3_prefix: str = 'cur-exports/',
                                   workgroup: str = 'primary',
                                   aws_profile: str = None,
                                   region: str = 'us-east-1') -> Dict[str, str]:
        """
        Set up Amazon Athena integration for querying CUR data.
        
        Parameters:
        - database_name: Athena database name for CUR data
        - table_name: Athena table name for CUR data
        - s3_bucket: S3 bucket containing CUR data
        - s3_prefix: S3 prefix where CUR data is stored
        - workgroup: Athena workgroup to use
        - aws_profile: AWS profile to use
        - region: AWS region
        
        Returns:
        - Dictionary with setup details
        """
        try:
            # Initialize AWS clients
            session = boto3.Session(profile_name=aws_profile, region_name=region) if aws_profile else boto3.Session(region_name=region)
            
            athena_client = session.client('athena', region_name=region)
            glue_client = session.client('glue', region_name=region)
            
            # Set up default output location for Athena queries
            default_output_location = f's3://aws-athena-query-results-{region}-{self._get_account_id(aws_profile, region)}'
            
            # Create Athena database if it doesn't exist
            database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            
            # Execute database creation query
            response = athena_client.start_query_execution(
                QueryString=database_query,
                ResultConfiguration={'OutputLocation': default_output_location},
                WorkGroup=workgroup
            )
            
            # Wait for database creation to complete
            self._wait_for_athena_query(athena_client, response['QueryExecutionId'])
            
            # Create external table for CUR data (Parquet format)
            if s3_bucket:
                table_query = f'''
                CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
                    bill_billing_period_start_date string,
                    bill_billing_period_end_date string,
                    bill_payer_account_id string,
                    line_item_usage_account_id string,
                    line_item_product_code string,
                    line_item_usage_type string,
                    line_item_operation string,
                    line_item_line_item_type string,
                    line_item_usage_start_date string,
                    line_item_usage_end_date string,
                    line_item_usage_amount double,
                    line_item_currency_code string,
                    line_item_unblended_cost double,
                    line_item_blended_cost double,
                    product_product_name string,
                    product_region string,
                    pricing_term string,
                    reservation_reservation_a_r_n string,
                    savings_plan_savings_plan_a_r_n string
                )
                STORED AS PARQUET
                LOCATION 's3://{s3_bucket}/{s3_prefix}'
                '''
                
                # Execute table creation query
                response = athena_client.start_query_execution(
                    QueryString=table_query,
                    QueryExecutionContext={'Database': database_name},
                    ResultConfiguration={'OutputLocation': default_output_location},
                    WorkGroup=workgroup
                )
                
                self._wait_for_athena_query(athena_client, response['QueryExecutionId'])
            
            logging.info(f"Athena integration setup complete for database: {database_name}")
            
            return {
                'database_name': database_name,
                'table_name': table_name,
                's3_location': f's3://{s3_bucket}/{s3_prefix}' if s3_bucket else None,
                'workgroup': workgroup,
                'status': 'completed'
            }
            
        except Exception as e:
            logging.error(f"Failed to setup Athena integration: {e}")
            raise

    def query_cur_with_athena(self,
                            query: str,
                            database_name: str = 'cur_database',
                            workgroup: str = 'primary',
                            output_location: str = None,
                            aws_profile: str = None,
                            region: str = 'us-east-1',
                            return_as_dataframe: bool = True) -> Union[pd.DataFrame, Dict[str, Any]]:
        """
        Execute SQL queries against CUR data using Amazon Athena.
        
        Parameters:
        - query: SQL query to execute
        - database_name: Athena database name
        - workgroup: Athena workgroup
        - output_location: S3 location for query results (optional)
        - aws_profile: AWS profile to use
        - region: AWS region
        - return_as_dataframe: Return results as DataFrame if True
        
        Returns:
        - Query results as DataFrame or raw response
        """
        try:
            # Initialize AWS clients
            session = boto3.Session(profile_name=aws_profile, region_name=region) if aws_profile else boto3.Session(region_name=region)
            
            athena_client = session.client('athena', region_name=region)
            s3_client = session.client('s3', region_name=region)
            
            # Set up query execution context
            query_context = {'Database': database_name}
            
            # Configure result location if not provided
            if not output_location:
                # Use default S3 location for Athena results
                output_location = f's3://aws-athena-query-results-{region}-{self._get_account_id(aws_profile, region)}'
            
            # Execute the query
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext=query_context,
                ResultConfiguration={'OutputLocation': output_location},
                WorkGroup=workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            logging.info(f"Started Athena query execution: {query_execution_id}")
            
            # Wait for query completion
            final_status = self._wait_for_athena_query(athena_client, query_execution_id)
            
            if final_status != 'SUCCEEDED':
                raise Exception(f"Query failed with status: {final_status}")
            
            # Get query results
            if return_as_dataframe:
                return self._get_athena_results_as_dataframe(
                    athena_client, s3_client, query_execution_id, output_location
                )
            else:
                return athena_client.get_query_results(QueryExecutionId=query_execution_id)
                
        except Exception as e:
            logging.error(f"Failed to execute Athena query: {e}")
            raise

    def get_cost_by_service(self,
                          start_date: str,
                          end_date: str,
                          account_ids: List[str] = None,
                          group_by_account: bool = True,
                          aws_profile: str = None,
                          region: str = 'us-east-1') -> pd.DataFrame:
        """
        Get cost breakdown by AWS service using CUR data.
        
        Parameters:
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)  
        - account_ids: List of AWS account IDs to filter (optional)
        - group_by_account: Include account breakdown
        - aws_profile: AWS profile to use
        - region: AWS region
        
        Returns:
        - DataFrame with cost breakdown by service
        """
        account_filter = ""
        if account_ids:
            account_list = "', '".join(account_ids)
            account_filter = f"AND line_item_usage_account_id IN ('{account_list}')"
        
        group_by_clause = "line_item_usage_account_id, product_product_name" if group_by_account else "product_product_name"
        select_clause = "line_item_usage_account_id, product_product_name" if group_by_account else "product_product_name"
        
        query = f'''
        SELECT 
            {select_clause},
            SUM(line_item_unblended_cost) as total_cost,
            SUM(line_item_usage_amount) as total_usage,
            line_item_currency_code
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= '{start_date}'
        AND line_item_usage_start_date < '{end_date}'
        AND line_item_line_item_type != 'Tax'
        {account_filter}
        GROUP BY {group_by_clause}, line_item_currency_code
        ORDER BY total_cost DESC
        '''
        
        return self.query_cur_with_athena(
            query=query,
            aws_profile=aws_profile,
            region=region
        )

    def get_daily_cost_trend(self,
                           start_date: str,
                           end_date: str,
                           service_name: str = None,
                           account_id: str = None,
                           aws_profile: str = None,
                           region: str = 'us-east-1') -> pd.DataFrame:
        """
        Get daily cost trends for analysis and forecasting.
        
        Parameters:
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)
        - service_name: Specific AWS service to analyze (optional)
        - account_id: Specific AWS account ID (optional)
        - aws_profile: AWS profile to use
        - region: AWS region
        
        Returns:
        - DataFrame with daily cost trends
        """
        filters = []
        if service_name:
            filters.append(f"AND product_product_name = '{service_name}'")
        if account_id:
            filters.append(f"AND line_item_usage_account_id = '{account_id}'")
        
        filter_clause = " ".join(filters)
        
        query = f'''
        SELECT 
            DATE(line_item_usage_start_date) as usage_date,
            product_product_name,
            line_item_usage_account_id,
            SUM(line_item_unblended_cost) as daily_cost,
            SUM(line_item_usage_amount) as daily_usage,
            line_item_currency_code
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= '{start_date}'
        AND line_item_usage_start_date < '{end_date}'
        AND line_item_line_item_type != 'Tax'
        {filter_clause}
        GROUP BY DATE(line_item_usage_start_date), product_product_name, 
                 line_item_usage_account_id, line_item_currency_code
        ORDER BY usage_date DESC, daily_cost DESC
        '''
        
        return self.query_cur_with_athena(
            query=query,
            aws_profile=aws_profile,
            region=region
        )

    def get_resource_level_costs(self,
                               start_date: str,
                               end_date: str,
                               service_name: str,
                               top_n: int = 50,
                               aws_profile: str = None,
                               region: str = 'us-east-1') -> pd.DataFrame:
        """
        Get resource-level cost breakdown for detailed analysis.
        
        Parameters:
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)
        - service_name: AWS service name
        - top_n: Number of top resources to return
        - aws_profile: AWS profile to use
        - region: AWS region
        
        Returns:
        - DataFrame with resource-level costs
        """
        query = f'''
        SELECT 
            line_item_resource_id,
            line_item_usage_type,
            line_item_operation,
            product_region,
            SUM(line_item_unblended_cost) as total_cost,
            SUM(line_item_usage_amount) as total_usage,
            COUNT(*) as line_item_count
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= '{start_date}'
        AND line_item_usage_start_date < '{end_date}'
        AND product_product_name = '{service_name}'
        AND line_item_line_item_type != 'Tax'
        AND line_item_resource_id IS NOT NULL
        AND line_item_resource_id != ''
        GROUP BY line_item_resource_id, line_item_usage_type, 
                 line_item_operation, product_region
        ORDER BY total_cost DESC
        LIMIT {top_n}
        '''
        
        return self.query_cur_with_athena(
            query=query,
            aws_profile=aws_profile,
            region=region
        )

    def analyze_savings_opportunities(self,
                                   start_date: str,
                                   end_date: str,
                                   aws_profile: str = None,
                                   region: str = 'us-east-1') -> Dict[str, pd.DataFrame]:
        """
        Analyze potential savings opportunities from Reserved Instances and Savings Plans.
        
        Parameters:
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)
        - aws_profile: AWS profile to use
        - region: AWS region
        
        Returns:
        - Dictionary containing various savings analysis DataFrames
        """
        results = {}
        
        # On-Demand vs Reserved Instance usage
        ri_analysis_query = f'''
        SELECT 
            product_product_name,
            line_item_usage_type,
            product_region,
            CASE 
                WHEN reservation_reservation_a_r_n IS NOT NULL THEN 'Reserved Instance'
                WHEN savings_plan_savings_plan_a_r_n IS NOT NULL THEN 'Savings Plan'
                ELSE 'On-Demand'
            END as pricing_model,
            SUM(line_item_unblended_cost) as total_cost,
            SUM(line_item_usage_amount) as total_usage
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= '{start_date}'
        AND line_item_usage_start_date < '{end_date}'
        AND product_product_name IN ('Amazon Elastic Compute Cloud - Compute', 'Amazon Relational Database Service')
        GROUP BY product_product_name, line_item_usage_type, product_region, pricing_model
        ORDER BY total_cost DESC
        '''
        
        results['pricing_model_analysis'] = self.query_cur_with_athena(
            query=ri_analysis_query,
            aws_profile=aws_profile,
            region=region
        )
        
        # Underutilized resources
        underutilized_query = f'''
        SELECT 
            product_product_name,
            line_item_usage_type,
            product_region,
            AVG(line_item_usage_amount) as avg_usage,
            MAX(line_item_usage_amount) as max_usage,
            SUM(line_item_unblended_cost) as total_cost,
            COUNT(DISTINCT DATE(line_item_usage_start_date)) as days_active
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= '{start_date}'
        AND line_item_usage_start_date < '{end_date}'
        AND line_item_usage_amount > 0
        GROUP BY product_product_name, line_item_usage_type, product_region
        HAVING avg_usage < max_usage * 0.3  -- Resources using less than 30% of peak
        ORDER BY total_cost DESC
        '''
        
        results['underutilized_resources'] = self.query_cur_with_athena(
            query=underutilized_query,
            aws_profile=aws_profile,
            region=region
        )
        
        return results

    # =========================================================================
    # HELPER METHODS FOR AWS INTEGRATION
    # =========================================================================
    
    def _wait_for_athena_query(self, athena_client, query_execution_id: str, max_wait_time: int = 300) -> str:
        """
        Wait for Athena query to complete.
        
        Parameters:
        - athena_client: Boto3 Athena client
        - query_execution_id: Query execution ID
        - max_wait_time: Maximum time to wait in seconds
        
        Returns:
        - Final query status
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'FAILED':
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logging.error(f"Athena query failed: {error_msg}")
                return status
            
            time.sleep(5)  # Wait 5 seconds before checking again
        
        raise TimeoutError(f"Query {query_execution_id} did not complete within {max_wait_time} seconds")

    def _get_athena_results_as_dataframe(self, athena_client, s3_client, query_execution_id: str, output_location: str) -> pd.DataFrame:
        """
        Convert Athena query results to pandas DataFrame.
        
        Parameters:
        - athena_client: Boto3 Athena client
        - s3_client: Boto3 S3 client
        - query_execution_id: Query execution ID
        - output_location: S3 location of results
        
        Returns:
        - Results as pandas DataFrame
        """
        try:
            # Get query results metadata
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Extract column names
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            
            # Extract data rows
            rows = []
            for row in results['ResultSet']['Rows'][1:]:  # Skip header row
                row_data = [field.get('VarCharValue', '') for field in row['Data']]
                rows.append(row_data)
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # Convert numeric columns
            for col in df.columns:
                if 'cost' in col.lower() or 'usage' in col.lower() or 'amount' in col.lower():
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to convert Athena results to DataFrame: {e}")
            raise

    def _get_account_id(self, aws_profile: str = None, region: str = 'us-east-1') -> str:
        """
        Get current AWS account ID.
        
        Parameters:
        - aws_profile: AWS profile to use
        - region: AWS region
        
        Returns:
        - AWS account ID
        """
        try:
            session = boto3.Session(profile_name=aws_profile, region_name=region) if aws_profile else boto3.Session(region_name=region)
            sts_client = session.client('sts', region_name=region)
            return sts_client.get_caller_identity()['Account']
        except Exception as e:
            logging.warning(f"Could not determine account ID: {e}")
            return "unknown"

    # =========================================================================
    # EXISTING METHODS (preserved from original)
    # =========================================================================
    
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
        # # isinstance(data, dict):
        #     # Convert int64 to int for JSON serialization
        #     def convert_np_types(obj):
        #         if isinstance(obj, np.integer):
        #             return int(obj)
        #         elif isinstance(obj, np.floating):
        #             return float(obj)
        #         elif isinstance(obj, np.ndarray):
        #             return obj.tolist()
        #         raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        #     with open(file_path, 'w') as json_file:
        #         json.dump(data, json_file, default=convert_np_types)
        #     print(f"Dictionary saved as JSON to {file_path}")
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
        elif file_path.endswith('.json'):
            with open(file_path, 'w') as json_file:
                json.dump(data, json_file)
                # data = data
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
        # Create directory if it doesn't exist
        os.makedirs(data_folder, exist_ok=True)
        
        file_path = os.path.join(data_folder, file_name)
        self.save_file(data, file_path, use_polars, delete_local)
        print(f"File saved to {file_path}")

    # df = pd.DataFrame({'date': [1, 2], 'value': [3, 4]})
    # save_s3(df, 'jtrade1-dir', 'data', 'testtest.parquet',use_polars=False, delete_local=True)
    # save_local(df, 'data','aaaaaa.csv', delete_local=False)

    # polars tables are stored in dictionary
    def polars_run_query(self, tables: dict, 
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
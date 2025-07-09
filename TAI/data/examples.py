#!/usr/bin/env python3
"""
TAI DataMaster Examples

This file demonstrates comprehensive usage of the enhanced DataMaster class
including AWS Cost and Usage Reports, Athena integration, and data operations.

Run examples:
    python TAI/data/examples.py
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import polars as pl

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data.master import DataMaster, Redshift


def example_1_basic_data_operations():
    """Example 1: Basic data loading and saving operations"""
    print("=" * 50)
    print("Example 1: Basic Data Operations")
    print("=" * 50)
    
    dm = DataMaster()
    
    # Create sample data
    sample_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=10),
        'sales': [100, 150, 200, 180, 220, 190, 250, 300, 280, 320],
        'region': ['US', 'EU', 'APAC'] * 3 + ['US']
    })
    
    print("Created sample sales data:")
    print(sample_data.head())
    
    # Save locally
    dm.save_local(
        data=sample_data,
        data_folder='./temp_data',
        file_name='sales_sample.csv'
    )
    print("\n✓ Saved sample data locally")
    
    # Load back
    loaded_data = dm.load_local(
        data_folder='./temp_data',
        file_name='sales_sample.csv'
    )
    print("\n✓ Loaded data back:")
    print(loaded_data.head())
    
    # Clean up
    try:
        os.remove('./temp_data/sales_sample.csv')
        os.rmdir('./temp_data')
        print("\n✓ Cleaned up temporary files")
    except:
        pass


def example_2_s3_operations():
    """Example 2: S3 data operations (requires AWS configuration)"""
    print("\n" + "=" * 50)
    print("Example 2: S3 Operations")
    print("=" * 50)
    
    dm = DataMaster()
    
    print("Note: This example requires AWS credentials and S3 bucket access")
    print("Skipping actual S3 operations for demo purposes")
    
    # Example configuration (would work with real AWS setup)
    example_s3_config = {
        'bucket_name': 'your-data-bucket',
        's3_directory': 'analytics/',
        'file_name': 'processed_data.parquet',
        'aws_profile': 'default'
    }
    
    print(f"Example S3 configuration: {example_s3_config}")
    print("Use dm.load_s3() and dm.save_s3() with your actual bucket configuration")


def example_3_polars_sql_operations():
    """Example 3: SQL operations with Polars"""
    print("\n" + "=" * 50)
    print("Example 3: Polars SQL Operations")
    print("=" * 50)
    
    dm = DataMaster()
    
    # Create sample datasets
    sales_data = pl.DataFrame({
        'order_id': [1, 2, 3, 4, 5],
        'customer_id': [101, 102, 101, 103, 102],
        'amount': [250.00, 150.00, 300.00, 100.00, 400.00],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })
    
    customer_data = pl.DataFrame({
        'customer_id': [101, 102, 103],
        'name': ['Alice Johnson', 'Bob Smith', 'Carol Davis'],
        'segment': ['Premium', 'Standard', 'Basic']
    })
    
    tables = {
        'sales': sales_data,
        'customers': customer_data
    }
    
    print("Sales data:")
    print(sales_data)
    print("\nCustomer data:")
    print(customer_data)
    
    # Execute SQL query
    query = '''
    SELECT 
        c.name,
        c.segment,
        COUNT(s.order_id) as order_count,
        SUM(s.amount) as total_sales,
        AVG(s.amount) as avg_order_value
    FROM sales s
    JOIN customers c ON s.customer_id = c.customer_id
    GROUP BY c.name, c.segment
    ORDER BY total_sales DESC
    '''
    
    result = dm.polars_run_query(tables, query)
    print(f"\nSQL Query Result:")
    print(result)


def example_4_cost_analytics_setup():
    """Example 4: AWS Cost Analytics Setup (conceptual)"""
    print("\n" + "=" * 50)
    print("Example 4: AWS Cost Analytics Setup")
    print("=" * 50)
    
    dm = DataMaster()
    
    print("Note: This demonstrates the API structure for cost analytics")
    print("Actual execution requires AWS billing permissions and proper setup\n")
    
    # Example 1: Create CUR Data Export
    print("1. Creating CUR Data Export:")
    cur_config = {
        'export_name': 'company-cost-analysis',
        's3_bucket': 'your-billing-bucket',
        's3_prefix': 'cur-exports/',
        'export_type': 'CUR_2_0',
        'format_type': 'PARQUET',
        'aws_profile': 'billing-account'
    }
    print(f"   Configuration: {cur_config}")
    
    # Example 2: Athena Setup
    print("\n2. Setting up Athena Integration:")
    athena_config = {
        'database_name': 'cost_analytics',
        'table_name': 'billing_data',
        's3_bucket': 'your-billing-bucket',
        's3_prefix': 'cur-exports/',
        'aws_profile': 'billing-account'
    }
    print(f"   Configuration: {athena_config}")
    
    # Example 3: Cost Analysis Queries
    print("\n3. Example Cost Analysis Queries:")
    
    sample_queries = {
        'Service Costs': '''
        SELECT 
            product_product_name,
            SUM(line_item_unblended_cost) as total_cost
        FROM cost_analytics.billing_data
        WHERE line_item_usage_start_date >= '2024-01-01'
        GROUP BY product_product_name
        ORDER BY total_cost DESC
        ''',
        
        'Daily Trends': '''
        SELECT 
            DATE(line_item_usage_start_date) as usage_date,
            SUM(line_item_unblended_cost) as daily_cost
        FROM cost_analytics.billing_data
        WHERE line_item_usage_start_date >= '2024-01-01'
        GROUP BY DATE(line_item_usage_start_date)
        ORDER BY usage_date
        ''',
        
        'Resource Analysis': '''
        SELECT 
            line_item_resource_id,
            product_product_name,
            SUM(line_item_unblended_cost) as resource_cost
        FROM cost_analytics.billing_data
        WHERE line_item_resource_id IS NOT NULL
        AND line_item_usage_start_date >= '2024-01-01'
        GROUP BY line_item_resource_id, product_product_name
        ORDER BY resource_cost DESC
        LIMIT 20
        '''
    }
    
    for query_name, query in sample_queries.items():
        print(f"\n   {query_name}:")
        print(f"   {query.strip()}")


def example_5_mock_cost_analysis():
    """Example 5: Mock cost analysis with sample data"""
    print("\n" + "=" * 50)
    print("Example 5: Mock Cost Analysis")
    print("=" * 50)
    
    # Create mock billing data
    mock_billing_data = pd.DataFrame({
        'usage_date': pd.date_range('2024-01-01', periods=30),
        'service_name': ['EC2', 'S3', 'RDS', 'Lambda', 'CloudFront'] * 6,
        'account_id': ['123456789012', '123456789013'] * 15,
        'daily_cost': [50 + i*2 + (i%7)*10 for i in range(30)],
        'usage_amount': [100 + i*5 for i in range(30)]
    })
    
    print("Mock billing data (first 10 rows):")
    print(mock_billing_data.head(10))
    
    # Analysis 1: Cost by service
    service_costs = mock_billing_data.groupby('service_name').agg({
        'daily_cost': 'sum',
        'usage_amount': 'sum'
    }).sort_values('daily_cost', ascending=False)
    
    print("\nCost by Service:")
    print(service_costs)
    
    # Analysis 2: Daily cost trends
    daily_trends = mock_billing_data.groupby('usage_date')['daily_cost'].sum()
    
    print(f"\nDaily Cost Trends (last 7 days):")
    print(daily_trends.tail(7))
    
    # Analysis 3: Account comparison
    account_costs = mock_billing_data.groupby(['account_id', 'service_name'])['daily_cost'].sum().unstack(fill_value=0)
    
    print(f"\nCost by Account and Service:")
    print(account_costs)


def example_6_directory_management():
    """Example 6: Directory management operations"""
    print("\n" + "=" * 50)
    print("Example 6: Directory Management")
    print("=" * 50)
    
    dm = DataMaster()
    
    # Create data directory
    print("Creating data directory...")
    dm.create_dir(method='data', parent_dir='./temp_workspace')
    
    # Create calendar-based directory
    print("Creating calendar-based directory...")
    dm.create_dir(method='calendar', parent_dir='./temp_workspace')
    
    # Locate directories
    data_path = dm.dir_locator(method='data', parent_dir='./temp_workspace')
    calendar_path = dm.dir_locator(method='calendar', parent_dir='./temp_workspace')
    
    print(f"Data directory: {data_path}")
    print(f"Calendar directory: {calendar_path}")
    
    # List files (would show files if any existed)
    try:
        files = dm.list_files('./temp_workspace', from_s3=False)
        print(f"Files found: {files}")
    except:
        print("No files found (expected for new directories)")
    
    # Clean up
    try:
        import shutil
        shutil.rmtree('./temp_workspace')
        print("\n✓ Cleaned up workspace directories")
    except:
        pass


def example_7_redshift_operations():
    """Example 7: Redshift operations (conceptual)"""
    print("\n" + "=" * 50)
    print("Example 7: Redshift Operations")
    print("=" * 50)
    
    print("Note: This demonstrates Redshift API usage")
    print("Actual execution requires Redshift cluster and proper credentials\n")
    
    # Example Redshift configuration
    redshift_config = {
        'host': 'your-redshift-cluster.region.redshift.amazonaws.com',
        'database': 'analytics',
        'user': 'analyst',
        'port': 5439
    }
    
    print(f"Example Redshift configuration: {redshift_config}")
    
    # Example queries
    sample_redshift_queries = [
        "SELECT COUNT(*) FROM customer_orders",
        "SELECT customer_id, SUM(order_total) FROM orders GROUP BY customer_id LIMIT 10",
        "SELECT DATE_TRUNC('month', order_date), COUNT(*) FROM orders GROUP BY 1 ORDER BY 1"
    ]
    
    print("\nExample Redshift queries:")
    for i, query in enumerate(sample_redshift_queries, 1):
        print(f"{i}. {query}")
    
    # Cached query example
    print(f"\nCached query example:")
    print("dm.run_sql_cache('monthly_sales.csv', query, refresh=False)")


def example_8_multi_account_patterns():
    """Example 8: Multi-account usage patterns"""
    print("\n" + "=" * 50)
    print("Example 8: Multi-Account Patterns")
    print("=" * 50)
    
    print("Multi-account usage patterns for enterprise environments:\n")
    
    # Pattern 1: Segregated environments
    patterns = {
        'Development Environment': {
            'aws_profile': 'dev-account',
            'bucket': 'dev-data-bucket',
            'purpose': 'Development and testing'
        },
        
        'Production Environment': {
            'aws_profile': 'prod-account', 
            'bucket': 'prod-data-bucket',
            'purpose': 'Production workloads'
        },
        
        'Billing/Analytics': {
            'aws_profile': 'billing-account',
            'bucket': 'cost-analytics-bucket',
            'purpose': 'Cost analysis and billing data'
        },
        
        'Data Lake': {
            'aws_profile': 'data-account',
            'bucket': 'enterprise-data-lake',
            'purpose': 'Centralized data storage and analytics'
        }
    }
    
    for env_name, config in patterns.items():
        print(f"{env_name}:")
        print(f"  Profile: {config['aws_profile']}")
        print(f"  Bucket: {config['bucket']}")
        print(f"  Purpose: {config['purpose']}\n")
    
    # Example usage patterns
    print("Usage Examples:")
    print("1. Load production data: dm.load_s3(bucket='prod-data', aws_profile='prod-account')")
    print("2. Save to dev environment: dm.save_s3(data=df, bucket='dev-data', aws_profile='dev-account')")
    print("3. Analyze costs: dm.get_cost_by_service(aws_profile='billing-account')")
    print("4. Cross-account data movement: Load from prod, transform, save to analytics")


def main():
    """Run all examples"""
    print("TAI DataMaster Enhanced Examples")
    print("=" * 50)
    print("This script demonstrates the enhanced DataMaster functionality")
    print("including AWS cost analytics, data operations, and multi-account support.\n")
    
    try:
        example_1_basic_data_operations()
        example_2_s3_operations()
        example_3_polars_sql_operations()
        example_4_cost_analytics_setup()
        example_5_mock_cost_analysis()
        example_6_directory_management()
        example_7_redshift_operations()
        example_8_multi_account_patterns()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print("=" * 50)
        print("\nNext Steps:")
        print("1. Configure AWS credentials for your accounts")
        print("2. Set up S3 buckets for data storage")
        print("3. Enable AWS Cost and Usage Reports")
        print("4. Configure Redshift connection if needed")
        print("5. Customize examples for your specific use cases")
        
    except Exception as e:
        print(f"\nExample execution failed: {e}")
        print("This is expected if AWS credentials or dependencies are not configured")


if __name__ == "__main__":
    main() 
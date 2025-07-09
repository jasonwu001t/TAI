# TAI Data Module Documentation

## Overview

The TAI Data module provides comprehensive utilities for data management, AWS services integration, and cost analytics. The `DataMaster` class serves as the central hub for:

- **Data Storage & Retrieval**: Local and S3 operations with support for CSV, Parquet, and JSON formats
- **AWS Cost Analytics**: Cost and Usage Report (CUR) creation, management, and analysis
- **Amazon Athena Integration**: SQL querying of cost data and other AWS services
- **Database Connectivity**: Redshift integration with caching capabilities
- **Multi-Account Support**: Seamless operations across multiple AWS accounts

## Table of Contents

1. [Quick Start](#quick-start)
2. [AWS Cost and Usage Reports (CUR)](#aws-cost-and-usage-reports)
3. [Data Operations](#data-operations)
4. [Database Integration](#database-integration)
5. [Multi-Account Configuration](#multi-account-configuration)
6. [API Reference](#api-reference)
7. [Examples](#examples)
8. [Best Practices](#best-practices)

## Quick Start

### Basic Initialization

```python
from TAI.data.master import DataMaster

# Initialize DataMaster
dm = DataMaster()

# Example: Load data from S3
data = dm.load_s3(
    bucket_name='my-bucket',
    s3_directory='data/',
    file_name='my_data.csv'
)

# Example: Get cost breakdown by service
cost_analysis = dm.get_cost_by_service(
    start_date='2024-01-01',
    end_date='2024-01-31',
    aws_profile='prod-account'
)
```

## AWS Cost and Usage Reports

### Overview

TAI provides comprehensive support for AWS Cost and Usage Reports (CUR) 2.0 using the modern [AWS Data Exports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html) service. This enables:

- **Automated Cost Analysis**: Daily/monthly cost breakdowns
- **Resource-Level Insights**: Detailed usage and cost per resource
- **Savings Opportunities**: Reserved Instance and Savings Plan analysis
- **Multi-Account Reporting**: Consolidated billing analysis

### Setting Up CUR Data Exports

#### 1. Create Data Export

```python
# Create a CUR 2.0 data export
export_result = dm.create_cur_data_export(
    export_name='company-cur-export',
    s3_bucket='your-billing-bucket',
    s3_prefix='cur-exports/',
    export_type='CUR_2_0',  # Options: CUR_2_0, COST_OPTIMIZATION_RECOMMENDATIONS, FOCUS_1_0
    format_type='PARQUET',  # PARQUET or CSV
    time_granularity='DAILY',  # DAILY or MONTHLY
    aws_profile='billing-account'
)
```

#### 2. Setup Athena Integration

```python
# Configure Athena for querying CUR data
athena_setup = dm.setup_cur_athena_integration(
    database_name='cost_analytics',
    table_name='billing_data',
    s3_bucket='your-billing-bucket',
    s3_prefix='cur-exports/',
    aws_profile='billing-account'
)
```

### Cost Analysis Methods

#### Cost by Service

```python
# Get cost breakdown by AWS service
service_costs = dm.get_cost_by_service(
    start_date='2024-01-01',
    end_date='2024-01-31',
    account_ids=['123456789012', '123456789013'],  # Optional filter
    group_by_account=True,
    aws_profile='billing-account'
)
```

#### Daily Cost Trends

```python
# Analyze daily cost trends
daily_trends = dm.get_daily_cost_trend(
    start_date='2024-01-01',
    end_date='2024-01-31',
    service_name='Amazon Elastic Compute Cloud - Compute',  # Optional
    account_id='123456789012',  # Optional
    aws_profile='billing-account'
)
```

#### Resource-Level Analysis

```python
# Get detailed resource-level costs
resource_costs = dm.get_resource_level_costs(
    start_date='2024-01-01',
    end_date='2024-01-31',
    service_name='Amazon Elastic Compute Cloud - Compute',
    top_n=50,
    aws_profile='billing-account'
)
```

#### Savings Opportunities

```python
# Analyze savings opportunities
savings_analysis = dm.analyze_savings_opportunities(
    start_date='2024-01-01',
    end_date='2024-01-31',
    aws_profile='billing-account'
)

# Returns dictionary with:
# - pricing_model_analysis: On-Demand vs Reserved Instance usage
# - underutilized_resources: Resources with low utilization
```

### Custom Athena Queries

```python
# Execute custom SQL queries against CUR data
custom_query = '''
SELECT
    product_product_name,
    product_region,
    SUM(line_item_unblended_cost) as total_cost
FROM cost_analytics.billing_data
WHERE line_item_usage_start_date >= '2024-01-01'
AND line_item_usage_start_date < '2024-02-01'
GROUP BY product_product_name, product_region
ORDER BY total_cost DESC
LIMIT 20
'''

results = dm.query_cur_with_athena(
    query=custom_query,
    database_name='cost_analytics',
    aws_profile='billing-account'
)
```

## Data Operations

### File Operations

#### Loading Data

```python
# Load single file from S3
df = dm.load_s3(
    bucket_name='data-bucket',
    s3_directory='processed/',
    file_name='sales_data.parquet',
    use_polars=False  # Use pandas by default
)

# Load all files from S3 directory
all_data = dm.load_s3(
    bucket_name='data-bucket',
    s3_directory='raw/',
    load_all=True,
    selected_files=['file1.csv', 'file2.parquet']  # Optional filter
)

# Load from local directory
local_data = dm.load_local(
    data_folder='./data',
    file_name='analysis.csv'
)
```

#### Saving Data

```python
import pandas as pd

# Sample data
df = pd.DataFrame({
    'date': ['2024-01-01', '2024-01-02'],
    'value': [100, 200]
})

# Save to S3
dm.save_s3(
    data=df,
    bucket_name='data-bucket',
    s3_directory='processed/',
    file_name='results.parquet',
    use_polars=False,
    delete_local=True  # Clean up local temp file
)

# Save locally
dm.save_local(
    data=df,
    data_folder='./output',
    file_name='results.csv'
)
```

#### Directory Management

```python
# Create directories
dm.create_dir(
    method='data',  # or 'calendar' for date-based structure
    parent_dir='./workspace',
    is_s3=False
)

# Create S3 directories
dm.create_dir(
    method='calendar',  # Creates YYYY/MM/DD structure
    bucket_name='data-bucket',
    s3_directory='organized/',
    is_s3=True,
    aws_region='us-west-2'
)

# Locate directories
path = dm.dir_locator(
    method='data',
    parent_dir='./workspace'
)
```

### SQL Operations with Polars

```python
import polars as pl

# Load multiple datasets
tables = {
    'sales': pl.DataFrame({'id': [1, 2], 'amount': [100, 200]}),
    'customers': pl.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
}

# Execute SQL queries
result = dm.polars_run_query(
    tables=tables,
    query='''
    SELECT
        c.name,
        SUM(s.amount) as total_sales
    FROM sales s
    JOIN customers c ON s.id = c.id
    GROUP BY c.name
    '''
)
```

## Database Integration

### Redshift Operations

```python
from TAI.data.master import Redshift

# Initialize Redshift connection
redshift = Redshift(use_connection_pooling=True)

# Execute SQL queries
result_df = redshift.run_sql("""
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 100
""")

# Cached query execution
cached_result = dm.run_sql_cache(
    csv_path='cached_results.csv',
    query="SELECT * FROM expensive_query_table",
    refresh=False  # Use cache if available
)
```

## Multi-Account Configuration

### AWS Profile Setup

```python
# Use different AWS accounts for different operations
dm.get_cost_by_service(
    start_date='2024-01-01',
    end_date='2024-01-31',
    aws_profile='billing-account'  # Dedicated billing account
)

dm.load_s3(
    bucket_name='prod-data',
    s3_directory='analytics/',
    aws_profile='prod-account'  # Production data account
)

dm.save_s3(
    data=results,
    bucket_name='dev-storage',
    s3_directory='experiments/',
    aws_profile='dev-account'  # Development account
)
```

## API Reference

### Core Classes

#### DataMaster

Main class providing all data management and cost analytics functionality.

**Methods:**

**AWS Cost Analytics:**

- `create_cur_data_export()` - Create AWS Data Export for CUR 2.0
- `setup_cur_athena_integration()` - Configure Athena for CUR querying
- `query_cur_with_athena()` - Execute SQL queries against CUR data
- `get_cost_by_service()` - Service-level cost breakdown
- `get_daily_cost_trend()` - Daily cost trend analysis
- `get_resource_level_costs()` - Resource-level cost details
- `analyze_savings_opportunities()` - RI and Savings Plan analysis

**Data Exports Management:**

- `list_data_exports()` - List all existing AWS Data Exports
- `get_export_details()` - Get detailed information about specific export
- `find_export_by_name()` - Find export by name and return S3 location
- `create_focus_export_from_existing()` - Create FOCUS export using existing S3 bucket
- `query_latest_month_data()` - Query latest month data from any export

**Data Operations:**

- `load_s3()`, `load_local()` - Data loading from various sources
- `save_s3()`, `save_local()` - Data saving with format conversion
- `create_dir()`, `dir_locator()` - Directory management
- `list_files()` - File discovery and listing
- `polars_run_query()` - SQL operations on DataFrames

**Database:**

- `run_sql_cache()` - Cached SQL execution

#### Redshift

Dedicated class for Amazon Redshift operations.

**Methods:**

- `run_sql()` - Execute SQL queries with result DataFrame
- `get_connection()` - Access underlying connection

### Supported Data Formats

- **CSV**: Comma-separated values with automatic type inference
- **Parquet**: Columnar format with compression and fast querying
- **JSON**: Nested data structures with custom serialization

### Multi-Account AWS Integration

All AWS operations support multi-account access through:

- **AWS Profiles**: `aws_profile` parameter in all methods
- **Runtime Configuration**: Dynamic account switching
- **Session Caching**: Automatic performance optimization

## Examples

### Complete Cost Analysis Pipeline

```python
from TAI.data.master import DataMaster
import pandas as pd

# Initialize
dm = DataMaster()

# 1. Setup CUR data export (one-time setup)
export_config = dm.create_cur_data_export(
    export_name='monthly-cost-analysis',
    s3_bucket='company-billing-data',
    s3_prefix='cur-exports/',
    export_type='CUR_2_0',
    aws_profile='billing-account'
)

# 2. Configure Athena (one-time setup)
athena_config = dm.setup_cur_athena_integration(
    database_name='cost_analytics',
    table_name='billing_data',
    s3_bucket='company-billing-data',
    s3_prefix='cur-exports/',
    aws_profile='billing-account'
)

# 3. Analyze costs
service_costs = dm.get_cost_by_service(
    start_date='2024-01-01',
    end_date='2024-01-31',
    group_by_account=True,
    aws_profile='billing-account'
)

# 4. Get daily trends
daily_costs = dm.get_daily_cost_trend(
    start_date='2024-01-01',
    end_date='2024-01-31',
    aws_profile='billing-account'
)

# 5. Analyze savings opportunities
savings = dm.analyze_savings_opportunities(
    start_date='2024-01-01',
    end_date='2024-01-31',
    aws_profile='billing-account'
)

# 6. Save results for reporting
dm.save_s3(
    data=service_costs,
    bucket_name='analytics-reports',
    s3_directory='cost-analysis/',
    file_name='monthly-service-costs.parquet',
    aws_profile='analytics-account'
)
```

### Data Exports Management Pipeline

```python
# Complete Data Exports management workflow
dm = DataMaster()

# 1. List existing data exports
exports = dm.list_data_exports(aws_profile='billing-account')
print(f"Found {len(exports)} data exports")

# 2. Find specific export by name
cur2_export = dm.find_export_by_name('cur2', aws_profile='billing-account')
if cur2_export:
    print(f"cur2 export S3 location: {cur2_export['s3_full_location']}")

    # 3. Create new FOCUS export using same S3 bucket
    focus_export = dm.create_focus_export_from_existing(
        source_export_name='cur2',
        new_export_name='focus-cost-report',
        aws_profile='billing-account'
    )
    print(f"Created FOCUS export: {focus_export['ExportName']}")

# 4. Query latest month data from cur2
latest_data = dm.query_latest_month_data(
    export_name='cur2',
    database_name='cur_database',
    limit=10,
    aws_profile='billing-account'
)
print(f"Retrieved {len(latest_data)} latest month records")

# 5. Batch analysis of all exports
for export in exports:
    print(f"Export: {export['ExportName']}")
    print(f"  Type: {export['ExportType']}")
    print(f"  S3: s3://{export['S3Bucket']}/{export['S3Prefix']}")
```

### Multi-Source Data Pipeline

```python
# Load data from multiple sources
sales_data = dm.load_s3(
    bucket_name='sales-data',
    s3_directory='2024/',
    load_all=True,
    aws_profile='sales-account'
)

customer_data = dm.load_local(
    data_folder='./reference',
    file_name='customers.csv'
)

# Combine and analyze using SQL
combined_analysis = dm.polars_run_query(
    tables={
        'sales': sales_data['sales_2024.parquet'],
        'customers': customer_data
    },
    query='''
    SELECT
        c.segment,
        SUM(s.revenue) as total_revenue,
        COUNT(DISTINCT s.customer_id) as customer_count
    FROM sales s
    JOIN customers c ON s.customer_id = c.id
    GROUP BY c.segment
    ORDER BY total_revenue DESC
    '''
)

# Save results
dm.save_s3(
    data=combined_analysis,
    bucket_name='analytics-output',
    s3_directory='insights/',
    file_name='customer-segment-analysis.parquet',
    aws_profile='analytics-account'
)
```

## Best Practices

### Cost Analytics

1. **Regular Monitoring**: Set up daily cost trend analysis to catch unexpected spikes
2. **Account Segregation**: Use dedicated billing accounts for cost data access
3. **Resource Tagging**: Ensure proper resource tagging for detailed cost allocation
4. **Automated Alerts**: Implement cost threshold alerts based on analysis results

### Data Management

1. **Format Selection**: Use Parquet for analytical workloads, CSV for compatibility
2. **Compression**: Enable compression for S3 storage cost optimization
3. **Partitioning**: Organize data by date/account for efficient querying
4. **Caching**: Use SQL caching for expensive, repetitive queries

### Security & Access

1. **Least Privilege**: Use minimal required permissions for each AWS profile
2. **Credential Rotation**: Regularly rotate AWS access keys
3. **Data Encryption**: Enable S3 encryption for sensitive data
4. **Network Security**: Use VPC endpoints for private AWS service access

### Performance Optimization

1. **Batch Operations**: Process multiple files together when possible
2. **Parallel Execution**: Leverage multi-account setup for parallel processing
3. **Query Optimization**: Use appropriate WHERE clauses and LIMIT statements
4. **Resource Monitoring**: Monitor Athena query performance and costs

## Troubleshooting

### Common Issues

**Authentication Errors:**

```python
# Verify AWS profile configuration
from TAI.utils import AWSConfigManager
manager = AWSConfigManager()
profiles = manager.list_available_profiles()
is_valid = manager.validate_credentials(profile='your-profile')
```

**Athena Query Failures:**

- Check S3 permissions for query results location
- Verify table schema matches actual data structure
- Ensure proper partitioning for large datasets

**Data Loading Issues:**

- Confirm S3 bucket permissions and region settings
- Validate file formats and encoding
- Check network connectivity for large file transfers

**Cost Data Discrepancies:**

- Allow 24-48 hours for CUR data availability
- Verify timezone settings in queries
- Check for tax line items in cost calculations

### Support and Documentation

- **AWS CUR Documentation**: [Data Exports User Guide](https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html)
- **TAI Configuration**: See main README.md for authentication setup
- **Performance Tuning**: Contact your AWS solutions architect for optimization

---

_For additional support or feature requests, please refer to the main TAI documentation or submit an issue._

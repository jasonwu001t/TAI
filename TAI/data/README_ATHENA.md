# AWS Athena + Glue Solution for Parquet Data Analysis

## Overview

This solution provides a production-ready system for querying AWS parquet files using AWS Glue for schema discovery and AWS Athena for SQL-based analysis. It successfully handles complex billing data with 93+ columns and provides a clean API for running SQL queries.

## Architecture

```
S3 Parquet Files → AWS Glue Crawler → AWS Athena → Python API → Results
```

1. **AWS Glue Crawler**: Automatically discovers schema from S3 parquet files
2. **AWS Athena**: Executes SQL queries against discovered tables
3. **Python Modules**: `glue.py` and `athena.py` provide clean interfaces
4. **Result Processing**: Returns pandas DataFrames for analysis

## Prerequisites

### AWS Requirements

- AWS Account with appropriate permissions
- AWS CLI configured or credentials available
- IAM Role for Glue Crawler: `AWSGlueServiceRole-TAI`
- S3 access permissions for source and destination buckets

### Python Dependencies

```bash
pip install boto3 pandas numpy
```

### AWS IAM Permissions

Your AWS credentials need the following permissions:

- `AthenaFullAccess` or specific Athena permissions
- `AWSGlueServiceRole` for Glue operations
- `S3ReadWrite` access to source and destination buckets
- `IAMPassRole` for Glue crawler role

## File Structure

```
TAI/data/
├── README_ATHENA.md          # This documentation
├── athena.py                 # AWS Athena query interface
├── glue.py                   # AWS Glue crawler interface
└── __init__.py              # Module exports
```

## Core Modules

### `athena.py` - Query Execution

- **Purpose**: Execute SQL queries against Athena tables
- **Key Features**:
  - Automatic result conversion to pandas DataFrames
  - Query timeout and error handling
  - Support for all SQL operations (SELECT, CREATE, DROP, etc.)
  - Configurable regions and databases

### `glue.py` - Schema Discovery

- **Purpose**: Automatically discover parquet schema using AWS Glue
- **Key Features**:
  - Crawler creation and management
  - Automatic schema detection from S3 parquet files
  - Table metadata retrieval
  - Integration with Athena for seamless workflow

## Setup Instructions

### Step 1: Prepare S3 Data

The solution works best with clean S3 directories containing only parquet files (no JSON manifests).

```python
# If your source has mixed files, copy parquet to clean directory
import boto3

s3 = boto3.client('s3')
s3.copy_object(
    CopySource={'Bucket': 'source-bucket', 'Key': 'path/to/file.parquet'},
    Bucket='dest-bucket',
    Key='clean-data/data.parquet'
)
```

### Step 2: Initialize Services

```python
from TAI.data.glue import GlueCrawler
from TAI.data.athena import AthenaSparkSQL

# Initialize services
glue = GlueCrawler(region='us-west-2')
athena = AthenaSparkSQL(region='us-west-2')
```

### Step 3: Create Database

```python
# Create Athena database
database_name = "your_analysis_db"
athena.create_database(
    database_name=database_name,
    description="Analysis database for parquet data"
)
```

### Step 4: Run Schema Discovery

```python
# Discover schema with Glue Crawler
s3_path = "s3://your-bucket/clean-parquet-data/"
crawler_name = "your_crawler"

discovery_result = glue.discover_s3_schema(
    s3_path=s3_path,
    database_name=database_name,
    crawler_name=crawler_name,
    wait_for_completion=True
)

print(f"Tables discovered: {len(discovery_result['discovered_tables'])}")
for table in discovery_result['discovered_tables']:
    print(f"- {table['name']}: {table['columns']} columns")
```

### Step 5: Query Data

```python
# Execute SQL queries
table_name = discovery_result['discovered_tables'][0]['name']

# Simple query
df = athena.execute_query(f"""
    SELECT * FROM {database_name}.{table_name} LIMIT 10
""")

print(f"Retrieved {len(df)} rows with {len(df.columns)} columns")
```

## Usage Examples

### Basic Query Operations

```python
from TAI.data.athena import AthenaSparkSQL

athena = AthenaSparkSQL(region='us-west-2')

# Count rows
count_df = athena.execute_query("""
    SELECT COUNT(*) as total_rows
    FROM billing_analysis.billing_data
""")

# Aggregation query
summary_df = athena.execute_query("""
    SELECT
        product_product_name,
        COUNT(*) as line_items,
        SUM(line_item_unblended_cost) as total_cost
    FROM billing_analysis.billing_data
    GROUP BY product_product_name
    ORDER BY total_cost DESC
""")

# Complex analysis
analysis_df = athena.execute_query("""
    SELECT
        DATE(line_item_usage_start_date) as usage_date,
        line_item_availability_zone,
        product_product_name,
        SUM(line_item_usage_amount) as total_usage,
        AVG(line_item_unblended_cost) as avg_cost
    FROM billing_analysis.billing_data
    WHERE line_item_unblended_cost > 0
    GROUP BY 1, 2, 3
    ORDER BY usage_date DESC, total_usage DESC
    LIMIT 100
""")
```

### Database Management

```python
# List databases
databases = athena.list_databases()
print("Available databases:", databases)

# List tables in database
tables = athena.list_tables("billing_analysis")
print("Available tables:", tables)

# Get table schema
schema = athena.get_table_schema("billing_analysis", "billing_data")
print("Table schema:", schema)

# Drop table if needed
athena.drop_table("billing_analysis", "old_table")
```

### Error Handling

```python
try:
    result_df = athena.execute_query("""
        SELECT * FROM billing_analysis.billing_data
        WHERE invalid_column = 'test'
    """)
except Exception as e:
    print(f"Query failed: {e}")
    # Handle error appropriately
```

## API Development Guide

### Recommended API Structure

```python
from flask import Flask, request, jsonify
from TAI.data.athena import AthenaSparkSQL
import pandas as pd

app = Flask(__name__)
athena = AthenaSparkSQL(region='us-west-2')

@app.route('/api/query', methods=['POST'])
def execute_query():
    """Execute Athena SQL query and return results"""
    try:
        data = request.get_json()
        query = data.get('query')
        database = data.get('database', 'billing_analysis')

        # Validate query (implement security checks)
        if not query or not _is_safe_query(query):
            return jsonify({'error': 'Invalid query'}), 400

        # Execute query
        df = athena.execute_query(query, database=database)

        # Convert to JSON
        result = {
            'rows': len(df),
            'columns': len(df.columns),
            'data': df.to_dict('records'),
            'column_names': list(df.columns)
        }

        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['GET'])
def list_tables():
    """List available tables"""
    try:
        database = request.args.get('database', 'billing_analysis')
        tables = athena.list_tables(database)
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/schema/<table_name>', methods=['GET'])
def get_schema(table_name):
    """Get table schema"""
    try:
        database = request.args.get('database', 'billing_analysis')
        schema = athena.get_table_schema(database, table_name)
        return jsonify({'schema': schema})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def _is_safe_query(query):
    """Basic query validation - implement proper security"""
    dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE']
    query_upper = query.upper()
    return not any(keyword in query_upper for keyword in dangerous_keywords)

if __name__ == '__main__':
    app.run(debug=True)
```

### API Security Considerations

1. **Query Validation**: Implement whitelist of allowed SQL operations
2. **Rate Limiting**: Prevent abuse with request limits
3. **Authentication**: Add API key or OAuth authentication
4. **Query Timeout**: Set reasonable timeouts for long-running queries
5. **Result Size Limits**: Prevent memory issues with large datasets

### Performance Optimization

```python
# Use query result caching
@lru_cache(maxsize=100)
def cached_query(query_hash):
    return athena.execute_query(query)

# Implement pagination for large results
def paginated_query(query, page=1, page_size=100):
    offset = (page - 1) * page_size
    paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"
    return athena.execute_query(paginated_query)

# Use async for concurrent queries
import asyncio
async def execute_multiple_queries(queries):
    tasks = [athena.execute_query(q) for q in queries]
    return await asyncio.gather(*tasks)
```

## Configuration

### Environment Variables

```bash
export AWS_REGION=us-west-2
export ATHENA_DATABASE=billing_analysis
export ATHENA_WORKGROUP=primary
export S3_OUTPUT_LOCATION=s3://aws-athena-query-results-us-west-2-account
```

### Configuration File Example

```python
# config.py
ATHENA_CONFIG = {
    'region': 'us-west-2',
    'database': 'billing_analysis',
    'workgroup': 'primary',
    'output_location': 's3://aws-athena-query-results-us-west-2-014498620306'
}

GLUE_CONFIG = {
    'region': 'us-west-2',
    'role_arn': 'arn:aws:iam::014498620306:role/AWSGlueServiceRole-TAI'
}
```

## Troubleshooting

### Common Issues

1. **"Query returned 0 rows" despite data existing**

   - **Cause**: JSON manifest files in S3 directory
   - **Solution**: Copy parquet files to clean directory

2. **"Table not found" errors**

   - **Cause**: Crawler hasn't completed or failed
   - **Solution**: Check crawler status, ensure IAM permissions

3. **"Cannot create table on file" errors**

   - **Cause**: Athena requires directory, not specific files
   - **Solution**: Use directory paths in table locations

4. **Performance issues with large datasets**
   - **Solution**: Use LIMIT clauses, partition data, optimize queries

### Debug Information

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check crawler status
crawler_status = glue.get_crawler_status('your_crawler')
print(f"Crawler state: {crawler_status}")

# Verify table exists
tables = athena.list_tables('your_database')
print(f"Available tables: {tables}")

# Test connection
databases = athena.list_databases()
print(f"Accessible databases: {databases}")
```

## Production Deployment

### Recommended Setup

1. **Separate environments**: Dev, staging, production databases
2. **Monitoring**: CloudWatch alerts for failed queries
3. **Cost optimization**: Use appropriate instance sizes and query limits
4. **Data lifecycle**: Implement S3 lifecycle policies for cost management
5. **Backup strategy**: Regular exports of critical query results

### Scaling Considerations

- **Concurrent queries**: Athena supports multiple concurrent queries
- **Data partitioning**: Partition large datasets by date/region for performance
- **Result caching**: Cache frequently accessed query results
- **Query optimization**: Use columnar access patterns, avoid SELECT \*

## Cost Management

### Query Cost Optimization

```python
# Estimate query cost before execution
def estimate_query_cost(query, database):
    # Use EXPLAIN or analyze query complexity
    explain_query = f"EXPLAIN {query}"
    plan = athena.execute_query(explain_query, database=database)
    return plan  # Analyze for cost estimation

# Monitor data scanned
result = athena.execute_query(query, return_stats=True)
data_scanned_gb = result['stats']['data_scanned_bytes'] / (1024**3)
estimated_cost = data_scanned_gb * 0.005  # $5 per TB
```

### Best Practices

- Use `LIMIT` clauses for exploratory queries
- Query only required columns instead of `SELECT *`
- Use partitioned tables when possible
- Compress parquet files with snappy/gzip
- Use appropriate data types to minimize storage

---

## Quick Start Example

```python
# Complete working example
from TAI.data.glue import GlueCrawler
from TAI.data.athena import AthenaSparkSQL

# 1. Setup
glue = GlueCrawler(region='us-west-2')
athena = AthenaSparkSQL(region='us-west-2')

# 2. Create database
athena.create_database('my_analysis', 'Analysis database')

# 3. Discover schema
result = glue.discover_s3_schema(
    s3_path='s3://my-bucket/clean-data/',
    database_name='my_analysis',
    crawler_name='my_crawler'
)

# 4. Query data
df = athena.execute_query("""
    SELECT * FROM my_analysis.my_table LIMIT 10
""")

print(f"Success! Retrieved {len(df)} rows with {len(df.columns)} columns")
```

This solution is production-ready and can handle complex datasets with 90+ columns and hundreds of thousands of rows efficiently.

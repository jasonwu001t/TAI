#!/usr/bin/env python3
"""
S3 Parquet to Athena Table & Custom Queries

Discover parquet files in S3, create Athena table once, then run custom queries.

S3 Path: s3://billing-data-exports-cur/legacy-cur/legacy-cur/20250601-20250701/20250603T230015Z/
Region: us-west-2
"""

import boto3
from datetime import datetime
from TAI.data.athena import AthenaSparkSQL

def list_s3_files(bucket_name, prefix, region='us-west-2'):
    """
    List all files in the specified S3 bucket and prefix
    """
    print(f"\n📂 Listing files in S3 bucket: {bucket_name}")
    print(f"📁 Prefix: {prefix}")
    print("=" * 60)
    
    try:
        s3_client = boto3.client('s3', region_name=region)
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        parquet_files = []
        total_size = 0
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    size_bytes = obj['Size']
                    
                    if key.endswith('.parquet'):
                        parquet_files.append(key)
                        total_size += size_bytes
        
        print(f"✅ Found {len(parquet_files)} parquet files")
        print(f"📊 Total size: {total_size / (1024 * 1024 * 1024):.2f} GB")
        
        return len(parquet_files) > 0, parquet_files
        
    except Exception as e:
        print(f"❌ Error listing S3 files: {e}")
        return False, []

def create_athena_table_from_parquet(athena, database_name, table_name, s3_location):
    """
    Create Athena external table from S3 parquet files with automatic schema detection
    """
    print(f"\n🔧 Creating Athena table: {database_name}.{table_name}")
    
    try:
        # Check if table already exists
        try:
            existing_tables = athena.execute_query(
                query=f"SHOW TABLES IN {database_name}",
                database=database_name
            )
            
            if table_name in existing_tables.values.flatten():
                print(f"✅ Table {database_name}.{table_name} already exists!")
                return True
        except:
            pass  # Database might not exist yet
        
        # Create database if it doesn't exist
        try:
            athena.create_database(database_name, description="Billing data analysis")
            print(f"✅ Database {database_name} ready")
        except Exception as e:
            print(f"ℹ️  Database exists: {e}")
        
        # Method 1: Try CREATE TABLE AS with automatic schema detection
        print("🔧 Attempting automatic schema detection...")
        
        create_table_query = f"""
        CREATE TABLE {database_name}.{table_name}
        WITH (
            external_location = '{s3_location}',
            format = 'PARQUET'
        ) AS
        SELECT * FROM "{s3_location}"
        WHERE 1=0
        """
        
        try:
            athena.execute_query(
                query=create_table_query,
                database=database_name,
                return_as_dataframe=False
            )
            print(f"✅ Table created with automatic schema detection!")
            return True
            
        except Exception as e:
            print(f"⚠️  Automatic schema detection failed: {e}")
            
            # Method 2: Create external table with manual schema from sample
            print("🔧 Trying manual schema detection from sample...")
            
            try:
                # Sample the data to get schema
                sample_query = f'SELECT * FROM "{s3_location}" LIMIT 1'
                sample_result = athena.execute_query(query=sample_query, database=database_name)
                
                # Generate column definitions
                column_definitions = []
                for col in sample_result.columns:
                    dtype = str(sample_result[col].dtype)
                    
                    if 'object' in dtype or 'string' in dtype:
                        sql_type = 'string'
                    elif 'int' in dtype:
                        sql_type = 'bigint'
                    elif 'float' in dtype:
                        sql_type = 'double'
                    elif 'bool' in dtype:
                        sql_type = 'boolean'
                    elif 'datetime' in dtype:
                        sql_type = 'timestamp'
                    else:
                        sql_type = 'string'
                    
                    column_definitions.append(f"  `{col}` {sql_type}")
                
                columns_sql = ",\n".join(column_definitions)
                
                manual_create_query = f"""
                CREATE EXTERNAL TABLE {database_name}.{table_name} (
{columns_sql}
                )
                STORED AS PARQUET
                LOCATION '{s3_location}'
                TBLPROPERTIES ('has_encrypted_data'='false')
                """
                
                athena.execute_query(
                    query=manual_create_query,
                    database=database_name,
                    return_as_dataframe=False
                )
                
                print(f"✅ Table created with manual schema ({len(column_definitions)} columns)!")
                return True
                
            except Exception as e2:
                print(f"❌ Manual schema detection also failed: {e2}")
                return False
    
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        return False

def run_custom_queries(athena, database_name, table_name):
    """
    Run a set of custom queries against the Athena table
    """
    print(f"\n📊 Running custom queries against {database_name}.{table_name}")
    print("=" * 60)
    
    # Define your custom queries here
    queries = {
        "Row Count": f"SELECT COUNT(*) as total_rows FROM {database_name}.{table_name}",
        
        "Sample Data": f"SELECT * FROM {database_name}.{table_name} LIMIT 5",
        
        "Table Schema": f"DESCRIBE {database_name}.{table_name}",
        
        "Data Date Range": f"""
        SELECT 
            MIN(bill_billing_period_start_date) as earliest_date,
            MAX(bill_billing_period_end_date) as latest_date
        FROM {database_name}.{table_name}
        WHERE bill_billing_period_start_date IS NOT NULL
        """,
        
        "Top Services by Cost": f"""
        SELECT 
            product_servicecode,
            SUM(CAST(line_item_unblended_cost AS DOUBLE)) as total_cost,
            COUNT(*) as line_items
        FROM {database_name}.{table_name}
        WHERE line_item_unblended_cost IS NOT NULL
        GROUP BY product_servicecode
        ORDER BY total_cost DESC
        LIMIT 10
        """,
        
        "Cost by Account": f"""
        SELECT 
            line_item_usage_account_id,
            line_item_usage_account_name,
            SUM(CAST(line_item_unblended_cost AS DOUBLE)) as total_cost
        FROM {database_name}.{table_name}
        WHERE line_item_unblended_cost IS NOT NULL
        GROUP BY line_item_usage_account_id, line_item_usage_account_name
        ORDER BY total_cost DESC
        LIMIT 10
        """
    }
    
    # Execute each query
    for query_name, query_sql in queries.items():
        print(f"\n🔍 {query_name}")
        print("-" * 40)
        
        try:
            result = athena.execute_query(query=query_sql, database=database_name)
            
            if len(result) > 0:
                print(f"✅ Results ({len(result)} rows):")
                print(result)
            else:
                print("ℹ️  No results returned")
                
        except Exception as e:
            print(f"❌ Query failed: {e}")
            print(f"💡 Query: {query_sql[:100]}...")

def run_adhoc_query(athena, database_name, table_name, custom_query):
    """
    Run a single ad-hoc query against the table
    """
    print(f"\n🔍 Running ad-hoc query:")
    print(f"Query: {custom_query}")
    print("-" * 60)
    
    try:
        result = athena.execute_query(query=custom_query, database=database_name)
        
        if len(result) > 0:
            print(f"✅ Results ({len(result)} rows × {len(result.columns)} columns):")
            print(result)
        else:
            print("ℹ️  No results returned")
            
        return result
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

def main():
    """
    Main function: Create table once, then run queries
    """
    print("📊 Athena Parquet Table & Custom Queries")
    print("=" * 60)
    
    # Configuration
    S3_BUCKET = "billing-data-exports-cur"
    S3_PREFIX = "legacy-cur/legacy-cur/20250601-20250701/20250603T230015Z/"
    AWS_REGION = 'us-west-2'
    DATABASE_NAME = "billing_data"
    TABLE_NAME = "legacy_cur_data"
    
    # Clean up the S3 prefix
    clean_prefix = S3_PREFIX.strip('/')
    s3_location = f"s3://{S3_BUCKET}/{clean_prefix}/"
    
    print(f"🎯 S3 Location: {s3_location}")
    print(f"🗄️  Database: {DATABASE_NAME}")
    print(f"📊 Table: {TABLE_NAME}")
    
    # Initialize Athena
    athena = AthenaSparkSQL(region=AWS_REGION)
    
    # Step 1: Verify S3 files exist
    files_found, parquet_files = list_s3_files(S3_BUCKET, clean_prefix, AWS_REGION)
    
    if not files_found:
        print("❌ No parquet files found in S3 location")
        return
    
    # Step 2: Create Athena table (once)
    table_created = create_athena_table_from_parquet(
        athena, DATABASE_NAME, TABLE_NAME, s3_location
    )
    
    if not table_created:
        print("❌ Cannot proceed without creating table")
        return
    
    # Step 3: Run predefined queries
    run_custom_queries(athena, DATABASE_NAME, TABLE_NAME)
    
    # Step 4: Example of running ad-hoc queries
    print(f"\n" + "="*60)
    print("💡 Example: Running ad-hoc query")
    
    # Example custom query - modify this as needed
    example_query = f"""
    SELECT 
        product_servicecode,
        line_item_usage_type,
        SUM(CAST(line_item_usage_amount AS DOUBLE)) as total_usage,
        SUM(CAST(line_item_unblended_cost AS DOUBLE)) as total_cost
    FROM {DATABASE_NAME}.{TABLE_NAME}
    WHERE product_servicecode = 'AmazonEC2'
    GROUP BY product_servicecode, line_item_usage_type
    ORDER BY total_cost DESC
    LIMIT 15
    """
    
    run_adhoc_query(athena, DATABASE_NAME, TABLE_NAME, example_query)
    
    print(f"\n" + "="*60)
    print("✅ Analysis completed!")
    print(f"🔧 Your table: {DATABASE_NAME}.{TABLE_NAME}")
    print(f"📂 S3 location: {s3_location}")
    print("💡 Modify the queries in the script to analyze your data differently")
    print("💡 Use run_adhoc_query() function for custom analysis")

if __name__ == "__main__":
    main() 
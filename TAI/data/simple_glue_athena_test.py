#!/usr/bin/env python3
"""
Simple Glue + Athena Test
Demonstrates querying parquet files from S3 using our modules.
Automatically handles multiple parquet files by combining them into a single table.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data import GlueCrawler, AthenaSparkSQL
import time

def simple_glue_athena_test():
    """
    Simple test demonstrating Glue + Athena workflow for parquet files.
    When multiple parquet files exist, Glue automatically combines them into one table.
    """
    
    # Configuration - Update these for your use case
    S3_PATH = "s3://billing-data-exports-cur/clean-parquet/billing-data/"  # Directory with parquet files
    REGION = "us-west-2"
    DATABASE = "simple_test_db"
    CRAWLER_NAME = "simple_test_crawler"
    
    print("=== Simple Glue + Athena Test ===")
    print(f"S3 Path: {S3_PATH}")
    print(f"Region: {REGION}")
    print(f"Database: {DATABASE}")
    print()
    
    try:
        # Step 1: Initialize services
        print("1. Initializing Glue and Athena...")
        glue = GlueCrawler(region=REGION)
        athena = AthenaSparkSQL(region=REGION)
        print("✅ Services initialized")
        
        # Step 2: Create database
        print(f"\n2. Creating database '{DATABASE}'...")
        athena.create_database(
            database_name=DATABASE,
            description="Simple test database for parquet analysis"
        )
        print("✅ Database created")
        
        # Step 3: Clean up any existing crawler
        print(f"\n3. Cleaning up existing crawler...")
        try:
            glue.delete_crawler(CRAWLER_NAME)
            print("✅ Old crawler deleted")
            time.sleep(5)  # Wait for cleanup
        except Exception as e:
            print(f"ℹ️  No existing crawler: {e}")
        
        # Step 4: Discover schema with Glue Crawler
        print(f"\n4. Running Glue Crawler to discover parquet schema...")
        print("   📝 Note: Glue automatically combines multiple parquet files into one table")
        
        discovery_result = glue.discover_s3_schema(
            s3_path=S3_PATH,
            database_name=DATABASE,
            crawler_name=CRAWLER_NAME,
            wait_for_completion=True
        )
        
        print("✅ Schema discovery completed!")
        print(f"   Tables found: {len(discovery_result['discovered_tables'])}")
        
        # Step 5: Show discovered tables
        if discovery_result['discovered_tables']:
            for i, table in enumerate(discovery_result['discovered_tables']):
                print(f"   Table {i+1}: {table['name']} ({table['columns']} columns)")
                print(f"      Location: {table['location']}")
                print(f"      Format: {table['input_format']}")
        
        # Step 6: Query the data
        if discovery_result['discovered_tables']:
            table_name = discovery_result['discovered_tables'][0]['name']
            print(f"\n5. Querying data from table: {table_name}")
            
            # Test 1: Count total rows (shows all parquet files combined)
            print(f"\n   Test 1: Count total rows...")
            count_query = f"SELECT COUNT(*) as total_rows FROM {DATABASE}.{table_name}"
            count_result = athena.execute_query(count_query, database=DATABASE)
            
            if not count_result.empty:
                total_rows = count_result['total_rows'].iloc[0]
                print(f"   🎯 Total rows across all parquet files: {total_rows:,}")
            
            # Test 2: Sample data
            print(f"\n   Test 2: Sample data...")
            sample_query = f"SELECT * FROM {DATABASE}.{table_name} LIMIT 5"
            sample_result = athena.execute_query(sample_query, database=DATABASE)
            
            print(f"   ✅ Sample returned: {len(sample_result)} rows × {len(sample_result.columns)} columns")
            
            # Show sample data in a readable format
            if not sample_result.empty:
                print(f"\n   📊 Sample Data:")
                print("   " + "-" * 80)
                
                # Show key columns for billing data (adjust for your data)
                key_columns = []
                for col in ['identity_line_item_id', 'line_item_usage_account_id', 'product_product_name', 
                           'line_item_unblended_cost', 'line_item_usage_start_date']:
                    if col in sample_result.columns:
                        key_columns.append(col)
                
                # If billing columns don't exist, show first few columns
                if not key_columns:
                    key_columns = list(sample_result.columns)[:5]
                
                for idx, row in sample_result.iterrows():
                    print(f"   Row {idx+1}:")
                    for col in key_columns[:5]:  # Show max 5 columns
                        value = row[col]
                        if isinstance(value, str) and len(str(value)) > 40:
                            value = str(value)[:37] + "..."
                        print(f"     {col}: {value}")
                    print()
            
            # Test 3: Data aggregation (if multiple files, this shows they're combined)
            print(f"   Test 3: Data aggregation...")
            
            # Try different aggregation queries based on available columns
            if 'line_item_usage_start_date' in sample_result.columns:
                # Billing data aggregation
                agg_query = f"""
                SELECT 
                    DATE(line_item_usage_start_date) as usage_date,
                    COUNT(*) as records_count
                FROM {DATABASE}.{table_name}
                GROUP BY DATE(line_item_usage_start_date)
                ORDER BY usage_date
                LIMIT 10
                """
                
                agg_result = athena.execute_query(agg_query, database=DATABASE)
                print(f"   ✅ Aggregation by date: {len(agg_result)} days found")
                
                if not agg_result.empty:
                    print(f"   📈 Daily record counts:")
                    for idx, row in agg_result.iterrows():
                        date = row['usage_date']
                        count = row['records_count']
                        print(f"     {date}: {count:,} records")
            
            else:
                # Generic aggregation for any data
                first_col = sample_result.columns[0]
                agg_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT {first_col}) as unique_values
                FROM {DATABASE}.{table_name}
                """
                
                agg_result = athena.execute_query(agg_query, database=DATABASE)
                print(f"   ✅ General aggregation completed")
                
                if not agg_result.empty:
                    total = agg_result['total_records'].iloc[0]
                    unique = agg_result['unique_values'].iloc[0]
                    print(f"     Total records: {total:,}")
                    print(f"     Unique {first_col}: {unique:,}")
        
        # Summary
        print(f"\n" + "="*60)
        print(f"🎉 Test completed successfully!")
        print(f"✅ Glue Crawler automatically combined multiple parquet files")
        print(f"✅ Athena queries work against the unified table")
        print(f"✅ Schema auto-discovered: {len(sample_result.columns)} columns")
        print(f"")
        print(f"💡 Key Points:")
        print(f"   • Multiple parquet files → Single queryable table")
        print(f"   • No manual joining required")
        print(f"   • Schema discovered automatically")
        print(f"   • Ready for complex SQL queries")
        print(f"")
        print(f"🚀 Usage for your API:")
        print(f"   athena = AthenaSparkSQL(region='{REGION}')")
        print(f"   df = athena.execute_query('YOUR_SQL_HERE', database='{DATABASE}')")
        print(f"="*60)
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

def cleanup_test_resources():
    """Optional: Clean up test resources"""
    
    REGION = "us-west-2"
    DATABASE = "simple_test_db"
    CRAWLER_NAME = "simple_test_crawler"
    
    print("\n=== Cleanup Test Resources ===")
    
    try:
        glue = GlueCrawler(region=REGION)
        
        # Delete crawler
        try:
            glue.delete_crawler(CRAWLER_NAME)
            print(f"✅ Crawler '{CRAWLER_NAME}' deleted")
        except Exception as e:
            print(f"ℹ️  Crawler cleanup: {e}")
        
        # Note: We keep the database and table for continued use
        print(f"ℹ️  Database '{DATABASE}' kept for continued use")
        print("✅ Cleanup completed")
        
    except Exception as e:
        print(f"⚠️  Cleanup failed: {e}")

if __name__ == "__main__":
    # Run the test
    simple_glue_athena_test()
    
    # Uncomment to clean up resources after testing
    # cleanup_test_resources() 
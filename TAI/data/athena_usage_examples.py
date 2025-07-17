#!/usr/bin/env python3
"""
AWS Athena Spark SQL Usage Examples

This file demonstrates how to use the independent athena.py module 
with enhanced Spark SQL capabilities for AWS cost analysis and 
general data querying.

Created as part of the separation of Athena functionality from master.py
"""

from TAI.data.athena import AthenaSparkSQL
import pandas as pd
from datetime import datetime, timedelta

def main():
    """
    Comprehensive examples of using the AthenaSparkSQL class
    """
    
    # Initialize Athena handler
    print("🚀 Initializing Athena Spark SQL handler...")
    athena = AthenaSparkSQL(aws_profile=None, region='us-east-1')
    
    # =========================================================================
    # BASIC OPERATIONS
    # =========================================================================
    
    print("\n📊 BASIC OPERATIONS")
    print("=" * 50)
    
    # List available databases
    try:
        databases = athena.list_databases()
        print(f"Available databases: {databases}")
    except Exception as e:
        print(f"Note: Could not list databases - {e}")
    
    # =========================================================================
    # CUR INTEGRATION EXAMPLES
    # =========================================================================
    
    print("\n💰 AWS COST AND USAGE REPORTS (CUR) INTEGRATION")
    print("=" * 50)
    
    # Setup CUR integration with optimizations
    try:
        setup_result = athena.setup_cur_integration(
            database_name='demo_cur_db',
            table_name='demo_cur_table',
            s3_bucket='your-cur-bucket',  # Replace with actual bucket
            s3_prefix='cur-data/',
            enable_partition_projection=True
        )
        print(f"CUR setup result: {setup_result}")
    except Exception as e:
        print(f"CUR setup failed (expected without real bucket): {e}")
    
    # Query latest month's cost data with Spark optimizations
    try:
        latest_costs = athena.query_latest_month_data(
            database='demo_cur_db',
            table='demo_cur_table',
            limit=20,
            cost_threshold=1.0  # Only costs >= $1
        )
        print(f"Latest month costs shape: {latest_costs.shape}")
        print(latest_costs.head())
    except Exception as e:
        print(f"Latest costs query failed (expected without real data): {e}")
    
    # =========================================================================
    # ADVANCED SPARK SQL FEATURES
    # =========================================================================
    
    print("\n⚡ ADVANCED SPARK SQL FEATURES")
    print("=" * 50)
    
    # Example 1: Create optimized view
    sample_view_query = """
    SELECT 
        product_product_name as service,
        SUM(line_item_unblended_cost) as total_cost,
        COUNT(*) as line_items
    FROM demo_cur_db.demo_cur_table
    WHERE line_item_usage_start_date >= current_date - interval '30' day
    GROUP BY product_product_name
    ORDER BY total_cost DESC
    """
    
    try:
        view_result = athena.create_view(
            view_name='monthly_service_costs',
            query=sample_view_query,
            database='demo_cur_db'
        )
        print(f"View creation result: {view_result}")
    except Exception as e:
        print(f"View creation failed (expected): {e}")
    
    # Example 2: Query optimization
    sample_query = """
    SELECT c.service, c.region, SUM(c.cost) as total_cost
    FROM costs c
    JOIN services s ON c.service_id = s.id
    WHERE c.date >= '2024-01-01'
    GROUP BY c.service, c.region
    ORDER BY total_cost DESC
    """
    
    optimized_query = athena.optimize_query_performance(
        query=sample_query,
        enable_adaptive_query=True,
        enable_dynamic_filtering=True,
        enable_join_reordering=True
    )
    
    print("Original query:")
    print(sample_query[:100] + "...")
    print("\nOptimized query:")
    print(optimized_query[:200] + "...")
    
    # Example 3: Query analysis
    analysis_result = athena.analyze_query_plan(sample_query)
    print(f"\nQuery analysis: {analysis_result}")
    
    # Example 4: Batch query execution
    batch_queries = [
        {
            'name': 'top_services',
            'query': 'SELECT service, SUM(cost) FROM costs GROUP BY service ORDER BY SUM(cost) DESC LIMIT 10',
            'database': 'demo_cur_db'
        },
        {
            'name': 'daily_trends', 
            'query': 'SELECT date, SUM(cost) FROM costs WHERE date >= current_date - 7 GROUP BY date',
            'database': 'demo_cur_db'
        }
    ]
    
    try:
        batch_results = athena.execute_batch_queries(
            queries=batch_queries,
            parallel_execution=True,
            max_workers=3
        )
        print(f"\nBatch query results: {len(batch_results)} queries executed")
        for result in batch_results:
            print(f"  - {result['query_name']}: {result['status']}")
    except Exception as e:
        print(f"Batch execution failed (expected): {e}")
    
    # =========================================================================
    # COST ANALYSIS EXAMPLES
    # =========================================================================
    
    print("\n📈 COST ANALYSIS EXAMPLES")
    print("=" * 50)
    
    # Cost by service analysis
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        service_costs = athena.get_cost_by_service(
            start_date=start_date,
            end_date=end_date,
            database='demo_cur_db',
            table='demo_cur_table',
            group_by_account=True,
            min_cost=10.0
        )
        print(f"Service costs analysis shape: {service_costs.shape}")
    except Exception as e:
        print(f"Service costs analysis failed (expected): {e}")
    
    # Daily cost trends
    try:
        daily_trends = athena.get_daily_cost_trend(
            start_date=start_date,
            end_date=end_date,
            database='demo_cur_db',
            table='demo_cur_table',
            service_name='Amazon Elastic Compute Cloud - Compute'
        )
        print(f"Daily trends shape: {daily_trends.shape}")
    except Exception as e:
        print(f"Daily trends analysis failed (expected): {e}")
    
    # Cost anomaly detection
    try:
        anomalies = athena.analyze_cost_anomalies(
            start_date=start_date,
            end_date=end_date,
            database='demo_cur_db',
            table='demo_cur_table',
            anomaly_threshold=2.0  # 2 standard deviations
        )
        print(f"Cost anomalies detected: {len(anomalies)} records")
    except Exception as e:
        print(f"Anomaly detection failed (expected): {e}")
    
    # =========================================================================
    # CACHING AND PERFORMANCE
    # =========================================================================
    
    print("\n🚀 CACHING AND PERFORMANCE FEATURES")
    print("=" * 50)
    
    # Cache expensive query results
    expensive_query = """
    WITH monthly_aggregates AS (
        SELECT 
            DATE_TRUNC('month', line_item_usage_start_date) as month,
            product_product_name,
            SUM(line_item_unblended_cost) as monthly_cost
        FROM demo_cur_db.demo_cur_table
        GROUP BY 1, 2
    )
    SELECT * FROM monthly_aggregates
    WHERE monthly_cost > 100
    ORDER BY month DESC, monthly_cost DESC
    """
    
    try:
        cache_result = athena.cache_query_result(
            query=expensive_query,
            cache_name='monthly_high_cost_services',
            database='demo_cur_db',
            ttl_hours=24
        )
        print(f"Query caching result: {cache_result}")
    except Exception as e:
        print(f"Query caching failed (expected): {e}")
    
    # Create table from query (CTAS)
    try:
        ctas_result = athena.create_table_from_query(
            new_table_name='high_cost_services_summary',
            query=expensive_query,
            database='demo_cur_db',
            s3_location='s3://your-bucket/tables/high-cost-summary/',
            file_format='PARQUET',
            partition_by=['month']
        )
        print(f"CTAS result: {ctas_result}")
    except Exception as e:
        print(f"CTAS failed (expected): {e}")
    
    print("\n✅ Athena Spark SQL examples completed!")
    print("\n💡 Key Features Demonstrated:")
    print("   • Enhanced Spark SQL query execution with optimizations")
    print("   • AWS CUR integration with partition projection")
    print("   • Advanced cost analysis and anomaly detection") 
    print("   • Batch query processing with parallel execution")
    print("   • Query result caching and performance monitoring")
    print("   • CTAS (Create Table As Select) functionality")
    print("   • Query plan analysis and optimization hints")

def demonstrate_real_world_usage():
    """
    Real-world usage patterns for the Athena Spark SQL module
    """
    print("\n🌍 REAL-WORLD USAGE PATTERNS")
    print("=" * 50)
    
    athena = AthenaSparkSQL()
    
    # Pattern 1: Monthly cost reporting
    print("\n1. Monthly Cost Reporting Pipeline")
    monthly_report_queries = [
        {
            'name': 'total_monthly_spend',
            'query': """
                SELECT 
                    DATE_TRUNC('month', line_item_usage_start_date) as month,
                    SUM(line_item_unblended_cost) as total_cost
                FROM cur_database.cur_table 
                WHERE line_item_usage_start_date >= current_date - interval '12' month
                GROUP BY 1 ORDER BY 1 DESC
            """,
            'database': 'cur_database'
        },
        {
            'name': 'top_services_monthly',
            'query': """
                SELECT 
                    product_product_name,
                    SUM(line_item_unblended_cost) as cost
                FROM cur_database.cur_table
                WHERE line_item_usage_start_date >= date_trunc('month', current_date)
                GROUP BY 1 ORDER BY 2 DESC LIMIT 20
            """,
            'database': 'cur_database'
        }
    ]
    
    # Pattern 2: Cost optimization workflow
    print("\n2. Cost Optimization Workflow")
    optimization_query = """
    WITH resource_utilization AS (
        SELECT 
            line_item_resource_id,
            product_product_name,
            AVG(line_item_usage_amount) as avg_usage,
            MAX(line_item_usage_amount) as max_usage,
            SUM(line_item_unblended_cost) as total_cost
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= current_date - interval '30' day
        AND line_item_resource_id IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT *,
           CASE 
               WHEN avg_usage / NULLIF(max_usage, 0) < 0.3 THEN 'Underutilized'
               WHEN avg_usage / NULLIF(max_usage, 0) > 0.8 THEN 'Well-utilized'
               ELSE 'Moderate'
           END as utilization_category
    FROM resource_utilization
    WHERE total_cost > 50
    ORDER BY total_cost DESC
    """
    
    optimized_opt_query = athena.optimize_query_performance(
        query=optimization_query,
        enable_adaptive_query=True,
        enable_dynamic_filtering=True
    )
    
    print("Cost optimization query optimized with Spark SQL hints")
    
    # Pattern 3: Alerting and monitoring
    print("\n3. Cost Alerting Pipeline")
    alert_query = """
    WITH daily_costs AS (
        SELECT 
            DATE(line_item_usage_start_date) as date,
            product_product_name,
            SUM(line_item_unblended_cost) as daily_cost
        FROM cur_database.cur_table
        WHERE line_item_usage_start_date >= current_date - interval '7' day
        GROUP BY 1, 2
    ),
    cost_baselines AS (
        SELECT 
            product_product_name,
            AVG(daily_cost) as baseline_cost,
            STDDEV(daily_cost) as cost_stddev
        FROM daily_costs
        WHERE date < current_date - interval '1' day
        GROUP BY 1
    )
    SELECT 
        dc.date,
        dc.product_product_name,
        dc.daily_cost,
        cb.baseline_cost,
        (dc.daily_cost - cb.baseline_cost) / NULLIF(cb.cost_stddev, 0) as z_score
    FROM daily_costs dc
    JOIN cost_baselines cb ON dc.product_product_name = cb.product_product_name
    WHERE dc.date = current_date - interval '1' day
    AND ABS((dc.daily_cost - cb.baseline_cost) / NULLIF(cb.cost_stddev, 0)) > 2
    ORDER BY ABS((dc.daily_cost - cb.baseline_cost) / NULLIF(cb.cost_stddev, 0)) DESC
    """
    
    print("Cost alerting query ready for anomaly detection")

if __name__ == "__main__":
    print("🔍 AWS Athena Spark SQL - Independent Module Examples")
    print("=" * 60)
    
    # Run main examples
    main()
    
    # Show real-world patterns
    demonstrate_real_world_usage()
    
    print("\n" + "=" * 60)
    print("📚 For more information, see:")
    print("   • TAI/data/athena.py - Main Athena class")
    print("   • AWS Athena documentation")
    print("   • Apache Spark SQL optimization guides") 
#!/usr/bin/env python3
"""
TAI DataMaster - Test AWS Data Exports Creation

This script demonstrates creating all supported AWS Data Export types:
1. CUR 2.0 - Cost and Usage Reports 2.0
2. FOCUS 1.0 - FOCUS billing data with AWS columns  
3. Cost Optimization Recommendations
4. Carbon Emissions

Run with: python TAI/data/test_create_data_exports.py
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data.master import DataMaster
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')


class DataExportsTestSuite:
    """
    Comprehensive test suite for AWS Data Exports creation.
    """
    
    def __init__(self, aws_profile: str = None, region: str = 'us-east-1'):
        """
        Initialize the test suite.
        
        Parameters:
        - aws_profile: AWS profile to use for testing
        - region: AWS region for exports
        """
        self.dm = DataMaster()
        self.aws_profile = aws_profile
        self.region = region
        self.test_results = []
        
        # Test configuration - try to get bucket from existing cur2 export
        try:
            # Try to find existing cur2 export and use its bucket
            cur2_export = self.dm.find_export_by_name('cur2', aws_profile=aws_profile, region=region)
            self.base_s3_bucket = cur2_export['s3_bucket'] if cur2_export else 'billing-data-exports-cur'
        except:
            # Fallback to default bucket
            self.base_s3_bucket = 'billing-data-exports-cur'
        
        self.test_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        
        print("=" * 80)
        print("TAI DataMaster - AWS Data Exports Creation Test Suite")
        print("=" * 80)
        print(f"AWS Profile: {aws_profile or 'default'}")
        print(f"AWS Region: {region}")
        print(f"Test Timestamp: {self.test_timestamp}")
        print(f"Base S3 Bucket: {self.base_s3_bucket}")
        print("=" * 80)
        
        # Display supported export types
        self.display_supported_export_types()

    def display_supported_export_types(self):
        """Display all supported export types and their capabilities."""
        print("\n📋 SUPPORTED AWS DATA EXPORT TYPES:")
        print("-" * 50)
        
        supported_types = self.dm.get_supported_export_types()
        for export_type, info in supported_types.items():
            print(f"\n🔹 {export_type}")
            print(f"   Description: {info['description']}")
            print(f"   Supported Formats: {', '.join(info['supported_formats'])}")
            print(f"   Frequency: {info['frequency']}")
            print(f"   Table Name: {info['table_name']}")
            
            capabilities = []
            if info['supports_resources']:
                capabilities.append("Resources")
            if info['supports_split_cost_allocation']:
                capabilities.append("Split Cost Allocation")
            if info['supports_manual_discounts']:
                capabilities.append("Manual Discounts")
            
            if capabilities:
                print(f"   Capabilities: {', '.join(capabilities)}")
            else:
                print(f"   Capabilities: None")

    def test_cur_2_0_export(self) -> Dict[str, Any]:
        """
        Test creating CUR 2.0 export with full configuration options.
        """
        print("\n1. 🧪 TESTING CUR 2.0 EXPORT CREATION")
        print("-" * 50)
        
        export_name = f'test-cur2-{self.test_timestamp}'
        s3_prefix = f'test-exports/cur2/{self.test_timestamp}/'
        
        try:
            print(f"Creating CUR 2.0 export: {export_name}")
            print(f"S3 Location: s3://{self.base_s3_bucket}/{s3_prefix}")
            
            result = self.dm.create_data_export(
                export_name=export_name,
                s3_bucket=self.base_s3_bucket,
                s3_prefix=s3_prefix,
                export_type='CUR_2_0',
                format_type='PARQUET',
                compression='GZIP',
                time_granularity='DAILY',
                aws_profile=self.aws_profile,
                region=self.region,
                include_resources=True,
                include_split_cost_allocation=True,
                include_manual_discounts=True
            )
            
            print("✅ CUR 2.0 export creation successful!")
            self._print_export_details(result)
            
            self.test_results.append({
                'export_type': 'CUR_2_0',
                'export_name': export_name,
                'status': 'SUCCESS',
                'result': result
            })
            
            return result
            
        except Exception as e:
            print(f"❌ CUR 2.0 export creation failed: {e}")
            self.test_results.append({
                'export_type': 'CUR_2_0',
                'export_name': export_name,
                'status': 'FAILED',
                'error': str(e)
            })
            return {'error': str(e)}

    def test_focus_1_0_export(self) -> Dict[str, Any]:
        """
        Test creating FOCUS 1.0 export with AWS-specific columns.
        """
        print("\n2. 🧪 TESTING FOCUS 1.0 EXPORT CREATION")
        print("-" * 50)
        
        export_name = f'test-focus1-{self.test_timestamp}'
        s3_prefix = f'test-exports/focus1/{self.test_timestamp}/'
        
        try:
            print(f"Creating FOCUS 1.0 export: {export_name}")
            print(f"S3 Location: s3://{self.base_s3_bucket}/{s3_prefix}")
            
            result = self.dm.create_data_export(
                export_name=export_name,
                s3_bucket=self.base_s3_bucket,
                s3_prefix=s3_prefix,
                export_type='FOCUS_1_0',
                format_type='PARQUET',
                compression='GZIP',
                time_granularity='SYNCHRONOUS',  # FOCUS uses SYNCHRONOUS
                aws_profile=self.aws_profile,
                region=self.region,
                include_resources=True,
                include_split_cost_allocation=True,
                include_manual_discounts=False  # Not supported for FOCUS
            )
            
            print("✅ FOCUS 1.0 export creation successful!")
            self._print_export_details(result)
            
            self.test_results.append({
                'export_type': 'FOCUS_1_0',
                'export_name': export_name,
                'status': 'SUCCESS',
                'result': result
            })
            
            return result
            
        except Exception as e:
            print(f"❌ FOCUS 1.0 export creation failed: {e}")
            self.test_results.append({
                'export_type': 'FOCUS_1_0',
                'export_name': export_name,
                'status': 'FAILED',
                'error': str(e)
            })
            return {'error': str(e)}

    def test_cost_optimization_export(self) -> Dict[str, Any]:
        """
        Test creating Cost Optimization Recommendations export.
        """
        print("\n3. 🧪 TESTING COST OPTIMIZATION RECOMMENDATIONS EXPORT")
        print("-" * 50)
        
        export_name = f'test-cost-opt-{self.test_timestamp}'
        s3_prefix = f'test-exports/cost-optimization/{self.test_timestamp}/'
        
        try:
            print(f"Creating Cost Optimization export: {export_name}")
            print(f"S3 Location: s3://{self.base_s3_bucket}/{s3_prefix}")
            
            result = self.dm.create_data_export(
                export_name=export_name,
                s3_bucket=self.base_s3_bucket,
                s3_prefix=s3_prefix,
                export_type='COST_OPTIMIZATION_RECOMMENDATIONS',
                format_type='CSV',  # CSV format for recommendations
                compression='GZIP',
                time_granularity='SYNCHRONOUS',
                aws_profile=self.aws_profile,
                region=self.region,
                include_resources=False,  # Not applicable
                include_split_cost_allocation=False,  # Not applicable
                include_manual_discounts=False  # Not supported
            )
            
            print("✅ Cost Optimization export creation successful!")
            self._print_export_details(result)
            
            self.test_results.append({
                'export_type': 'COST_OPTIMIZATION_RECOMMENDATIONS',
                'export_name': export_name,
                'status': 'SUCCESS',
                'result': result
            })
            
            return result
            
        except Exception as e:
            print(f"❌ Cost Optimization export creation failed: {e}")
            self.test_results.append({
                'export_type': 'COST_OPTIMIZATION_RECOMMENDATIONS',
                'export_name': export_name,
                'status': 'FAILED',
                'error': str(e)
            })
            return {'error': str(e)}

    def test_carbon_emissions_export(self) -> Dict[str, Any]:
        """
        Test creating Carbon Emissions export.
        """
        print("\n4. 🧪 TESTING CARBON EMISSIONS EXPORT CREATION")
        print("-" * 50)
        
        export_name = f'test-carbon-{self.test_timestamp}'
        s3_prefix = f'test-exports/carbon-emissions/{self.test_timestamp}/'
        
        try:
            print(f"Creating Carbon Emissions export: {export_name}")
            print(f"S3 Location: s3://{self.base_s3_bucket}/{s3_prefix}")
            
            result = self.dm.create_data_export(
                export_name=export_name,
                s3_bucket=self.base_s3_bucket,
                s3_prefix=s3_prefix,
                export_type='CARBON_EMISSIONS',
                format_type='PARQUET',
                compression='GZIP',
                time_granularity='SYNCHRONOUS',
                aws_profile=self.aws_profile,
                region=self.region,
                include_resources=False,  # Not applicable for carbon data
                include_split_cost_allocation=False,  # Not applicable
                include_manual_discounts=False  # Not supported
            )
            
            print("✅ Carbon Emissions export creation successful!")
            self._print_export_details(result)
            
            self.test_results.append({
                'export_type': 'CARBON_EMISSIONS',
                'export_name': export_name,
                'status': 'SUCCESS',
                'result': result
            })
            
            return result
            
        except Exception as e:
            print(f"❌ Carbon Emissions export creation failed: {e}")
            self.test_results.append({
                'export_type': 'CARBON_EMISSIONS',
                'export_name': export_name,
                'status': 'FAILED',
                'error': str(e)
            })
            return {'error': str(e)}

    def test_bulk_export_creation(self) -> List[Dict[str, Any]]:
        """
        Test creating multiple exports with different configurations.
        """
        print("\n5. 🧪 TESTING BULK EXPORT CREATION")
        print("-" * 50)
        
        bulk_configs = [
            {
                'export_name': f'bulk-cur2-daily-{self.test_timestamp}',
                'export_type': 'CUR_2_0',
                'time_granularity': 'DAILY',
                'format_type': 'PARQUET',
                's3_prefix': f'bulk-test/cur2-daily/{self.test_timestamp}/'
            },
            {
                'export_name': f'bulk-cur2-monthly-{self.test_timestamp}',
                'export_type': 'CUR_2_0',
                'time_granularity': 'MONTHLY',
                'format_type': 'CSV',
                's3_prefix': f'bulk-test/cur2-monthly/{self.test_timestamp}/'
            },
            {
                'export_name': f'bulk-focus-{self.test_timestamp}',
                'export_type': 'FOCUS_1_0',
                'time_granularity': 'SYNCHRONOUS',
                'format_type': 'PARQUET',
                's3_prefix': f'bulk-test/focus/{self.test_timestamp}/'
            }
        ]
        
        results = []
        
        for config in bulk_configs:
            try:
                print(f"\nCreating bulk export: {config['export_name']}")
                print(f"Type: {config['export_type']}, Format: {config['format_type']}")
                
                result = self.dm.create_data_export(
                    export_name=config['export_name'],
                    s3_bucket=self.base_s3_bucket,
                    s3_prefix=config['s3_prefix'],
                    export_type=config['export_type'],
                    format_type=config['format_type'],
                    time_granularity=config['time_granularity'],
                    aws_profile=self.aws_profile,
                    region=self.region
                )
                
                print(f"✅ Bulk export {config['export_name']} created successfully!")
                results.append({**config, 'status': 'SUCCESS', 'result': result})
                
            except Exception as e:
                print(f"❌ Bulk export {config['export_name']} failed: {e}")
                results.append({**config, 'status': 'FAILED', 'error': str(e)})
        
        return results

    def test_export_validation(self) -> Dict[str, Any]:
        """
        Test export creation with invalid parameters to verify validation.
        """
        print("\n6. 🧪 TESTING EXPORT VALIDATION")
        print("-" * 50)
        
        validation_tests = []
        
        # Test 1: Invalid export type
        try:
            print("Testing invalid export type...")
            self.dm.create_data_export(
                export_name=f'invalid-type-{self.test_timestamp}',
                s3_bucket=self.base_s3_bucket,
                s3_prefix='validation-test/',
                export_type='INVALID_TYPE',
                aws_profile=self.aws_profile,
                region=self.region
            )
            validation_tests.append({'test': 'invalid_export_type', 'status': 'FAILED', 'reason': 'Should have raised ValueError'})
        except ValueError as e:
            print(f"✅ Correctly caught invalid export type: {e}")
            validation_tests.append({'test': 'invalid_export_type', 'status': 'PASSED', 'error': str(e)})
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            validation_tests.append({'test': 'invalid_export_type', 'status': 'FAILED', 'error': str(e)})
        
        return {'validation_tests': validation_tests}

    def _print_export_details(self, result: Dict[str, Any]):
        """Helper method to print export creation details."""
        print(f"   Export Name: {result.get('ExportName', 'N/A')}")
        print(f"   Export Type: {result.get('ExportType', 'N/A')}")
        print(f"   S3 Bucket: {result.get('S3Bucket', 'N/A')}")
        print(f"   S3 Prefix: {result.get('S3Prefix', 'N/A')}")
        print(f"   Format: {result.get('Format', 'N/A')}")
        print(f"   Status: {result.get('Status', 'N/A')}")
        if result.get('ExportArn'):
            print(f"   Export ARN: {result.get('ExportArn')}")

    def run_all_tests(self) -> Dict[str, Any]:
        """
        Run all data export creation tests.
        """
        print(f"\n🚀 STARTING COMPREHENSIVE DATA EXPORTS TEST SUITE")
        print(f"Test Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Run individual export type tests
        cur2_result = self.test_cur_2_0_export()
        focus_result = self.test_focus_1_0_export()
        cost_opt_result = self.test_cost_optimization_export()
        carbon_result = self.test_carbon_emissions_export()
        
        # Run bulk tests
        bulk_results = self.test_bulk_export_creation()
        
        # Run validation tests
        validation_results = self.test_export_validation()
        
        # Generate test summary
        self.generate_test_summary()
        
        return {
            'test_timestamp': self.test_timestamp,
            'individual_tests': {
                'cur2': cur2_result,
                'focus1': focus_result,
                'cost_optimization': cost_opt_result,
                'carbon_emissions': carbon_result
            },
            'bulk_tests': bulk_results,
            'validation_tests': validation_results,
            'summary': self.test_results
        }

    def generate_test_summary(self):
        """
        Generate and display a comprehensive test summary.
        """
        print("\n\n" + "=" * 80)
        print("TEST SUMMARY REPORT")
        print("=" * 80)
        
        total_tests = len(self.test_results)
        successful_tests = len([r for r in self.test_results if r['status'] == 'SUCCESS'])
        failed_tests = total_tests - successful_tests
        
        print(f"📊 Test Results Overview:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Successful: {successful_tests} ✅")
        print(f"   Failed: {failed_tests} ❌")
        print(f"   Success Rate: {(successful_tests/total_tests*100):.1f}%" if total_tests > 0 else "   Success Rate: 0%")
        
        print(f"\n📋 Individual Test Results:")
        for result in self.test_results:
            status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
            print(f"   {status_icon} {result['export_type']}: {result['export_name']} - {result['status']}")
            if result['status'] == 'FAILED':
                print(f"      Error: {result.get('error', 'Unknown error')}")
        
        print(f"\n🔧 AWS Data Export Types Tested:")
        tested_types = set([r['export_type'] for r in self.test_results])
        for export_type in tested_types:
            type_results = [r for r in self.test_results if r['export_type'] == export_type]
            success_count = len([r for r in type_results if r['status'] == 'SUCCESS'])
            total_count = len(type_results)
            print(f"   • {export_type}: {success_count}/{total_count} successful")
        
        print(f"\n💡 Key Features Tested:")
        print(f"   ✓ Dynamic export configuration for all types")
        print(f"   ✓ Flexible parameter validation")
        print(f"   ✓ Multiple S3 bucket and prefix configurations")
        print(f"   ✓ Different format types (PARQUET, CSV)")
        print(f"   ✓ Various time granularities (DAILY, MONTHLY, SYNCHRONOUS)")
        print(f"   ✓ Real AWS API integration")
        
        print(f"\n🏁 Test Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)


def main():
    """
    Main function to run the data exports test suite.
    """
    # You can customize these parameters
    AWS_PROFILE = None  # Set to your AWS profile name or None for default
    AWS_REGION = 'us-east-1'  # Set to your preferred region
    
    try:
        # Initialize and run test suite
        test_suite = DataExportsTestSuite(
            aws_profile=AWS_PROFILE,
            region=AWS_REGION
        )
        
        # Run all tests
        results = test_suite.run_all_tests()
        
        # Optionally save results to file
        import json
        results_file = f'test_results_{test_suite.test_timestamp}.json'
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Full test results saved to: {results_file}")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        print("Common causes:")
        print("   • AWS credentials not configured")
        print("   • Insufficient IAM permissions for Data Exports")
        print("   • Invalid S3 bucket name or permissions")
        print("   • bcm-data-exports service not available in region")


if __name__ == "__main__":
    main() 
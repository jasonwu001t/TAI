#!/usr/bin/env python3
"""
Step-by-step test to create and verify AWS CUR 2.0 Data Export
This will help us diagnose any issues with export creation.
"""

import sys
import os
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data.master import DataMaster
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

def test_step_by_step():
    """
    Step-by-step test to create and verify CUR 2.0 export
    """
    print("=" * 80)
    print("STEP-BY-STEP CUR 2.0 DATA EXPORT TEST")
    print("=" * 80)
    
    # Initialize DataMaster
    dm = DataMaster()
    
    # Step 1: List existing exports before creation
    print("\n📋 STEP 1: List existing exports BEFORE creation")
    print("-" * 50)
    
    try:
        existing_exports = dm.list_data_exports()
        print(f"Found {len(existing_exports)} existing exports:")
        for export in existing_exports:
            print(f"   • {export['ExportName']} ({export['ExportType']}) - Status: {export['Status']}")
    except Exception as e:
        print(f"❌ Error listing exports: {e}")
        return False
    
    # Step 2: Create a simple CUR 2.0 export with minimal configuration
    print("\n🔧 STEP 2: Create a new CUR 2.0 export")
    print("-" * 50)
    
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    export_name = f'test-step-by-step-{timestamp}'
    
    # Try to use existing S3 bucket from cur2 export
    try:
        cur2_export = dm.find_export_by_name('cur2')
        s3_bucket = cur2_export['s3_bucket'] if cur2_export else 'billing-data-exports-cur'
    except:
        s3_bucket = 'billing-data-exports-cur'
    
    print(f"Creating export: {export_name}")
    print(f"S3 Bucket: {s3_bucket}")
    print(f"Export Type: CUR_2_0")
    
    try:
        # Create export with exact same configuration as working cur2 export
        result = dm.create_data_export(
            export_name=export_name,
            s3_bucket=s3_bucket,
            s3_prefix=f'step-by-step-test/{timestamp}/',
            export_type='CUR_2_0',
            format_type='PARQUET',
            compression='PARQUET',  # Match working export
            time_granularity='DAILY',  # This will be converted to SYNCHRONOUS
            aws_profile=None,
            region='us-east-1',
            include_resources=True,
            include_split_cost_allocation=True,
            include_manual_discounts=False  # Match working export (FALSE)
        )
        
        print(f"✅ Export creation completed!")
        print(f"   Result Status: {result.get('Status', 'UNKNOWN')}")
        print(f"   Export Name: {result.get('ExportName', 'N/A')}")
        print(f"   S3 Location: s3://{result.get('S3Bucket', 'N/A')}/{result.get('S3Prefix', 'N/A')}")
        
        # Check if creation was actually successful
        if result.get('Status') == 'FAILED':
            print(f"❌ Export creation FAILED!")
            print(f"   Error: {result.get('Error', 'Unknown error')}")
            return False
        elif result.get('Status') == 'CREATED':
            print(f"✅ Export creation SUCCEEDED!")
        else:
            print(f"⚠️  Export creation status unclear: {result.get('Status')}")
            
    except Exception as e:
        print(f"❌ Error creating export: {e}")
        return False
    
    # Step 3: Wait a moment for AWS to process
    print("\n⏳ STEP 3: Wait for AWS processing")
    print("-" * 50)
    import time
    print("Waiting 5 seconds for AWS to process the export...")
    time.sleep(5)
    
    # Step 4: List exports again to verify creation
    print("\n📋 STEP 4: List exports AFTER creation to verify")
    print("-" * 50)
    
    try:
        new_exports = dm.list_data_exports()
        print(f"Found {len(new_exports)} total exports:")
        
        # Look for our new export
        found_new_export = False
        for export in new_exports:
            status_icon = "✅" if export['Status'] in ['CREATED', 'ACTIVE'] else "❌"
            print(f"   {status_icon} {export['ExportName']} ({export['ExportType']}) - Status: {export['Status']}")
            
            if export['ExportName'] == export_name:
                found_new_export = True
                print(f"      🎯 THIS IS OUR NEW EXPORT!")
                print(f"      S3 Bucket: {export['S3Bucket']}")
                print(f"      S3 Prefix: {export['S3Prefix']}")
                print(f"      Format: {export['Format']}")
        
        if found_new_export:
            print(f"\n✅ SUCCESS: New export '{export_name}' found in export list!")
        else:
            print(f"\n❌ FAILED: New export '{export_name}' NOT found in export list!")
            return False
            
    except Exception as e:
        print(f"❌ Error listing exports after creation: {e}")
        return False
    
    # Step 5: Get detailed info about the new export
    print("\n📊 STEP 5: Get detailed information about the new export")
    print("-" * 50)
    
    try:
        export_details = dm.get_export_details(export_name)
        if export_details:
            print(f"✅ Export details retrieved successfully:")
            print(f"   Export Name: {export_details.get('ExportName', 'N/A')}")
            print(f"   Export Type: {export_details.get('ExportType', 'N/A')}")
            print(f"   Status: {export_details.get('Status', 'N/A')}")
            print(f"   S3 Location: s3://{export_details.get('S3Bucket', 'N/A')}/{export_details.get('S3Prefix', 'N/A')}")
            print(f"   Format: {export_details.get('Format', 'N/A')}")
            print(f"   Created: {export_details.get('CreatedDate', 'N/A')}")
        else:
            print(f"❌ Could not retrieve export details for '{export_name}'")
            return False
            
    except Exception as e:
        print(f"❌ Error getting export details: {e}")
        return False
    
    # Step 6: Summary
    print("\n🎉 STEP 6: Test Summary")
    print("-" * 50)
    print(f"✅ Export creation: SUCCESS")
    print(f"✅ Export verification: SUCCESS")
    print(f"✅ Export details: SUCCESS")
    print(f"\n🎯 Export '{export_name}' created and verified successfully!")
    
    return True

def main():
    """
    Main function to run the step-by-step test
    """
    try:
        success = test_step_by_step()
        if success:
            print(f"\n🎉 OVERALL TEST RESULT: SUCCESS")
        else:
            print(f"\n❌ OVERALL TEST RESULT: FAILED")
            
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
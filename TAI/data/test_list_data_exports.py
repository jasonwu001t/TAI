#!/usr/bin/env python3
"""
Simple script to list real AWS Data Exports names step by step
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data.master import DataMaster


def list_real_export_names():
    """
    Step 1: List all real AWS Data Export names
    """
    print("=" * 60)
    print("STEP 1: LISTING REAL AWS DATA EXPORTS NAMES")
    print("=" * 60)
    
    dm = DataMaster()
    
    try:
        print("🔍 Searching for AWS Data Exports...")
        print("   - Checking bcm-data-exports service...")
        print("   - Checking legacy CUR service...")
        print()
        
        # Get real exports from AWS
        exports = dm.list_data_exports()
        
        if exports:
            print(f"✅ Found {len(exports)} data export(s):")
            print()
            
            for i, export in enumerate(exports, 1):
                print(f"   {i}. Export Name: '{export['ExportName']}'")
                print(f"      Type: {export['ExportType']}")
                print(f"      Status: {export['Status']}")
                if export['S3Bucket']:
                    print(f"      S3 Bucket: {export['S3Bucket']}")
                    print(f"      S3 Prefix: {export['S3Prefix']}")
                print()
            
            # Extract just the names for easy reference
            export_names = [export['ExportName'] for export in exports]
            print("📋 Export Names Summary:")
            for name in export_names:
                print(f"   • {name}")
                
        else:
            print("❌ No data exports found in your AWS account")
            print()
            print("Possible reasons:")
            print("   1. No Data Exports have been created yet")
            print("   2. Insufficient permissions to access Data Exports")
            print("   3. Wrong AWS region (try us-east-1)")
            print("   4. AWS credentials not configured")
            
    except Exception as e:
        print(f"❌ Error accessing AWS: {e}")
        print()
        print("Troubleshooting steps:")
        print("   1. Check AWS credentials: aws configure list")
        print("   2. Verify IAM permissions for Data Exports")
        print("   3. Try running: aws sts get-caller-identity")
        
    print("\n" + "=" * 60)
    

if __name__ == "__main__":
    list_real_export_names() 
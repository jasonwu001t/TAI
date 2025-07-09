#!/usr/bin/env python3
"""
Step 2: Get detailed information about cur2 export including S3 bucket
"""

import sys
import os
import boto3
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data.master import DataMaster


def get_cur2_details():
    """
    Step 2: Get detailed information about cur2 export and its S3 bucket
    """
    print("=" * 60)
    print("STEP 2: GETTING CUR2 EXPORT DETAILS & S3 BUCKET")
    print("=" * 60)
    
    try:
        # Get AWS session
        session = boto3.Session()
        bcm_client = session.client('bcm-data-exports', region_name='us-east-1')
        
        print("🔍 Getting detailed information for cur2 export...")
        
        # Get cur2 export ARN from the list we saw earlier
        cur2_arn = "arn:aws:bcm-data-exports:us-east-1:014498620306:export/cur2-2b3b9ac0-c31e-484d-81db-4bfdd10278f3"
        
        # Get detailed export information
        print(f"📋 Fetching details for: {cur2_arn}")
        response = bcm_client.get_export(ExportArn=cur2_arn)
        
        if 'Export' in response:
            export = response['Export']
            print("\n✅ CUR2 Export Details:")
            print(f"   • Export Name: {export.get('ExportName', 'N/A')}")
            print(f"   • Export ARN: {export.get('ExportArn', 'N/A')}")
            print(f"   • Status: {export.get('ExportStatus', {}).get('StatusCode', 'N/A')}")
            print(f"   • Created: {export.get('ExportStatus', {}).get('CreatedAt', 'N/A')}")
            print(f"   • Last Refreshed: {export.get('ExportStatus', {}).get('LastRefreshedAt', 'N/A')}")
            
            # Extract S3 destination details
            dest_configs = export.get('DestinationConfigurations', {})
            s3_configs = dest_configs.get('S3OutputConfigurations', {})
            
            if s3_configs:
                print(f"\n📦 S3 Destination Details:")
                print(f"   • S3 Bucket: {s3_configs.get('BucketName', 'N/A')}")
                print(f"   • S3 Prefix: {s3_configs.get('Prefix', 'N/A')}")
                print(f"   • Format: {s3_configs.get('Format', 'N/A')}")
                print(f"   • Compression: {s3_configs.get('Compression', 'N/A')}")
                print(f"   • Output Type: {s3_configs.get('OutputType', 'N/A')}")
                print(f"   • Full S3 Location: s3://{s3_configs.get('BucketName', 'unknown')}/{s3_configs.get('Prefix', '')}")
                
                # Store for next steps
                bucket_name = s3_configs.get('BucketName', '')
                s3_prefix = s3_configs.get('Prefix', '')
                
                print(f"\n🎯 KEY FINDINGS:")
                print(f"   • cur2 export FOUND ✅")
                print(f"   • S3 Bucket: '{bucket_name}'")
                print(f"   • S3 Prefix: '{s3_prefix}'")
                print(f"   • This is the REAL S3 location (not 'company-billing-data')")
                
                return {
                    'export_name': export.get('ExportName', ''),
                    's3_bucket': bucket_name,
                    's3_prefix': s3_prefix,
                    'format': s3_configs.get('Format', ''),
                    'status': export.get('ExportStatus', {}).get('StatusCode', '')
                }
            else:
                print("\n❌ No S3 destination configuration found")
                print(f"Raw destination configs: {dest_configs}")
        else:
            print("❌ Export details not found in response")
            
    except Exception as e:
        print(f"❌ Error getting cur2 details: {e}")
        print("\nTrying alternative approach...")
        
        # Fallback: try to get details via DataMaster
        try:
            dm = DataMaster()
            export_details = dm.get_export_details('cur2')
            print(f"Fallback result: {export_details}")
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
        
    print("\n" + "=" * 60)
    

if __name__ == "__main__":
    cur2_info = get_cur2_details()
    if cur2_info:
        print(f"\n🚀 Ready for next step with:")
        print(f"   Bucket: {cur2_info['s3_bucket']}")
        print(f"   Prefix: {cur2_info['s3_prefix']}") 
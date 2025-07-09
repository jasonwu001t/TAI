#!/usr/bin/env python3
"""
Debug script to examine existing working exports and their configurations
"""

import sys
import os
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TAI.data.master import DataMaster
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

def debug_existing_exports():
    """
    Debug existing exports to understand the correct format
    """
    print("=" * 80)
    print("DEBUG: EXAMINE EXISTING WORKING EXPORTS")
    print("=" * 80)
    
    # Initialize DataMaster
    dm = DataMaster()
    
    # Get detailed information about existing exports
    print("\n📋 STEP 1: List all existing exports")
    print("-" * 50)
    
    try:
        exports = dm.list_data_exports()
        print(f"Found {len(exports)} existing exports:")
        for export in exports:
            print(f"   • {export['ExportName']} ({export['ExportType']}) - Status: {export['Status']}")
    except Exception as e:
        print(f"❌ Error listing exports: {e}")
        return
    
    # Get detailed information about the cur2 export
    print("\n🔍 STEP 2: Get detailed information about cur2 export")
    print("-" * 50)
    
    try:
        cur2_details = dm.get_export_details('cur2')
        if cur2_details:
            print(f"✅ Found cur2 export details:")
            print(f"   Export Name: {cur2_details.get('ExportName', 'N/A')}")
            print(f"   Export Type: {cur2_details.get('ExportType', 'N/A')}")
            print(f"   Status: {cur2_details.get('Status', 'N/A')}")
            print(f"   S3 Bucket: {cur2_details.get('S3Bucket', 'N/A')}")
            print(f"   S3 Prefix: {cur2_details.get('S3Prefix', 'N/A')}")
            print(f"   Format: {cur2_details.get('Format', 'N/A')}")
        else:
            print("❌ Could not find cur2 export details")
    except Exception as e:
        print(f"❌ Error getting cur2 details: {e}")
    
    # Use boto3 directly to get the full export configuration
    print("\n🔧 STEP 3: Get full export configuration via boto3")
    print("-" * 50)
    
    try:
        session = boto3.Session()
        bcm_client = session.client('bcm-data-exports', region_name='us-east-1')
        
        # List exports to get ARNs
        response = bcm_client.list_exports()
        
        if 'Exports' in response:
            for export in response['Exports']:
                export_name = export.get('ExportName', 'Unknown')
                export_arn = export.get('ExportArn', '')
                
                print(f"\n📊 Export: {export_name}")
                print(f"   ARN: {export_arn}")
                
                if export_name == 'cur2':
                    print(f"   🎯 This is the cur2 export we want to examine!")
                    
                    # Get detailed export configuration
                    if export_arn:
                        try:
                            detail_response = bcm_client.get_export(ExportArn=export_arn)
                            
                            if 'Export' in detail_response:
                                full_export = detail_response['Export']
                                print(f"\n📋 Full cur2 export configuration:")
                                print(json.dumps(full_export, indent=2, default=str))
                                
                                # Extract key information
                                data_query = full_export.get('DataQuery', {})
                                query_statement = data_query.get('QueryStatement', 'N/A')
                                table_configs = data_query.get('TableConfigurations', {})
                                
                                print(f"\n🔑 Key Configuration Details:")
                                print(f"   QueryStatement: {query_statement}")
                                print(f"   TableConfigurations: {json.dumps(table_configs, indent=2)}")
                                
                        except Exception as e:
                            print(f"❌ Error getting detailed export config: {e}")
                            
    except Exception as e:
        print(f"❌ Error accessing bcm-data-exports directly: {e}")
    
    print("\n" + "=" * 80)

def main():
    """
    Main function to run the debug script
    """
    try:
        debug_existing_exports()
    except Exception as e:
        print(f"\n❌ DEBUG FAILED WITH ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
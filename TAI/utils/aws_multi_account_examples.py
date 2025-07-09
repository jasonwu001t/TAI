"""
Examples for managing multiple AWS accounts in TAI

This file demonstrates various approaches for handling multiple AWS accounts
with different credentials, profiles, and runtime switching capabilities.
"""

import os
from TAI.utils import AWSConfigManager, AWSAccountConfig, aws_config, get_aws_client
from TAI.data.master import DataMaster


# =============================================================================
# 1. AWS PROFILES APPROACH (Most Recommended)
# =============================================================================

def example_1_aws_profiles():
    """
    Using AWS CLI profiles - most recommended approach
    
    Prerequisites:
    - Run: aws configure --profile dev-account
    - Run: aws configure --profile prod-account
    """
    print("=== AWS Profiles Approach ===")
    
    # List available profiles
    manager = AWSConfigManager()
    profiles = manager.list_available_profiles()
    print(f"Available profiles: {profiles}")
    
    # Method 1: Using profile parameter directly
    s3_dev = get_aws_client('s3', profile='dev-account')
    s3_prod = get_aws_client('s3', profile='prod-account')
    
    # Method 2: Set default profile for session
    dev_manager = AWSConfigManager(default_profile='dev-account')
    s3_dev_default = dev_manager.get_client('s3')
    
    # Validate credentials work
    print("Validating dev account...")
    dev_valid = manager.validate_credentials(profile='dev-account')
    print(f"Dev credentials valid: {dev_valid}")
    
    if dev_valid:
        dev_info = manager.get_account_info(profile='dev-account')
        print(f"Dev Account ID: {dev_info.get('Account')}")
        print(f"Dev User ARN: {dev_info.get('Arn')}")


def example_2_switch_accounts_runtime():
    """
    Switch between accounts at runtime using profiles
    """
    print("=== Runtime Account Switching ===")
    
    accounts = {
        'development': 'dev-account',
        'production': 'prod-account',
        'client-a': 'client-a-profile'
    }
    
    for env_name, profile in accounts.items():
        print(f"\nSwitching to {env_name} environment...")
        try:
            # Get S3 client for specific account
            s3 = get_aws_client('s3', profile=profile)
            
            # Get account info
            sts = get_aws_client('sts', profile=profile)
            account_info = sts.get_caller_identity()
            
            print(f"✅ Connected to account: {account_info['Account']}")
            
            # List buckets to verify access
            buckets = s3.list_buckets()
            print(f"   Buckets found: {len(buckets['Buckets'])}")
            
        except Exception as e:
            print(f"❌ Failed to connect to {env_name}: {e}")


# =============================================================================
# 2. ENVIRONMENT-BASED MULTI-ACCOUNT APPROACH
# =============================================================================

def example_3_environment_based():
    """
    Using environment variables for different accounts
    """
    print("=== Environment-Based Multi-Account ===")
    
    # Account configurations via environment variables
    account_configs = {
        'dev': {
            'AWS_ACCESS_KEY_ID': os.getenv('DEV_AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('DEV_AWS_SECRET_ACCESS_KEY'),
            'AWS_DEFAULT_REGION': 'us-east-1'
        },
        'prod': {
            'AWS_ACCESS_KEY_ID': os.getenv('PROD_AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('PROD_AWS_SECRET_ACCESS_KEY'),
            'AWS_DEFAULT_REGION': 'us-west-2'
        }
    }
    
    for env_name, config in account_configs.items():
        if config['AWS_ACCESS_KEY_ID']:
            print(f"\nTesting {env_name} environment...")
            
            # Create account config
            account_config = AWSAccountConfig(
                access_key_id=config['AWS_ACCESS_KEY_ID'],
                secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
                region_name=config['AWS_DEFAULT_REGION'],
                account_alias=env_name
            )
            
            # Test connection
            manager = AWSConfigManager()
            is_valid = manager.validate_credentials(account_config=account_config)
            print(f"{env_name} credentials valid: {is_valid}")


# =============================================================================
# 3. ENHANCED CONFIGLOADER WITH MULTIPLE AWS SECTIONS
# =============================================================================

def example_4_enhanced_config_file():
    """
    Enhanced auth.ini with multiple AWS account sections
    
    Create auth.ini with sections like:
    [AWS_DEV]
    aws_access_key_id = ...
    aws_secret_access_key = ...
    region_name = us-east-1
    
    [AWS_PROD]
    aws_access_key_id = ...
    aws_secret_access_key = ...
    region_name = us-west-2
    """
    print("=== Enhanced ConfigLoader with Multiple AWS Sections ===")
    
    from TAI.utils import ConfigLoader
    
    # Load different AWS configurations
    config = ConfigLoader()
    
    environments = ['DEV', 'PROD', 'STAGING']
    
    for env in environments:
        section = f'AWS_{env}'
        print(f"\nChecking {env} environment...")
        
        access_key = config.get_config(section, 'aws_access_key_id')
        secret_key = config.get_config(section, 'aws_secret_access_key')
        region = config.get_config(section, 'region_name')
        
        if access_key and secret_key:
            account_config = AWSAccountConfig(
                access_key_id=access_key,
                secret_access_key=secret_key,
                region_name=region or 'us-east-1',
                account_alias=env.lower()
            )
            
            manager = AWSConfigManager()
            is_valid = manager.validate_credentials(account_config=account_config)
            print(f"{env} credentials valid: {is_valid}")
        else:
            print(f"{env} credentials not found in config")


# =============================================================================
# 4. DYNAMIC RUNTIME CONFIGURATION
# =============================================================================

class MultiAccountDataMaster(DataMaster):
    """
    Enhanced DataMaster with multi-account support
    """
    
    def __init__(self, default_profile: str = None):
        super().__init__()
        self.aws_manager = AWSConfigManager(default_profile=default_profile)
        self.current_profile = default_profile
        
    def set_aws_account(self, profile: str = None, account_config: AWSAccountConfig = None):
        """Switch AWS account context"""
        if profile:
            self.current_profile = profile
            print(f"Switched to AWS profile: {profile}")
        elif account_config:
            self.aws_manager.set_account_config(account_config)
            print(f"Switched to AWS account: {account_config.account_alias}")
            
    def load_s3_multi_account(self, bucket_name: str, profile: str = None, **kwargs):
        """Load S3 data with specific account profile"""
        s3_client = self.aws_manager.get_client('s3', profile=profile or self.current_profile)
        
        # Override the s3_client in the parent method parameters
        kwargs['s3_client'] = s3_client
        return super().load_s3(bucket_name, **kwargs)
        
    def save_s3_multi_account(self, data, bucket_name: str, profile: str = None, **kwargs):
        """Save S3 data with specific account profile"""
        s3_client = self.aws_manager.get_client('s3', profile=profile or self.current_profile)
        
        # Custom implementation using the specific s3_client
        # (Implementation details would depend on your specific needs)
        print(f"Saving to S3 bucket '{bucket_name}' using profile '{profile or self.current_profile}'")


def example_5_dynamic_data_master():
    """
    Using enhanced DataMaster with multiple accounts
    """
    print("=== Dynamic Multi-Account DataMaster ===")
    
    # Initialize with default dev account
    dm = MultiAccountDataMaster(default_profile='dev-account')
    
    # Load data from dev account
    print("Loading from dev account...")
    try:
        data = dm.load_s3_multi_account(
            bucket_name='dev-bucket',
            s3_directory='data/',
            load_all=True
        )
        print("✅ Loaded data from dev account")
    except Exception as e:
        print(f"❌ Error loading from dev: {e}")
    
    # Switch to production account
    dm.set_aws_account(profile='prod-account')
    
    # Save data to production account
    print("Saving to prod account...")
    try:
        # dm.save_s3_multi_account(data, 'prod-bucket', s3_directory='processed/')
        print("✅ Would save data to prod account")
    except Exception as e:
        print(f"❌ Error saving to prod: {e}")


# =============================================================================
# 5. AWS STS ROLE ASSUMPTION
# =============================================================================

def example_6_assume_roles():
    """
    Using AWS STS to assume roles across accounts
    """
    print("=== AWS STS Role Assumption ===")
    
    manager = AWSConfigManager(default_profile='management-account')
    
    # Assume role in different accounts
    roles = [
        {
            'role_arn': 'arn:aws:iam::111111111111:role/DevAccountAccess',
            'session_name': 'TAI-Dev-Session',
            'alias': 'dev'
        },
        {
            'role_arn': 'arn:aws:iam::222222222222:role/ProdAccountAccess', 
            'session_name': 'TAI-Prod-Session',
            'alias': 'prod'
        }
    ]
    
    for role_info in roles:
        try:
            print(f"\nAssuming role for {role_info['alias']} account...")
            
            # Assume the role
            temp_config = manager.assume_role(
                role_arn=role_info['role_arn'],
                session_name=role_info['session_name'],
                duration_seconds=3600
            )
            
            # Test the temporary credentials
            is_valid = manager.validate_credentials(account_config=temp_config)
            print(f"✅ Successfully assumed {role_info['alias']} role: {is_valid}")
            
            if is_valid:
                # Use the temporary credentials
                s3 = manager.get_client('s3', account_config=temp_config)
                buckets = s3.list_buckets()
                print(f"   Found {len(buckets['Buckets'])} buckets")
                
        except Exception as e:
            print(f"❌ Failed to assume {role_info['alias']} role: {e}")


# =============================================================================
# 6. PRACTICAL WORKFLOW EXAMPLES
# =============================================================================

def example_7_practical_workflow():
    """
    Real-world workflow with multiple accounts
    """
    print("=== Practical Multi-Account Workflow ===")
    
    # Development workflow
    print("\n1. Development Phase")
    dev_s3 = get_aws_client('s3', profile='dev-account')
    dev_bedrock = get_aws_client('bedrock-runtime', profile='dev-account', region='us-west-2')
    
    print("   - Connected to dev S3 and Bedrock")
    
    # Staging validation
    print("\n2. Staging Validation")  
    staging_s3 = get_aws_client('s3', profile='staging-account')
    
    print("   - Connected to staging environment")
    
    # Production deployment
    print("\n3. Production Deployment")
    prod_s3 = get_aws_client('s3', profile='prod-account')
    prod_redshift = get_aws_client('redshift', profile='prod-account')
    
    print("   - Connected to production S3 and Redshift")
    
    # Cross-account data pipeline
    print("\n4. Cross-Account Data Pipeline")
    try:
        # Read from dev
        print("   - Reading from dev bucket...")
        
        # Process in staging
        print("   - Processing in staging...")
        
        # Deploy to prod
        print("   - Deploying to production...")
        
        print("✅ Cross-account pipeline completed successfully")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Run all examples to demonstrate multi-account capabilities
    """
    print("TAI Multi-Account AWS Configuration Examples")
    print("=" * 50)
    
    examples = [
        example_1_aws_profiles,
        example_2_switch_accounts_runtime,
        example_3_environment_based,
        example_4_enhanced_config_file,
        example_5_dynamic_data_master,
        example_6_assume_roles,
        example_7_practical_workflow
    ]
    
    for example in examples:
        try:
            print(f"\n{'='*50}")
            example()
        except Exception as e:
            print(f"❌ Example failed: {e}")
        print()


if __name__ == "__main__":
    main() 
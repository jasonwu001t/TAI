import boto3
import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from botocore.exceptions import ProfileNotFound, NoCredentialsError
import logging

logger = logging.getLogger(__name__)

@dataclass
class AWSAccountConfig:
    """Configuration for a specific AWS account"""
    profile_name: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    region_name: str = 'us-east-1'
    account_alias: Optional[str] = None

class AWSConfigManager:
    """
    Enhanced AWS configuration manager supporting multiple accounts.
    
    Supports multiple authentication methods:
    1. AWS Profiles (Recommended)
    2. Environment Variables 
    3. Runtime Configuration
    4. IAM Roles (for EC2/Lambda)
    """
    
    def __init__(self, default_profile: Optional[str] = None, default_region: str = 'us-east-1'):
        self.default_profile = default_profile
        self.default_region = default_region
        self.current_config: Optional[AWSAccountConfig] = None
        self._sessions: Dict[str, boto3.Session] = {}
        
    def set_account_config(self, config: AWSAccountConfig):
        """Set the current AWS account configuration"""
        self.current_config = config
        
    def get_session(self, 
                   profile: Optional[str] = None,
                   account_config: Optional[AWSAccountConfig] = None) -> boto3.Session:
        """
        Get a boto3 session for the specified account.
        
        Priority:
        1. account_config parameter
        2. profile parameter  
        3. current_config
        4. default_profile
        5. default session (environment variables or IAM role)
        """
        
        # Create cache key
        cache_key = self._get_cache_key(profile, account_config)
        
        if cache_key in self._sessions:
            return self._sessions[cache_key]
            
        try:
            if account_config:
                session = self._create_session_from_config(account_config)
            elif profile:
                session = boto3.Session(profile_name=profile)
            elif self.current_config:
                session = self._create_session_from_config(self.current_config)
            elif self.default_profile:
                session = boto3.Session(profile_name=self.default_profile)
            else:
                session = boto3.Session()
                
            # Cache the session
            self._sessions[cache_key] = session
            logger.info(f"Created AWS session with cache key: {cache_key}")
            return session
            
        except ProfileNotFound as e:
            logger.error(f"AWS profile not found: {e}")
            raise
        except NoCredentialsError as e:
            logger.error(f"AWS credentials not found: {e}")
            raise
            
    def _create_session_from_config(self, config: AWSAccountConfig) -> boto3.Session:
        """Create a boto3 session from AWSAccountConfig"""
        session_kwargs = {
            'region_name': config.region_name
        }
        
        if config.profile_name:
            session_kwargs['profile_name'] = config.profile_name
        elif config.access_key_id and config.secret_access_key:
            session_kwargs.update({
                'aws_access_key_id': config.access_key_id,
                'aws_secret_access_key': config.secret_access_key
            })
            if config.session_token:
                session_kwargs['aws_session_token'] = config.session_token
                
        return boto3.Session(**session_kwargs)
        
    def _get_cache_key(self, profile: Optional[str], config: Optional[AWSAccountConfig]) -> str:
        """Generate a cache key for session caching"""
        if config:
            return f"config_{config.profile_name or config.access_key_id or 'default'}_{config.region_name}"
        elif profile:
            return f"profile_{profile}"
        elif self.current_config:
            return f"current_{self.current_config.profile_name or 'default'}_{self.current_config.region_name}"
        else:
            return f"default_{self.default_profile or 'none'}"
            
    def get_client(self, 
                  service_name: str,
                  profile: Optional[str] = None,
                  account_config: Optional[AWSAccountConfig] = None,
                  **kwargs) -> Any:
        """Get a boto3 client for the specified service and account"""
        session = self.get_session(profile, account_config)
        return session.client(service_name, **kwargs)
        
    def get_resource(self, 
                    service_name: str,
                    profile: Optional[str] = None,
                    account_config: Optional[AWSAccountConfig] = None,
                    **kwargs) -> Any:
        """Get a boto3 resource for the specified service and account"""
        session = self.get_session(profile, account_config)
        return session.resource(service_name, **kwargs)
        
    def list_available_profiles(self) -> list:
        """List all available AWS profiles"""
        try:
            session = boto3.Session()
            return session.available_profiles
        except Exception as e:
            logger.error(f"Error listing profiles: {e}")
            return []
            
    def validate_credentials(self, 
                           profile: Optional[str] = None,
                           account_config: Optional[AWSAccountConfig] = None) -> bool:
        """Validate that the specified credentials work"""
        try:
            sts_client = self.get_client('sts', profile, account_config)
            response = sts_client.get_caller_identity()
            logger.info(f"Credentials valid for account: {response.get('Account')} "
                       f"user: {response.get('Arn')}")
            return True
        except Exception as e:
            logger.error(f"Credential validation failed: {e}")
            return False
            
    def get_account_info(self, 
                        profile: Optional[str] = None,
                        account_config: Optional[AWSAccountConfig] = None) -> Dict[str, str]:
        """Get information about the current AWS account"""
        try:
            sts_client = self.get_client('sts', profile, account_config)
            return sts_client.get_caller_identity()
        except Exception as e:
            logger.error(f"Error getting account info: {e}")
            return {}
            
    def assume_role(self, 
                   role_arn: str,
                   session_name: str,
                   duration_seconds: int = 3600,
                   profile: Optional[str] = None) -> AWSAccountConfig:
        """
        Assume an IAM role and return temporary credentials as AWSAccountConfig
        """
        try:
            sts_client = self.get_client('sts', profile)
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                DurationSeconds=duration_seconds
            )
            
            credentials = response['Credentials']
            return AWSAccountConfig(
                access_key_id=credentials['AccessKeyId'],
                secret_access_key=credentials['SecretAccessKey'],
                session_token=credentials['SessionToken'],
                account_alias=f"assumed_{session_name}"
            )
        except Exception as e:
            logger.error(f"Error assuming role {role_arn}: {e}")
            raise

# Global instance for easy access
aws_config = AWSConfigManager()

# Convenience functions for backward compatibility
def get_aws_client(service_name: str, 
                  profile: Optional[str] = None, 
                  region: Optional[str] = None,
                  **kwargs) -> Any:
    """Convenience function to get AWS client"""
    if region:
        kwargs['region_name'] = region
    return aws_config.get_client(service_name, profile=profile, **kwargs)

def get_aws_resource(service_name: str, 
                    profile: Optional[str] = None, 
                    region: Optional[str] = None,
                    **kwargs) -> Any:
    """Convenience function to get AWS resource"""
    if region:
        kwargs['region_name'] = region
    return aws_config.get_resource(service_name, profile=profile, **kwargs) 
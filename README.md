# TAI - Trading Analytics Intelligence

![Python](https://img.shields.io/badge/python-v3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Version](https://img.shields.io/badge/version-0.2.0-orange.svg)

TAI (Trading Analytics Intelligence) is a comprehensive Python library for financial data analytics, trading, and economic research. It provides unified access to multiple financial data sources, powerful analytics capabilities, and ready-to-deploy applications for financial analysis and trading strategies.

## 🚀 Features

### 📊 Data Sources Integration

- **Trading Platforms**: Alpaca, Robinhood, Interactive Brokers, Polygon
- **Economic Data**: FRED (Federal Reserve Economic Data), BLS (Bureau of Labor Statistics)
- **Financial Filings**: SEC EDGAR database integration
- **Market Data**: Treasury bonds, FOMC minutes, options chains
- **Alternative Data**: Atom Finance integration
- **AWS Data Exports**: Dynamic creation and management of AWS billing exports (CUR 2.0, FOCUS 1.0, Cost Optimization, Carbon Emissions)

### 🔧 Analytics & AI

- **Financial Analytics**: P/E ratios, fundamental analysis, technical indicators
- **Data Visualization**: Interactive Plotly charts, quick plotting utilities
- **GenAI Integration**: AWS Bedrock, OpenAI, LangChain support
- **Text-to-SQL**: Natural language to SQL query conversion
- **Options Analysis**: Options chain analysis, strategy backtesting

### 🖥️ Applications

- **Streamlit Apps**: Interactive web dashboards
- **FastAPI**: RESTful API endpoints
- **Flask**: Chart server and web applications
- **Slack Bot**: Trading alerts and data queries
- **AWS Lambda**: Serverless API deployment with Chalice

### 📈 Trading Capabilities

- **Multi-Broker Support**: Execute trades across multiple platforms
- **Options Trading**: Comprehensive options analysis and trading
- **Risk Management**: Portfolio analytics and position sizing
- **Strategy Backtesting**: Historical performance analysis

## 🛠️ Installation

### Prerequisites

- Python 3.10 or higher
- pip package manager

### Install from Source

```bash
git clone https://github.com/jasonwu001t/TAI.git
cd TAI
pip install -e .
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## 🔐 Authentication & Configuration

TAI uses a sophisticated dual-layer authentication system that supports both environment variables and configuration files, with different precedence rules depending on the service.

### Authentication Patterns

TAI implements **two distinct authentication patterns**:

#### 1. ConfigLoader Pattern (Most Services)

Uses the `ConfigLoader` class which checks environment variables first, then falls back to auth.ini:

**Precedence Rule**: `Environment Variables → auth.ini → None`

**Services**: Alpaca, Interactive Brokers, Fred, BLS, Robinhood, Redshift, AWS, OpenAI

#### 2. Direct Environment Pattern (Some Services)

Only uses environment variables, ignoring auth.ini files:

**Services**: Polygon, SEC (user-agent), Slack

### Environment Variables Setup

#### Trading Platforms

```bash
# Alpaca Markets
export ALPACA_API_KEY="your_alpaca_key"
export ALPACA_API_SECRET="your_alpaca_secret"
export ALPACA_BASE_URL="https://paper-api.alpaca.markets"

# Interactive Brokers
export IB_HOST="127.0.0.1"
export IB_PORT="7497"
export IB_CLIENT_ID="1"

# Robinhood
export ROBINHOOD_USERNAME="your_username"
export ROBINHOOD_PASSWORD="your_password"
export ROBINHOOD_PYOPT="your_2fa_secret"

# Polygon
export POLYGON_API_KEY="your_polygon_key"
```

#### Economic Data Sources

```bash
# Federal Reserve Economic Data
export FRED_API_KEY="your_fred_key"

# Bureau of Labor Statistics
export BLS_API_KEY="your_bls_key"
```

#### Database Connections

```bash
# AWS Redshift
export REDSHIFT_HOST="your_redshift_host"
export REDSHIFT_USER="your_redshift_user"
export REDSHIFT_PASSWORD="your_redshift_password"
export REDSHIFT_DATABASE="your_redshift_database"
export REDSHIFT_PORT="5439"

# AWS Services
export AWS_ACCESS_KEY_ID="your_aws_access_key_id"
export AWS_SECRET_ACCESS_KEY="your_aws_secret_access_key"
export AWS_REGION_NAME="us-east-1"
```

#### AI & Communication

```bash
# OpenAI
export OPENAI_API_KEY="your_openai_key"

# SEC EDGAR (User Agent)
export SEC_USER_AGENT="YourCompany/1.0 (your-email@example.com)"

# Slack Bot
export SLACK_BOT_TOKEN="xoxb-your-bot-token"
export SLACK_APP_TOKEN="xapp-your-app-token"
```

### Configuration File Setup

Create `TAI/utils/auth.ini` for services that support file-based configuration:

```ini
[Alpaca]
api_key = your_alpaca_key
api_secret = your_alpaca_secret
base_url = https://paper-api.alpaca.markets

[IB]
host = 127.0.0.1
port = 7497
client_id = 1

[Fred]
api_key = your_fred_key

[BLS]
api_key = your_bls_key

[Robinhood]
username = your_username
password = your_password
pyopt = your_2fa_secret

[Redshift]
host = your_redshift_host
user = your_redshift_user
password = your_redshift_password
database = your_redshift_database
port = 5439

[AWS]
aws_access_key_id = your_aws_access_key_id
aws_secret_access_key = your_aws_secret_access_key
region_name = us-east-1

[OpenAI]
api_key = your_openai_key

[Broker]
api_key = your_broker_api_key
secret_key = your_broker_secret_key

[Slack]
webhook_url = your_slack_webhook_url
bot_token = your_slack_bot_token
channel_id = your_slack_channel_id
```

### Precedence & Conflict Resolution

#### Default Behavior (ConfigLoader Services)

When both environment variables and auth.ini values exist:

1. **Environment Variable Wins**: `ALPACA_API_KEY=env_value` beats `[Alpaca] api_key = file_value`
2. **Format**: Environment variables use `{SECTION_UPPER}_{KEY_UPPER}` format
3. **Fallback**: If env var doesn't exist, auth.ini value is used
4. **Final Fallback**: If neither exists, returns `None`

**Example**:

```python
# Environment: ALPACA_API_KEY=env_key_123
# auth.ini: [Alpaca] api_key = file_key_456
# Result: Uses "env_key_123"
```

#### Override Behavior (AuthSync Utility)

Force auth.ini values to take precedence by clearing environment variables:

**Command Line (Recommended):**

```bash
# Use the comprehensive auth sync utility
python -m TAI.utils.auth_sync_run
```

**Programmatic Usage:**

```python
from TAI.utils.auth_sync import AuthSync

# This will:
# 1. Read auth.ini file
# 2. Clear existing environment variables
# 3. Set environment variables from auth.ini values
auth_sync = AuthSync()
auth_sync.sync()
```

**Important:** The `auth_sync_run.py` utility is the recommended way to sync configurations as it:

- Provides visual feedback of changes
- Generates shell scripts for persistence
- Handles all supported service types
- Shows before/after environment state

#### Direct Environment Services

These services **only** check environment variables:

- **Polygon**: `POLYGON_API_KEY`
- **SEC**: `SEC_USER_AGENT`
- **Slack**: `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`

### Configuration Templates

Use the provided templates as starting points:

```bash
# Copy and customize templates
cp TAI/utils/auth_clean.ini TAI/utils/auth.ini
# or
cp TAI/utils/auth_empty.ini TAI/utils/auth.ini
# or (for multi-account setup)
cp TAI/utils/auth_multi_account.ini TAI/utils/auth.ini

# After editing auth.ini, sync to environment variables
python -m TAI.utils.auth_sync_run
```

### Authentication Verification

Test your configuration:

**Step 1: Sync Configuration (Recommended First Step)**

```bash
# Sync auth.ini to environment variables
python -m TAI.utils.auth_sync_run
```

**Step 2: Verify Configuration Loading**

```python
from TAI.source import Alpaca, Fred, BLS
from TAI.utils import ConfigLoader
from TAI.utils.auth_sync_run import print_env_vars

# Check environment variables
print("Current TAI environment variables:")
print_env_vars()

# Test ConfigLoader pattern
config = ConfigLoader()
print(f"Alpaca API Key: {config.get_config('Alpaca', 'api_key')}")

# Test service initialization
try:
    alpaca = Alpaca()
    print("✅ Alpaca authentication successful")
    fred = Fred()
    print("✅ FRED authentication successful")
except Exception as e:
    print(f"❌ Authentication failed: {e}")
```

### Environment Variable Management

#### AuthSync Run Utility (Primary Tool)

TAI provides a comprehensive utility `auth_sync_run.py` that reads your `auth.ini` file and automatically loads all configurations into environment variables:

**Command Line Usage:**

```bash
# Run the auth sync utility
python -m TAI.utils.auth_sync_run

# Or run directly
cd TAI/utils
python auth_sync_run.py
```

**What it does:**

1. **Reads auth.ini** - Loads all configuration sections
2. **Clears existing** - Removes conflicting environment variables
3. **Sets new variables** - Exports all auth.ini values as environment variables
4. **Generates scripts** - Creates `set_env_vars.sh` and `set_env_vars.bat`
5. **Shows before/after** - Displays environment variable changes

**Supported Services:**

- `REDSHIFT_*` - Database connections
- `AWS_*` - AWS services
- `ALPACA_*` - Trading platform
- `IB_*` - Interactive Brokers
- `FRED_*` - Federal Reserve data
- `BLS_*` - Bureau of Labor Statistics
- `ROBINHOOD_*` - Robinhood trading
- `OPENAI_*` - AI services
- `SLACK_*` - Slack integration
- `BROKER_*`, `MYSQL_*`, `DYNAMODB_*` - Other services

**Example Output:**

```bash
$ python -m TAI.utils.auth_sync_run

Before sync:
Current environment variables:
AWS_ACCESS_KEY_ID=old_value

Deleting existing environment variable AWS_ACCESS_KEY_ID
Set environment variable AWS_ACCESS_KEY_ID = new_value_from_auth_ini
Set environment variable AWS_SECRET_ACCESS_KEY = secret_from_auth_ini
Set environment variable ALPACA_API_KEY = alpaca_key_from_auth_ini

After sync:
Current environment variables:
AWS_ACCESS_KEY_ID=new_value_from_auth_ini
AWS_SECRET_ACCESS_KEY=secret_from_auth_ini
ALPACA_API_KEY=alpaca_key_from_auth_ini

Run 'source set_env_vars.sh' to set environment variables in your current Unix shell session.
Run 'set_env_vars.bat' to set environment variables in your current Windows command prompt session.
```

**Generated Shell Scripts:**

```bash
# Use the generated scripts for persistent environment setup
source set_env_vars.sh    # Unix/Linux/macOS
set_env_vars.bat         # Windows
```

#### Programmatic Usage

**Option 1: Full Auth Sync (Recommended)**

```python
from TAI.utils.auth_sync_run import main

# Run complete auth sync process
main()  # Reads auth.ini, syncs to env vars, generates scripts
```

**Option 2: Manual Auth Sync**

```python
from TAI.utils.auth_sync import AuthSync

auth_sync = AuthSync()
auth_sync.sync()  # Just sync without script generation
```

**Option 3: Check Environment Variables**

```python
from TAI.utils.auth_sync_run import print_env_vars

# Display all TAI-related environment variables
print_env_vars()
```

#### Workflow Integration

**Development Workflow:**

```bash
# 1. Update your auth.ini file with credentials
vim TAI/utils/auth.ini

# 2. Sync to environment variables
python -m TAI.utils.auth_sync_run

# 3. Source the generated script (optional for persistence)
source set_env_vars.sh

# 4. Run your TAI applications
python your_tai_script.py
```

**CI/CD Integration:**

```bash
# In your CI/CD pipeline
export TAI_CONFIG_PATH="/path/to/auth.ini"
python -m TAI.utils.auth_sync_run
# Now all TAI services will use the synced environment variables
```

### Security Best Practices

1. **Never commit auth.ini**: Add to `.gitignore` (already included)
2. **Use environment variables in production**: More secure than config files
3. **Rotate API keys regularly**: Especially for production environments
4. **Use paper trading initially**: Set `ALPACA_BASE_URL` to paper trading endpoint
5. **Validate user agents**: SEC requires proper user agent format
6. **Limit permissions**: Use read-only API keys when possible

### Troubleshooting

#### Common Issues

**Authentication Failed**:

```python
# Check if credentials are loaded
from TAI.utils import ConfigLoader
config = ConfigLoader()
print(config.get_config('Alpaca', 'api_key'))  # Should not be None
```

**Environment vs File Conflicts**:

```bash
# Check current environment variables
env | grep -E "(ALPACA_|FRED_|BLS_|IB_)"

# Clear conflicting environment variables
unset ALPACA_API_KEY
# Or use AuthSync to sync from file
```

**Service-Specific Issues**:

- **SEC**: Requires proper user agent format with email
- **Polygon**: Only uses environment variables
- **Slack**: Requires both BOT_TOKEN and APP_TOKEN
- **IB**: Requires TWS/Gateway to be running

#### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)
# Shows detailed authentication attempts
```

### Multi-Account AWS Management

For organizations using multiple AWS accounts, TAI provides sophisticated multi-account support with several approaches:

#### 1. AWS Profiles Approach (Most Recommended)

**Setup AWS Profiles:**

```bash
# Configure multiple AWS profiles
aws configure --profile dev-account
aws configure --profile staging-account
aws configure --profile prod-account
aws configure --profile client-a
```

**Usage:**

```python
from TAI.utils import get_aws_client, AWSConfigManager

# Method 1: Direct profile usage
s3_dev = get_aws_client('s3', profile='dev-account')
s3_prod = get_aws_client('s3', profile='prod-account')

# Method 2: Manager with default profile
dev_manager = AWSConfigManager(default_profile='dev-account')
s3 = dev_manager.get_client('s3')

# Method 3: Runtime account switching
accounts = {
    'development': 'dev-account',
    'production': 'prod-account'
}

for env_name, profile in accounts.items():
    s3 = get_aws_client('s3', profile=profile)
    bedrock = get_aws_client('bedrock-runtime', profile=profile, region='us-west-2')
    print(f"Connected to {env_name}")
```

#### 2. Environment-Based Multi-Account

**Setup Environment Variables:**

```bash
# Development Account
export DEV_AWS_ACCESS_KEY_ID="AKIA...DEV"
export DEV_AWS_SECRET_ACCESS_KEY="..."
export DEV_AWS_DEFAULT_REGION="us-east-1"

# Production Account
export PROD_AWS_ACCESS_KEY_ID="AKIA...PROD"
export PROD_AWS_SECRET_ACCESS_KEY="..."
export PROD_AWS_DEFAULT_REGION="us-west-2"
```

**Usage:**

```python
from TAI.utils import AWSConfigManager, AWSAccountConfig
import os

# Create account configurations
dev_config = AWSAccountConfig(
    access_key_id=os.getenv('DEV_AWS_ACCESS_KEY_ID'),
    secret_access_key=os.getenv('DEV_AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1',
    account_alias='dev'
)

prod_config = AWSAccountConfig(
    access_key_id=os.getenv('PROD_AWS_ACCESS_KEY_ID'),
    secret_access_key=os.getenv('PROD_AWS_SECRET_ACCESS_KEY'),
    region_name='us-west-2',
    account_alias='prod'
)

# Use specific account
manager = AWSConfigManager()
s3_dev = manager.get_client('s3', account_config=dev_config)
s3_prod = manager.get_client('s3', account_config=prod_config)
```

#### 3. Enhanced Configuration File

**Create `TAI/utils/auth.ini` with multiple AWS sections:**

```ini
[AWS_DEV]
aws_access_key_id = your_dev_aws_access_key_id
aws_secret_access_key = your_dev_aws_secret_access_key
region_name = us-east-1

[AWS_PROD]
aws_access_key_id = your_prod_aws_access_key_id
aws_secret_access_key = your_prod_aws_secret_access_key
region_name = us-west-2

[AWS_CLIENT_A]
aws_access_key_id = your_client_a_aws_access_key_id
aws_secret_access_key = your_client_a_aws_secret_access_key
region_name = eu-west-1
```

**Usage:**

```python
from TAI.utils import ConfigLoader, AWSAccountConfig, AWSConfigManager

config = ConfigLoader()

# Load dev account config
dev_config = AWSAccountConfig(
    access_key_id=config.get_config('AWS_DEV', 'aws_access_key_id'),
    secret_access_key=config.get_config('AWS_DEV', 'aws_secret_access_key'),
    region_name=config.get_config('AWS_DEV', 'region_name'),
    account_alias='dev'
)

manager = AWSConfigManager()
s3_dev = manager.get_client('s3', account_config=dev_config)
```

#### 4. Multi-Account DataMaster

**Enhanced DataMaster for cross-account operations:**

```python
from TAI.utils.aws_multi_account_examples import MultiAccountDataMaster

# Initialize with default account
dm = MultiAccountDataMaster(default_profile='dev-account')

# Load data from dev account
data = dm.load_s3_multi_account(
    bucket_name='dev-bucket',
    s3_directory='data/',
    profile='dev-account'
)

# Switch to production account and save
dm.set_aws_account(profile='prod-account')
dm.save_s3_multi_account(
    data,
    bucket_name='prod-bucket',
    s3_directory='processed/'
)
```

#### 5. AWS STS Role Assumption

**Cross-account access using IAM roles:**

```python
from TAI.utils import AWSConfigManager

manager = AWSConfigManager(default_profile='management-account')

# Assume role in target account
temp_config = manager.assume_role(
    role_arn='arn:aws:iam::111111111111:role/DevAccountAccess',
    session_name='TAI-Dev-Session',
    duration_seconds=3600
)

# Use temporary credentials
s3 = manager.get_client('s3', account_config=temp_config)
```

#### 6. Account Validation and Management

**Validate and inspect accounts:**

```python
from TAI.utils import AWSConfigManager

manager = AWSConfigManager()

# List available profiles
profiles = manager.list_available_profiles()
print(f"Available profiles: {profiles}")

# Validate credentials
for profile in ['dev-account', 'prod-account']:
    is_valid = manager.validate_credentials(profile=profile)
    if is_valid:
        account_info = manager.get_account_info(profile=profile)
        print(f"{profile}: Account {account_info['Account']}")
```

#### Quick Start Templates

Use the provided templates:

```bash
# Multi-account template
cp TAI/utils/auth_multi_account.ini TAI/utils/auth.ini

# Basic template
cp TAI/utils/auth_clean.ini TAI/utils/auth.ini
```

#### Best Practices for Multi-Account

1. **Use AWS Profiles for Development**: Easier credential management
2. **Use IAM Roles for Production**: More secure than access keys
3. **Environment Separation**: Different accounts for dev/staging/prod
4. **Least Privilege**: Minimal permissions per account
5. **Credential Rotation**: Regular key rotation policies
6. **Session Caching**: TAI automatically caches sessions for performance
7. **Account Validation**: Always validate credentials before operations

## 🎯 Quick Start

### Basic Usage

```python
from TAI.source import Alpaca, Fred, BLS, SEC
from TAI.analytics import DataAnalytics, QuickPlot

# Initialize data sources
alpaca = Alpaca()
fred = Fred()
bls = BLS()
sec = SEC(user_agent="your-app/1.0")

# Get stock data
stock_data = alpaca.get_stock_historical('AAPL', lookback_period='30D')
print(stock_data.head())

# Get economic data
gdp_data = fred.get_series('gdp')
unemployment = bls.fetch_bls_data(['UNRATE'])

# Get company fundamentals
company_data = sec.get_financial_data('AAPL', metrics_to_retrieve=['revenue', 'net_income'])

# Create quick visualizations
plotter = QuickPlot(dataframes=[stock_data], labels=['AAPL'])
fig = plotter.plot_line(title='AAPL Stock Price')
fig.show()
```

### Options Trading

```python
from TAI.source.alpaca import Alpaca

alpaca = Alpaca()

# Get options chain
options_chain = alpaca.get_option_chain(
    underlying_symbol='SPY',
    strike_price_gte=400,
    strike_price_lte=450,
    expiration_date='2024-12-20'
)

# Analyze options
option_bet = alpaca.OptionBet('SPY', '2024-12-20', '30D')
analysis = option_bet.describe_perc_change()
print(analysis)
```

### Economic Data Analysis

```python
from TAI.source import Fred, BLS

# Federal Reserve Economic Data
fred = Fred()
inflation = fred.get_series('consumer_price_index')
fed_rate = fred.get_series('federal_funds_rate')

# Bureau of Labor Statistics
bls = BLS()
labor_data = bls.fetch_bls_data(['UNRATE', 'PAYEMS'])  # Unemployment rate, Non-farm payrolls
```

## 🌐 Web Applications

### Streamlit Dashboard

```python
from TAI.app import stframe

# Run Streamlit app
app = stframe()
if app.login():
    app.show_main_page()
```

### FastAPI Server

```python
from TAI.app import FastAPIApp
import uvicorn

app = FastAPIApp()
uvicorn.run(app.app, host="0.0.0.0", port=8000)

# Access endpoints:
# GET /data/{series_code} - Get FRED economic data
# GET /plot/{series_code} - Get interactive plots
```

### Slack Bot Integration

```python
from TAI.app import SlackApp

slack_app = SlackApp()
# Configure your Slack bot token and run
```

## 📊 Data Sources Details

### Alpaca Markets

- **Stock Data**: Historical and real-time stock prices
- **Options Data**: Options chains, Greeks, implied volatility
- **Trading**: Paper and live trading capabilities
- **Market Data**: Latest quotes, trades, and market snapshots

### Federal Reserve Economic Data (FRED)

- **Predefined Series**: GDP, CPI, Federal Funds Rate, M2 Money Supply
- **Custom Queries**: Search and retrieve any FRED series
- **Historical Data**: Long-term economic indicators

### Bureau of Labor Statistics (BLS)

- **Employment Data**: Unemployment rates, job openings
- **Price Data**: Consumer Price Index, Producer Price Index
- **Productivity Data**: Labor productivity metrics

### SEC EDGAR Database

- **Company Filings**: 10-K, 10-Q, 8-K filings
- **Financial Metrics**: Revenue, earnings, balance sheet items
- **Fundamental Analysis**: P/E ratios, financial ratios

### Interactive Brokers

- **Global Markets**: Stocks, options, futures, forex
- **Real-time Data**: Level 1 and Level 2 market data
- **Order Management**: Complex order types and strategies

### AWS Data Exports

TAI provides comprehensive support for AWS Data Export creation and management, enabling dynamic export generation for all supported AWS export types.

#### Supported Export Types

- **CUR 2.0**: Cost and Usage Report 2.0 with detailed billing data
- **FOCUS 1.0**: FinOps Open Cost and Usage Specification with AWS columns
- **Cost Optimization Recommendations**: Right-sizing and cost optimization insights
- **Carbon Emissions**: Environmental impact and carbon footprint data

#### Quick Start

```python
from TAI.data.master import DataMaster

# Initialize DataMaster
dm = DataMaster()

# Create a CUR 2.0 export
result = dm.create_data_export(
    export_name='my-cur-export',
    export_type='CUR_2_0',
    s3_bucket='my-billing-bucket',
    s3_prefix='cur-data/',
    data_query_format='PARQUET'
)

# Create a FOCUS 1.0 export
result = dm.create_data_export(
    export_name='my-focus-export',
    export_type='FOCUS_1_0',
    s3_bucket='my-billing-bucket',
    s3_prefix='focus-data/',
    data_query_format='PARQUET'
)

# Create cost optimization recommendations export
result = dm.create_data_export(
    export_name='my-cost-optimization',
    export_type='COST_OPTIMIZATION_RECOMMENDATIONS',
    s3_bucket='my-billing-bucket',
    s3_prefix='cost-optimization/',
    data_query_format='PARQUET'
)

# Create carbon emissions export
result = dm.create_data_export(
    export_name='my-carbon-export',
    export_type='CARBON_EMISSIONS',
    s3_bucket='my-billing-bucket',
    s3_prefix='carbon-data/',
    data_query_format='PARQUET'
)
```

#### Advanced Configuration

```python
# Get supported export types
supported_types = dm.get_supported_export_types()
print(f"Supported export types: {supported_types}")

# List existing exports
existing_exports = dm.list_data_exports()
print(f"Found {len(existing_exports)} existing exports")

# Create export with custom configuration
result = dm.create_data_export(
    export_name='advanced-cur-export',
    export_type='CUR_2_0',
    s3_bucket='my-billing-bucket',
    s3_prefix='advanced-cur/',
    data_query_format='PARQUET',
    data_query_frequency='SYNCHRONOUS',
    data_query_table_configurations={
        'TIME_GRANULARITY': 'HOURLY',
        'INCLUDE_RESOURCES': 'TRUE',
        'INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY': 'TRUE'
    }
)
```

#### Bulk Export Creation

```python
# Create multiple exports at once
export_configs = [
    {
        'export_name': 'production-cur',
        'export_type': 'CUR_2_0',
        's3_bucket': 'prod-billing-bucket',
        's3_prefix': 'cur/',
        'data_query_format': 'PARQUET'
    },
    {
        'export_name': 'production-focus',
        'export_type': 'FOCUS_1_0',
        's3_bucket': 'prod-billing-bucket',
        's3_prefix': 'focus/',
        'data_query_format': 'PARQUET'
    }
]

results = []
for config in export_configs:
    result = dm.create_data_export(**config)
    results.append(result)
```

#### Export Validation and Testing

```python
# Test export creation capabilities
from TAI.data.test_create_data_exports import DataExportsTestSuite

# Initialize test suite
test_suite = DataExportsTestSuite()

# Run comprehensive tests
test_suite.run_all_tests()

# Test specific export type
test_suite.test_cur_2_0_export()

# Test with custom configuration
test_suite.test_focus_1_0_export()
```

#### Troubleshooting

**Common Issues:**

1. **Invalid S3 Bucket**: Ensure the S3 bucket exists and is accessible
2. **Region Mismatch**: Verify S3 bucket region matches your AWS configuration
3. **Format Validation**: Use supported formats (PARQUET, TEXT_OR_CSV)
4. **Permission Errors**: Ensure proper IAM permissions for billing data exports

**Debug Mode:**

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable debug logging
dm = DataMaster()
result = dm.create_data_export(
    export_name='debug-export',
    export_type='CUR_2_0',
    s3_bucket='my-bucket',
    s3_prefix='debug/',
    data_query_format='PARQUET'
)
```

#### Features

- **Dynamic Configuration**: Supports all AWS Data Export types with intelligent defaults
- **Smart S3 Discovery**: Automatically discovers S3 buckets from existing exports
- **Validation**: Comprehensive parameter validation before export creation
- **Backward Compatibility**: Maintains compatibility with existing CUR creation methods
- **Error Handling**: Detailed error reporting and troubleshooting guidance
- **Testing Suite**: Comprehensive test suite for validation and development

## 🧪 Testing

Run the test suite:

```bash
pytest TAI/test/
```

Individual test files:

```bash
pytest TAI/test/test_alpaca.py
pytest TAI/test/test_fred.py
pytest TAI/test/test_bls.py
```

### AWS Data Export Testing

Test the AWS Data Export functionality:

```bash
# Run comprehensive AWS Data Export tests
python TAI/data/test_create_data_exports.py

# Run step-by-step verification test
python TAI/data/step_by_step_test.py

# Debug existing exports
python TAI/data/debug_existing_export.py
```

## 📁 Project Structure

```
TAI/
├── TAI/                    # Main package
│   ├── analytics/          # Data analysis and visualization
│   ├── app/               # Web applications (Streamlit, FastAPI, Flask)
│   ├── data/              # Data storage and management
│   │   ├── master.py      # Core data management (includes AWS Data Exports)
│   │   ├── test_create_data_exports.py  # AWS Data Export testing suite
│   │   ├── step_by_step_test.py         # Step-by-step export verification
│   │   └── debug_existing_export.py     # Export debugging utilities
│   ├── genai/             # AI/ML integrations
│   ├── source/            # Data source integrations
│   ├── test/              # Unit tests
│   └── utils/             # Utilities and configuration
├── projects/              # Standalone projects
│   ├── chalice_taiapi/    # AWS Lambda API
│   ├── forecast/          # Forecasting models
│   └── streamlit_app/     # Streamlit applications
├── requirements.txt       # Dependencies
└── setup.py              # Package configuration
```

## 🚀 Deployment Options

### AWS Lambda (Chalice)

```bash
cd projects/chalice_taiapi
chalice deploy
```

### Streamlit Cloud

```bash
streamlit run TAI/app/streamlit_app.py
```

### Docker Deployment

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install -e .
CMD ["python", "-m", "TAI.app.fastapi_app"]
```

## 🔍 Use Cases

### Quantitative Research

- **Market Analysis**: Analyze market trends using multiple data sources
- **Economic Research**: Correlate economic indicators with market performance
- **Risk Management**: Portfolio optimization and risk assessment

### Algorithmic Trading

- **Strategy Development**: Backtest trading strategies
- **Options Strategies**: Implement complex options strategies
- **Real-time Trading**: Execute trades based on market signals

### Financial Reporting

- **Company Analysis**: Generate comprehensive company reports
- **Economic Dashboards**: Create interactive economic dashboards
- **Regulatory Reporting**: Automate compliance reporting

### AWS Cost Management

- **Billing Analytics**: Automated AWS Cost and Usage Report (CUR 2.0) generation
- **Cost Optimization**: Generate cost optimization recommendations exports
- **FinOps Compliance**: Create FOCUS 1.0 standard billing exports
- **Carbon Footprint**: Track and export environmental impact data
- **Multi-Account Management**: Unified billing data across AWS accounts

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📋 Requirements

### Core Dependencies

- **Data Processing**: pandas, numpy, scipy, polars
- **Visualization**: plotly, matplotlib
- **APIs**: requests, aiohttp, fastapi, flask, streamlit
- **Trading**: alpaca-py, robin_stocks, ib_insync
- **Economic Data**: fredapi
- **AI/ML**: langchain, openai, boto3
- **Database**: psycopg2-binary, mysql-connector-python

### Optional Dependencies

- **Deep Learning**: torch, torchvision, sentence_transformers (commented out)
- **Additional Analysis**: statsmodels, lxml

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Federal Reserve Bank of St. Louis for FRED API
- Bureau of Labor Statistics for economic data
- SEC for EDGAR database access
- Alpaca Markets for trading API
- All the open-source contributors whose libraries make this project possible

## 📞 Support

For support, please open an issue on GitHub or contact the maintainers.

---

**Disclaimer**: This software is for educational and research purposes. Trading and investment involve risk of loss. Use at your own risk and ensure compliance with applicable regulations.

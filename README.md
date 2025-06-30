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

## ⚙️ Configuration

TAI uses a flexible configuration system that supports both environment variables and configuration files.

### Environment Variables

Set API keys and credentials using environment variables:

```bash
export ALPACA_API_KEY="your_alpaca_key"
export ALPACA_API_SECRET="your_alpaca_secret"
export ALPACA_BASE_URL="https://paper-api.alpaca.markets"
export FRED_API_KEY="your_fred_key"
export BLS_API_KEY="your_bls_key"
export OPENAI_API_KEY="your_openai_key"
```

### Configuration File

Create `TAI/utils/auth.ini`:

```ini
[Alpaca]
api_key = your_alpaca_key
api_secret = your_alpaca_secret
base_url = https://paper-api.alpaca.markets

[Fred]
api_key = your_fred_key

[BLS]
api_key = your_bls_key

[Robinhood]
username = your_username
password = your_password
pyopt = your_2fa_secret

[OpenAI]
api_key = your_openai_key
```

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

## 📁 Project Structure

```
TAI/
├── TAI/                    # Main package
│   ├── analytics/          # Data analysis and visualization
│   ├── app/               # Web applications (Streamlit, FastAPI, Flask)
│   ├── data/              # Data storage and management
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

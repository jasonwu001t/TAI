# PE Scanner - Detecting Oversold Stocks Using Derived PE Ratios

A comprehensive stock analysis tool that leverages the TAI library to identify potentially undervalued stocks using advanced PE ratio analysis, based on the methodology from [this Medium article](https://medium.com/@jwu8715/detecting-oversold-stocks-using-derived-price-to-earnings-pe-ratios-223fa490dc8).

## 🚀 Features

### **Core Capabilities**

- **SEC Data Integration**: Uses your enhanced SEC module for comprehensive fundamental analysis
- **Real-time Market Data**: Alpaca API integration for current stock prices
- **Advanced PE Analysis**: Multiple PE ratios (current, TTM, forward, historical)
- **Smart Screening**: Configurable criteria for identifying investment opportunities
- **Jupyter-Ready**: Modular design perfect for notebook-based analysis

### **Key Metrics Calculated**

- Current PE Ratio (Basic & Diluted)
- Trailing Twelve Months (TTM) PE
- Forward PE (projected)
- PEG Ratio (Price/Earnings to Growth)
- Historical PE Statistics (mean, median, percentiles)
- Relative Valuation Scores

## 📋 Quick Start

### **1. Basic Usage**

```python
from pe_scanner import PEScanner

# Initialize scanner
scanner = PEScanner()

# Analyze a single stock
pe_data = scanner.calculate_pe_metrics("AAPL")

# Generate detailed report
report = scanner.generate_analysis_report("AAPL")
print(report)
```

### **2. Multi-Stock Screening**

```python
# Screen multiple stocks
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
results = scanner.screen_stocks(tickers)

# View undervalued candidates
undervalued = results[results['potentially_undervalued'] == True]
print(undervalued[['ticker', 'current_pe', 'pe_relative_score']])
```

### **3. Custom Screening Criteria**

```python
# Define custom criteria
custom_criteria = {
    'max_pe': 20,              # Maximum PE ratio
    'min_pe': 8,               # Minimum PE ratio
    'require_positive_eps': True,
    'max_peg_ratio': 1.5,      # Maximum PEG ratio
    'undervalued_threshold': 0.8  # 20% below historical average
}

results = scanner.screen_stocks(tickers, custom_criteria)
```

## 🧪 Jupyter Notebook Usage

The PE Scanner is designed with modular blocks perfect for Jupyter notebooks:

### **Cell 1: Initialize**

```python
from pe_scanner import block_1_initialize_scanner
scanner = block_1_initialize_scanner()
```

### **Cell 2: Single Stock Analysis**

```python
from pe_scanner import block_2_analyze_single_stock
pe_data = block_2_analyze_single_stock(scanner, "AAPL")
```

### **Cell 3: Multi-Stock Screening**

```python
from pe_scanner import block_3_screen_multiple_stocks
results = block_3_screen_multiple_stocks(scanner)
```

### **Cell 4: Custom Analysis**

```python
from pe_scanner import block_4_custom_analysis
comparison = block_4_custom_analysis(scanner)
```

## 📊 Screening Strategies

### **Value Investing Strategy**

```python
value_criteria = {
    'max_pe': 15,
    'min_pe': 8,
    'require_positive_eps': True,
    'max_peg_ratio': 1.0,
    'undervalued_threshold': 0.7
}
```

### **Growth at Reasonable Price (GARP)**

```python
garp_criteria = {
    'max_pe': 25,
    'min_pe': 10,
    'require_positive_eps': True,
    'max_peg_ratio': 1.5,
    'undervalued_threshold': 0.85
}
```

### **Momentum Strategy**

```python
momentum_criteria = {
    'max_pe': 35,
    'min_pe': 15,
    'require_positive_eps': True,
    'max_peg_ratio': 2.0,
    'undervalued_threshold': 0.9
}
```

## 🔧 Configuration

### **Scanner Initialization Options**

```python
scanner = PEScanner(
    use_cache=True,     # Enable SEC data caching
    cache_ttl=3600      # Cache time-to-live (1 hour)
)
```

### **TAI Library Requirements**

- **SEC Module**: For fundamental data (EPS, financials)
- **Alpaca Module**: For real-time market data
- **Valid API Credentials**: Configured in TAI auth system

## 📈 Analysis Output

### **PE Metrics Structure**

```python
{
    'ticker': 'AAPL',
    'current_price': 175.43,
    'company_name': 'Apple Inc.',
    'pe_ratios': {
        'current_pe_basic': 28.5,
        'ttm_pe': 27.8,
        'forward_pe': 24.2
    },
    'valuation_scores': {
        'peg_ratio': 1.2,
        'pe_relative_score': 0.85,
        'is_undervalued_pe': False
    },
    'historical_analysis': {
        'avg_pe': 22.4,
        'median_pe': 21.8,
        'pe_percentile_25': 18.5,
        'pe_percentile_75': 26.2
    }
}
```

### **Screening Results DataFrame**

| ticker | current_pe | ttm_pe | peg_ratio | passes_screen | potentially_undervalued |
| ------ | ---------- | ------ | --------- | ------------- | ----------------------- |
| AAPL   | 28.5       | 27.8   | 1.2       | True          | False                   |
| MSFT   | 32.1       | 31.4   | 1.8       | False         | False                   |
| GOOGL  | 24.3       | 23.9   | 1.1       | True          | True                    |

## 🎯 Investment Signals

### **Undervalued Indicators**

- **PE Relative Score < 0.8**: Trading 20% below historical average
- **PEG Ratio < 1.0**: Growth rate exceeds PE ratio
- **Current PE < Historical 25th Percentile**: In lower valuation range

### **Quality Filters**

- **Positive EPS Required**: Profitable companies only
- **PE Range Validation**: Avoid extreme values (too high/low)
- **Growth Consistency**: Stable earnings growth patterns

## 🔍 Example Analysis Workflows

### **Daily Screening Routine**

```python
# 1. Screen S&P 500 subset
large_caps = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA']
results = scanner.screen_stocks(large_caps)

# 2. Identify opportunities
undervalued = results[results['potentially_undervalued']]
print(f"Found {len(undervalued)} undervalued stocks")

# 3. Deep dive on top candidates
for ticker in undervalued['ticker'].head(3):
    report = scanner.generate_analysis_report(ticker)
    print(report)
```

### **Sector Rotation Analysis**

```python
sectors = {
    'Tech': ['AAPL', 'MSFT', 'GOOGL'],
    'Finance': ['JPM', 'BAC', 'WFC'],
    'Healthcare': ['JNJ', 'PFE', 'UNH']
}

for sector, tickers in sectors.items():
    results = scanner.screen_stocks(tickers)
    avg_pe = results['current_pe'].mean()
    print(f"{sector} average PE: {avg_pe:.2f}")
```

## ⚠️ Important Notes

### **Data Dependencies**

- Requires valid TAI library configuration
- SEC API rate limiting (10 requests/second) automatically handled
- Alpaca API credentials needed for real-time data

### **Analysis Limitations**

- PE ratios can be misleading for cyclical businesses
- Does not account for debt levels or cash positions
- Historical PE may not predict future performance
- Requires manual review of sector-specific considerations

## 🛠️ Customization

### **Adding New Metrics**

Extend the `_calculate_valuation_scores` method to add custom scoring:

```python
def custom_score_calculation(self, pe_metrics):
    # Add your custom scoring logic here
    custom_score = pe_metrics['current_pe'] / industry_average_pe
    return custom_score
```

### **Custom Screening Criteria**

Create industry-specific screening functions:

```python
def tech_stock_criteria():
    return {
        'max_pe': 35,
        'min_pe': 15,
        'max_peg_ratio': 2.0,
        'undervalued_threshold': 0.9
    }
```

## 📚 Additional Resources

- [Medium Article](https://medium.com/@jwu8715/detecting-oversold-stocks-using-derived-price-to-earnings-pe-ratios-223fa490dc8): Original methodology
- **TAI Library Documentation**: For SEC and Alpaca API details
- **Usage Examples**: See `pe_scanner_usage_examples.py` for comprehensive examples

---

**Ready to find your next investment opportunity? Start with the Jupyter notebook blocks and customize for your strategy!** 🚀

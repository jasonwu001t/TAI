# quantstats_plotly/examples/example_usage.py

from quantstats import *

# Load sample data
data = load_data('sample_data.csv')

# Prepare returns
strategy_returns = prepare_returns(data['Strategy'])
benchmark_returns = prepare_returns(data['Benchmark'])

# Generate full report
generate_report(strategy_returns, strategy_returns.cumsum(), benchmark_returns, output_path='quantstats_report.html')

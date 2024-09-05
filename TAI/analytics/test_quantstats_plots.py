# test_quantstats_plots.py

import pandas as pd
import numpy as np
from data_analytics import DataAnalytics
from plotly_plots import QuantStatsPlot

# Step 1: Generate sample data (Strategy and Benchmark prices)
def generate_sample_data(start='2020-01-01', end='2023-01-01'):
    # Create a date range
    dates = pd.date_range(start=start, end=end, freq='B')  # Business days only

    # Simulate random daily price changes (using a geometric Brownian motion model)
    np.random.seed(42)
    price_changes = np.random.normal(0.0005, 0.02, len(dates))  # Daily returns with small drift

    # Generate prices starting at 100 for both Strategy and Benchmark
    strategy_prices = 100 * np.cumprod(1 + price_changes)
    benchmark_prices = 100 * np.cumprod(1 + price_changes + 0.001)  # Slightly higher drift for benchmark

    # Create a DataFrame with both prices
    data = pd.DataFrame({'Strategy': strategy_prices, 'Benchmark': benchmark_prices}, index=dates)
    return data

# Step 2: Load sample data and generate returns
stats = DataAnalytics()
plotter = QuantStatsPlot()

sample_data = generate_sample_data()
strategy_returns = stats.prepare_returns(sample_data['Strategy'])
benchmark_returns = stats.prepare_returns(sample_data['Benchmark'])

print ('sample data: ', sample_data)
print ('strategy return ', strategy_returns)
print ('benchmark return ', benchmark_returns)

# Step 3: Test statistical methods
print(f"Average Return: {stats.avg_return(strategy_returns):.4f}")
print(f"Average Loss: {stats.avg_loss(strategy_returns):.4f}")
print(f"Sharpe Ratio: {stats.sharpe(strategy_returns):.4f}")
print(f"Max Drawdown: {stats.max_drawdown(strategy_returns.cumsum()):.4f}")
print(f"Sortino Ratio: {stats.sortino(strategy_returns):.4f}")

# Step 4: Test all plots
# Cumulative Returns Plot (Linear and Log Scale)
cumulative_returns_fig, cumulative_returns_log_fig = plotter.plot_cumulative_returns(strategy_returns, benchmark_returns)
cumulative_returns_fig.show()
cumulative_returns_log_fig.show()

# Volatility Matched Returns Plot
volatility_matched_fig = plotter.plot_volatility_matched_returns(strategy_returns, benchmark_returns)
volatility_matched_fig.show()

# EOY Returns Plot
eoy_returns_fig = plotter.plot_eoy_returns(strategy_returns, benchmark_returns)
eoy_returns_fig.show()

# Key Performance Metrics Table
performance_metrics_fig = plotter.key_performance_metrics(strategy_returns, benchmark_returns)
performance_metrics_fig.show()

# Distribution of Monthly Returns
distribution_fig = plotter.plot_distribution_of_monthly_returns(strategy_returns)
distribution_fig.show()

# Daily Returns Plot
daily_returns_fig = plotter.plot_daily_returns(strategy_returns)
daily_returns_fig.show()

# Rolling Beta (6-Months) Plot
rolling_beta_fig = plotter.plot_rolling_beta(strategy_returns, benchmark_returns)
rolling_beta_fig.show()

# Rolling Volatility (6-Months) Plot
rolling_volatility_fig = plotter.plot_rolling_volatility(strategy_returns)
rolling_volatility_fig.show()

# Rolling Sharpe (6-Months) Plot
rolling_sharpe_fig = plotter.plot_rolling_sharpe(strategy_returns)
rolling_sharpe_fig.show()

# Rolling Sortino (6-Months) Plot
rolling_sortino_fig = plotter.plot_rolling_sortino(strategy_returns)
rolling_sortino_fig.show()

# Top 5 Drawdown Periods Plot
top_5_drawdowns_fig = plotter.plot_top_5_drawdowns(strategy_returns.cumsum())
top_5_drawdowns_fig.show()

# Underwater Plot
underwater_fig = plotter.plot_underwater(strategy_returns.cumsum())
underwater_fig.show()

# Monthly Returns Heatmap Plot
monthly_heatmap_fig = plotter.plot_monthly_returns_heatmap(strategy_returns)
monthly_heatmap_fig.show()

# Return Quantiles Plot
return_quantiles_fig = plotter.plot_return_quantiles(strategy_returns)
return_quantiles_fig.show()

# Step 5: Generate full HTML report
plotter.generate_report(strategy_returns, strategy_returns.cumsum(), benchmark_returns, output_path='quantstats_report.html')

# Test Output
print("All methods and plots tested successfully. Report generated as 'quantstats_report.html'.")

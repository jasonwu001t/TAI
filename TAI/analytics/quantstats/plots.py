# quantstats_plotly/plots.py

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import calendar
from .stats import *
import copy

# General layout improvement function
def update_layout(fig, title, xaxis_title, yaxis_title):
    fig.update_layout(
        title={
            'text': title,
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20, 'family': 'Arial'}
        },
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        font=dict(family='Arial', size=12),
        paper_bgcolor='white',
        plot_bgcolor='white',
        hovermode="x",
        xaxis=dict(showgrid=False, zeroline=False, linecolor='black'),
        yaxis=dict(showgrid=True, gridcolor='lightgray', zeroline=False, linecolor='black'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255, 255, 255, 0.5)",
            bordercolor="Black",
            borderwidth=1
        )
    )

# Reusable function to create line plots
def create_line_plot(data, name, color, title, xaxis_title, yaxis_title):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data.index, y=data, mode='lines', name=name, line=dict(width=2, color=color)))
    update_layout(fig, title, xaxis_title, yaxis_title)
    return fig

# Reusable function for bar plots
def create_bar_plot(x_data, y_data, name, color, title, xaxis_title, yaxis_title):
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x_data, y=y_data, name=name, marker_color=color))
    update_layout(fig, title, xaxis_title, yaxis_title)
    return fig

# Rolling Sharpe Ratio Plot
def plot_rolling_sharpe(strategy_returns, window=126, risk_free_rate=0):
    rolling_sharpe = (strategy_returns.rolling(window).mean() - risk_free_rate) / strategy_returns.rolling(window).std()
    return create_line_plot(rolling_sharpe, f'Rolling {window}-Day Sharpe', '#d62728', f'Rolling Sharpe Ratio ({window}-Day)', 'Date', 'Sharpe Ratio')

# Rolling Sortino Ratio Plot
def plot_rolling_sortino(strategy_returns, window=126, risk_free_rate=0):
    downside_risk = strategy_returns[strategy_returns < risk_free_rate].rolling(window).std()
    rolling_sortino = (strategy_returns.rolling(window).mean() - risk_free_rate) / downside_risk
    return create_line_plot(rolling_sortino, f'Rolling {window}-Day Sortino', '#9467bd', f'Rolling Sortino Ratio ({window}-Day)', 'Date', 'Sortino Ratio')

# Underwater Plot
def plot_underwater(drawdown_series):
    return create_line_plot(drawdown_series, 'Drawdown', '#1f77b4', 'Underwater Plot (Cumulative Drawdowns)', 'Date', 'Drawdown')

# Rolling Volatility Plot
def plot_rolling_volatility(strategy_returns, window=126):
    rolling_volatility = strategy_returns.rolling(window).std()
    return create_line_plot(rolling_volatility, f'Rolling {window}-Day Volatility', '#2ca02c', f'Rolling Volatility ({window}-Day)', 'Date', 'Volatility')

# Daily Returns Plot
def plot_daily_returns(strategy_returns):
    return create_line_plot(strategy_returns, 'Daily Returns', '#1f77b4', 'Daily Returns', 'Date', 'Returns')

# Distribution of Monthly Returns
def plot_distribution_of_monthly_returns(strategy_returns):
    monthly_returns = strategy_returns.resample('M').apply(comp)
    fig = px.histogram(monthly_returns, nbins=50)
    update_layout(fig, 'Distribution of Monthly Returns', 'Monthly Returns', 'Frequency')
    return fig

# Key Performance Metrics Table
def key_performance_metrics(strategy_returns, benchmark_returns):
    data = {
        "Metric": ["Sharpe Ratio", "Sortino Ratio", "CAGR", "Max Drawdown"],
        "Strategy": [sharpe(strategy_returns), sortino(strategy_returns), cagr(strategy_returns), max_drawdown(strategy_returns.cumsum())],
        "Benchmark": [sharpe(benchmark_returns), sortino(benchmark_returns), cagr(benchmark_returns), max_drawdown(benchmark_returns.cumsum())]
    }
    df = pd.DataFrame(data)
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns), fill_color='paleturquoise', align='left', font=dict(size=12)),
        cells=dict(values=[df.Metric, df.Strategy, df.Benchmark], fill_color='lavender', align='left', font=dict(size=12))
    )])
    fig.update_layout(title='Key Performance Metrics')
    return fig

# EOY Returns Plot
def plot_eoy_returns(strategy_returns, benchmark_returns):
    strategy_eoy = strategy_returns.resample('Y').apply(comp)
    benchmark_eoy = benchmark_returns.resample('Y').apply(comp)
    fig = create_bar_plot(strategy_eoy.index.year, strategy_eoy, 'Strategy', '#1f77b4', 'EOY Returns vs Benchmark', 'Year', 'Returns')
    fig.add_trace(go.Bar(x=benchmark_eoy.index.year, y=benchmark_eoy, name='Benchmark', marker_color='#ff7f0e'))
    return fig

# Cumulative Returns (Linear and Log Scale)
def plot_cumulative_returns(strategy_returns, benchmark_returns):
    cumulative_strategy = strategy_returns.cumsum()
    cumulative_benchmark = benchmark_returns.cumsum()

    # Linear scale plot
    fig_linear = create_line_plot(cumulative_strategy, 'Strategy', '#1f77b4', 'Cumulative Returns vs SPY', 'Date', 'Cumulative Returns')
    fig_linear.add_trace(go.Scatter(x=cumulative_benchmark.index, y=cumulative_benchmark, mode='lines', name='Benchmark', line=dict(width=3, dash='dash', color='#ff7f0e')))
    
    # Deep copy for log scale plot
    fig_log = copy.deepcopy(fig_linear)
    fig_log.update_yaxes(type='log')

    return fig_linear, fig_log

# Volatility Matched Returns
def plot_volatility_matched_returns(strategy_returns, benchmark_returns):
    volatility_ratio = strategy_returns.std() / benchmark_returns.std()
    matched_returns = strategy_returns / volatility_ratio
    cumulative_matched = matched_returns.cumsum()
    cumulative_benchmark = benchmark_returns.cumsum()
    fig = create_line_plot(cumulative_matched, 'Volatility Matched Strategy', '#1f77b4', 'Cumulative Returns vs SPY (Volatility Matched)', 'Date', 'Cumulative Returns')
    fig.add_trace(go.Scatter(x=cumulative_benchmark.index, y=cumulative_benchmark, mode='lines', name='Benchmark', line=dict(width=3, dash='dash', color='#ff7f0e')))
    return fig

# Rolling Beta Plot
def plot_rolling_beta(strategy_returns, benchmark_returns, window=126):
    rolling_beta = strategy_returns.rolling(window).cov(benchmark_returns) / benchmark_returns.rolling(window).var()
    return create_line_plot(rolling_beta, 'Rolling Beta', '#2ca02c', 'Rolling Beta (6-Months)', 'Date', 'Beta')

# Monthly Returns Heatmap
def plot_monthly_returns_heatmap(strategy_returns):
    monthly = strategy_returns.resample('M').apply(comp)
    monthly_df = monthly.to_frame(name='Returns')
    monthly_df['Year'] = monthly_df.index.year
    monthly_df['Month'] = monthly_df.index.month
    heatmap_data = monthly_df.pivot(index='Year', columns='Month', values='Returns')
    fig = go.Figure(data=go.Heatmap(z=heatmap_data.values, x=[calendar.month_abbr[m] for m in heatmap_data.columns], y=heatmap_data.index, colorscale='Viridis', hoverongaps=False))
    update_layout(fig, 'Monthly Returns Heatmap', 'Month', 'Year')
    return fig

# Top 5 Drawdown Periods
def plot_top_5_drawdowns(drawdown_series):
    drawdown_periods = drawdown_details(drawdown_series)
    top_5 = drawdown_periods.nsmallest(5, 'drawdown')
    fig = go.Figure()
    for _, row in top_5.iterrows():
        fig.add_trace(go.Scatter(x=[row['start'], row['end']], y=[row['drawdown'], row['drawdown']], mode='lines', name=f"{row['start']} - {row['end']}"))
    update_layout(fig, 'Top 5 Drawdown Periods', 'Date', 'Drawdown')
    return fig

# Return Quantiles Plot
def plot_return_quantiles(strategy_returns):
    return_periods = {
        'Daily': strategy_returns,
        'Weekly': strategy_returns.resample('W').sum(),
        'Monthly': strategy_returns.resample('M').sum(),
        'Quarterly': strategy_returns.resample('Q').sum(),
        'Yearly': strategy_returns.resample('Y').sum()
    }
    fig = go.Figure()
    for period, returns in return_periods.items():
        fig.add_trace(go.Box(y=returns, name=period))
    update_layout(fig, 'Return Quantiles', 'Return Periods', 'Returns')
    return fig

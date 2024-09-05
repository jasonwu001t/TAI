# Required imports
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import asyncio
import nest_asyncio

# Patch for asyncio
nest_asyncio.apply()

# Import your Alpaca and other necessary modules
from TAI.source import Alpaca
from quantstats import *
from quantstats.stats import drawdown_details, max_drawdown, avg_return, sharpe

ap = Alpaca()

# Ensure there's an event loop if missing
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Function to get stock historical data from Alpaca API, cached for 1 day
@st.cache_data(ttl=86400)  # Cache for 1 day (86400 seconds)
def get_stock_data(symbol):
    return ap.get_stock_historical(symbol_or_symbols=symbol, limit=None)

# Streamlit app configuration
st.set_page_config(page_title='Stock Analytical App', layout='wide', page_icon=':chart_with_upwards_trend:')

# Custom styles to keep a clean white background and modern look
st.markdown("""
    <style>
    .main-header { font-size: 32px; font-weight: bold; margin-bottom: 10px; }
    .sub-header { font-size: 24px; margin-top: 20px; }
    .section-header { margin-top: 30px; font-size: 20px; font-weight: bold; text-decoration: underline; }
    .metric-container { margin-bottom: 20px; }
    .chart-container { margin-top: 20px; margin-bottom: 40px; }
    .description-container { font-size: 14px; margin-right: 20px; }
    body { background-color: white !important; }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown("<div class='main-header'>Stock Analytical Dashboard</div>", unsafe_allow_html=True)
st.markdown("**Website:** https://www.yourwebsite.com **| Email:** mailto:info@yourwebsite.com")

# User input for selecting stock symbol
stock_input = st.text_input('Enter Stock Symbol', value='MSFT')

# Load data for the selected stock symbol
with st.spinner(f'Loading Stock Data for {stock_input}...'):
    stock_df = get_stock_data(stock_input)

# Processing the stock dataframe for analytics
# Ensure the 'timestamp' column is in datetime format and set it as the index
stock_df['timestamp'] = pd.to_datetime(stock_df['timestamp'])
stock_df.set_index('timestamp', inplace=True)

# Create a 'returns' column
stock_df['returns'] = stock_df['close'].pct_change().dropna()

# Stock Symbol Selector
symbol = st.selectbox('Select Stock Symbol', stock_df['symbol'].unique())

# Filter data for the selected stock
filtered_df = stock_df[stock_df['symbol'] == symbol]

# Metrics Section
st.markdown("<div class='section-header'>Metrics</div>", unsafe_allow_html=True)
m1, m2, m3 = st.columns((1, 1, 1))

latest_data = filtered_df.iloc[-1]
with m1:
    st.metric(label='Current Price', value=f"${latest_data['close']}", delta=f"{latest_data['close'] - latest_data['open']}")
with m2:
    st.metric(label='Volume', value=f"{latest_data['volume']:,}")
with m3:
    st.metric(label='VWAP', value=f"{latest_data['vwap']:.2f}")

# Keep the first 3 charts unchanged
g1, g2 = st.columns(2)

# Line Chart for Stock Prices
with g1:
    st.markdown("<div class='sub-header'>Stock Prices</div>", unsafe_allow_html=True)
    fig_price = px.line(filtered_df, x=filtered_df.index, y='close', title=f'{symbol} Stock Prices Over Time')
    st.plotly_chart(fig_price, use_container_width=True)

# Bar Chart for Volume
with g2:
    st.markdown("<div class='sub-header'>Trading Volume</div>", unsafe_allow_html=True)
    fig_volume = px.bar(filtered_df, x=filtered_df.index, y='volume', title=f'{symbol} Trading Volume Over Time')
    st.plotly_chart(fig_volume, use_container_width=True)

# Candlestick Chart (full-width)
st.markdown("<div class='sub-header'>Candlestick Chart</div>", unsafe_allow_html=True)
fig_candle = go.Figure(data=[go.Candlestick(x=filtered_df.index,
                open=filtered_df['open'],
                high=filtered_df['high'],
                low=filtered_df['low'],
                close=filtered_df['close'])])
fig_candle.update_layout(title=f'{symbol} Candlestick Chart', yaxis_title='Price')
st.plotly_chart(fig_candle, use_container_width=True)

# Advanced Analytics Section
st.markdown("<div class='section-header'>Advanced Analytics for Stock</div>", unsafe_allow_html=True)

# Top 5 Drawdown Periods with Description
dd_col1, dd_col2 = st.columns([1, 1.5])
with dd_col1:
    st.markdown("<div class='sub-header'>Top 5 Drawdown Periods</div>", unsafe_allow_html=True)
    st.write("""
        This chart shows the five largest drawdowns in the stock's history. A drawdown is the peak-to-trough decline during a specific 
        period of an investment. It measures the percentage drop from the highest point to the lowest point before a new high is reached.
    """)
with dd_col2:
    top_drawdowns_fig = plot_top_5_drawdowns(filtered_df['returns'].cumsum())  # Using cumulative returns for drawdowns
    st.plotly_chart(top_drawdowns_fig, use_container_width=True)

# Underwater Plot with Description
uw_col1, uw_col2 = st.columns([1, 1.5])
with uw_col1:
    st.markdown("<div class='sub-header'>Underwater Plot (Drawdown)</div>", unsafe_allow_html=True)
    st.write("""
        The Underwater Plot represents the cumulative drawdown over time. It helps visualize the depth and duration of all drawdowns 
        during a period, making it easy to see when an investment was below its previous high and by how much.
    """)
with uw_col2:
    underwater_fig = plot_underwater(filtered_df['returns'].cumsum())
    st.plotly_chart(underwater_fig, use_container_width=True)

# Monthly Returns Heatmap with Description
mh_col1, mh_col2 = st.columns([1, 1.5])
with mh_col1:
    st.markdown("<div class='sub-header'>Monthly Returns Heatmap</div>", unsafe_allow_html=True)
    st.write("""
        The Monthly Returns Heatmap provides a visual representation of returns over each month. It shows how the stock performed 
        during each month of the year, making it easy to spot periods of consistent growth or significant declines.
    """)
with mh_col2:
    monthly_heatmap_fig = plot_monthly_returns_heatmap(filtered_df['returns'])
    st.plotly_chart(monthly_heatmap_fig, use_container_width=True)

# Cumulative Returns and Sharpe Ratio
a1, a2 = st.columns([1, 1])
with a1:
    st.markdown("<div class='sub-header'>Cumulative Returns</div>", unsafe_allow_html=True)
    cum_returns_fig, cum_returns_log_fig = plot_cumulative_returns(filtered_df['returns'], filtered_df['returns'])
    st.plotly_chart(cum_returns_fig, use_container_width=True)

with a2:
    st.markdown("<div class='sub-header'>Rolling Sharpe Ratio (126-Day)</div>", unsafe_allow_html=True)
    st.write("""
        The Rolling Sharpe Ratio shows the stock's risk-adjusted returns over time. It uses a 126-day window to measure how much excess 
        return the stock is generating for each unit of risk (standard deviation).
    """)
    rolling_sharpe_fig = plot_rolling_sharpe(filtered_df['returns'])
    st.plotly_chart(rolling_sharpe_fig, use_container_width=True)

# Rolling Sortino Ratio (full-width)
st.markdown("<div class='sub-header'>Rolling Sortino Ratio (126-Day)</div>", unsafe_allow_html=True)
rolling_sortino_fig = plot_rolling_sortino(filtered_df['returns'])
st.plotly_chart(rolling_sortino_fig, use_container_width=True)

# Further Analytics in columns
a3, a4 = st.columns(2)

# Volatility Matched Returns with Description
with a3:
    st.markdown("<div class='sub-header'>Volatility Matched Returns</div>", unsafe_allow_html=True)
    vol_matched_fig = plot_volatility_matched_returns(filtered_df['returns'], filtered_df['returns'])
    st.plotly_chart(vol_matched_fig, use_container_width=True)

# Rolling Beta with Description
with a4:
    st.markdown("<div class='sub-header'>Rolling Beta (126-Day)</div>", unsafe_allow_html=True)
    rolling_beta_fig = plot_rolling_beta(filtered_df['returns'], filtered_df['returns'])
    st.plotly_chart(rolling_beta_fig, use_container_width=True)

# Key Performance Metrics
st.markdown("<div class='sub-header'>Key Performance Metrics</div>", unsafe_allow_html=True)
performance_metrics_fig = key_performance_metrics(filtered_df['returns'], filtered_df['returns'])
st.plotly_chart(performance_metrics_fig, use_container_width=True)

# Contact Form Section
st.markdown("<div class='section-header'>Contact Us</div>", unsafe_allow_html=True)
with st.expander("Send us a message"):
    with st.form(key='contact', clear_on_submit=True):
        email = st.text_input('Contact Email')
        message = st.text_area("Your Message")
        submit_button = st.form_submit_button(label='Send Message')

st.success('App loaded successfully!')


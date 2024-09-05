# Required imports
import streamlit as st
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import asyncio
import nest_asyncio

# Patch for asyncio
nest_asyncio.apply()

# Import your Alpaca and other necessary modules
from TAI.source import Alpaca
from TAI.analytics import QuickPlot

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

# Header
t1, t2 = st.columns((0.07, 1))
t2.title("Stock Analytical Dashboard")
t2.markdown(" **Website:** https://www.yourwebsite.com **| Email:** mailto:info@yourwebsite.com")

# User input for selecting stock symbol
stock_input = st.text_input('Enter Stock Symbol', value='MSFT')

# Load data for the selected stock symbol
with st.spinner(f'Loading Stock Data for {stock_input}...'):
    stock_df = get_stock_data(stock_input)

# Stock Symbol Selector
symbol = st.selectbox('Select Stock Symbol', stock_df['symbol'].unique())

# Filter data for the selected stock
filtered_df = stock_df[stock_df['symbol'] == symbol]

# Metrics
latest_data = filtered_df.iloc[-1]
m1, m2, m3 = st.columns((1, 1, 1))
m1.metric(label='Current Price', value=f"${latest_data['close']}", delta=f"{latest_data['close'] - latest_data['open']}")
m2.metric(label='Volume', value=f"{latest_data['volume']:,}")
m3.metric(label='VWAP', value=f"{latest_data['vwap']:.2f}")

# Charts
g1, g2, g3 = st.columns((1, 1, 1))

# Line Chart for Stock Prices
fig_price = px.line(filtered_df, x='timestamp', y='close', title=f'{symbol} Stock Prices Over Time')
g1.plotly_chart(fig_price, use_container_width=True)

# Bar Chart for Volume
fig_volume = px.bar(filtered_df, x='timestamp', y='volume', title=f'{symbol} Trading Volume Over Time')
g2.plotly_chart(fig_volume, use_container_width=True)

# Candlestick Chart for OHLC Data
fig_candle = go.Figure(data=[go.Candlestick(x=filtered_df['timestamp'],
                open=filtered_df['open'],
                high=filtered_df['high'],
                low=filtered_df['low'],
                close=filtered_df['close'])])
fig_candle.update_layout(title=f'{symbol} Candlestick Chart', yaxis_title='Price')
g3.plotly_chart(fig_candle, use_container_width=True)

# Contact Form
with st.expander("Contact us"):
    with st.form(key='contact', clear_on_submit=True):
        email = st.text_input('Contact Email')
        message = st.text_area("Your Message")
        submit_button = st.form_submit_button(label='Send Message')

st.success('App loaded successfully!')

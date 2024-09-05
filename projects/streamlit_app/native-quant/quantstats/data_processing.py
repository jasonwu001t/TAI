# quantstats_plotly/data_processing.py

import pandas as pd

def load_data(filepath):
    return pd.read_csv(filepath, index_col=0, parse_dates=True)

def prepare_returns(data):
    return data.pct_change().dropna()

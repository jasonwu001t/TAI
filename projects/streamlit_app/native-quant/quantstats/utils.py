# quantstats_plotly/utils.py

import numpy as np

def remove_outliers(returns, threshold=3):
    mean = np.mean(returns)
    std = np.std(returns)
    return returns[np.abs(returns - mean) < threshold * std]

def gain_to_pain_ratio(returns):
    return np.sum(returns) / np.abs(np.sum(returns[returns < 0]))

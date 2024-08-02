import numpy as np
import pandas as pd

def sharpe_ratio(returns, risk_free_rate=0):
    excess_returns = returns - risk_free_rate
    return np.mean(excess_returns) / np.std(excess_returns)

def sortino_ratio(returns, risk_free_rate=0):
    downside_returns = returns[returns < risk_free_rate]
    return np.mean(returns - risk_free_rate) / np.std(downside_returns)

def max_drawdown(returns):
    cumulative = (1 + returns).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak
    return drawdown.min()

def value_at_risk(returns, confidence_level=0.95):
    return np.percentile(returns, 100 * (1 - confidence_level))

def conditional_value_at_risk(returns, confidence_level=0.95):
    var = value_at_risk(returns, confidence_level)
    return returns[returns <= var].mean()

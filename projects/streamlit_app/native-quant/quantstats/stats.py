# quantstats_plotly/stats.py

import numpy as np
import pandas as pd

def avg_loss(returns):
    return returns[returns < 0].mean()

def avg_return(returns):
    return returns.mean()

def avg_win(returns):
    return returns[returns > 0].mean()

def best(returns):
    return returns.max()

def cagr(returns, periods=252):
    n = len(returns) / periods
    return (np.prod(returns + 1)) ** (1 / n) - 1

def calmar(returns, drawdown):
    return cagr(returns) / max_drawdown(drawdown)

def common_sense_ratio(returns, drawdown):
    return avg_return(returns) / max_drawdown(drawdown)

def comp(returns):
    """Compounds returns."""
    return np.prod(returns + 1) - 1

def conditional_value_at_risk(returns, level=0.05):
    return returns[returns <= returns.quantile(level)].mean()

def consecutive_losses(returns):
    return (returns < 0).astype(int).groupby((returns >= 0).astype(int).cumsum()).cumsum().max()

def consecutive_wins(returns):
    return (returns > 0).astype(int).groupby((returns <= 0).astype(int).cumsum()).cumsum().max()

def drawdown_details(returns):
    """Calculates detailed drawdown periods."""
    cumulative_returns = returns.cumsum()
    drawdowns = cumulative_returns - cumulative_returns.cummax()
    
    # Identify drawdown periods
    is_in_drawdown = drawdowns < 0
    drawdown_periods = []
    
    start = None
    for i in range(1, len(drawdowns)):
        if is_in_drawdown[i] and not is_in_drawdown[i - 1]:
            start = drawdowns.index[i]
        if not is_in_drawdown[i] and is_in_drawdown[i - 1] and start is not None:
            end = drawdowns.index[i]
            dd = drawdowns[start:end].min()
            drawdown_periods.append({'start': start, 'end': end, 'drawdown': dd})
            start = None

    if start is not None:  # For ongoing drawdowns
        drawdown_periods.append({'start': start, 'end': drawdowns.index[-1], 'drawdown': drawdowns[start:].min()})

    return pd.DataFrame(drawdown_periods)

def expected_return(returns, risk_free=0):
    return returns.mean() - risk_free

def expected_shortfall(returns):
    return conditional_value_at_risk(returns)

def gain_to_pain_ratio(returns):
    return np.sum(returns) / np.abs(np.sum(returns[returns < 0]))

def geometric_mean(returns):
    return np.prod(returns + 1) ** (1 / len(returns)) - 1

def max_drawdown(drawdown):
    return drawdown.min()

def monthly_returns(returns):
    return returns.resample('M').sum()

def sharpe(returns, risk_free=0, periods=252):
    return (returns.mean() - risk_free) / returns.std() * np.sqrt(periods)

def sortino(returns, risk_free=0, periods=252):
    downside_risk = returns[returns < risk_free].std() * np.sqrt(periods)
    return (returns.mean() - risk_free) / downside_risk

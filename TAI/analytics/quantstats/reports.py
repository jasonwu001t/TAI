from .stats import sharpe_ratio, sortino_ratio, max_drawdown, value_at_risk, conditional_value_at_risk
from .plots import plot_rolling_sharpe, plot_drawdown

def create_report(returns, risk_free_rate=0, confidence_level=0.95, window=252):
    report = {
        'Sharpe Ratio': sharpe_ratio(returns, risk_free_rate),
        'Sortino Ratio': sortino_ratio(returns, risk_free_rate),
        'Max Drawdown': max_drawdown(returns),
        'Value at Risk': value_at_risk(returns, confidence_level),
        'Conditional Value at Risk': conditional_value_at_risk(returns, confidence_level)
    }

    plot_rolling_sharpe(returns, window, risk_free_rate)
    plot_drawdown(returns)

    return report

def annualize_returns(returns, periods_per_year=252):
    compounded_growth = (1 + returns).prod()
    n_periods = returns.shape[0]
    return compounded_growth ** (periods_per_year / n_periods) - 1

def annualize_volatility(returns, periods_per_year=252):
    return returns.std() * (periods_per_year ** 0.5)

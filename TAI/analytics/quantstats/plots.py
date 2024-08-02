import plotly.graph_objects as go

def plot_rolling_sharpe(returns, window=252, risk_free_rate=0):
    sharpe = (returns.rolling(window=window).mean() - risk_free_rate) / returns.rolling(window=window).std()
    sharpe = sharpe.dropna()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sharpe.index, y=sharpe.values, mode='lines', name='Sharpe Ratio'))
    fig.update_layout(title=f'Rolling Sharpe Ratio ({window}-day)',
                      xaxis_title='Date',
                      yaxis_title='Sharpe Ratio')
    fig.show()

def plot_drawdown(returns):
    cumulative = (1 + returns).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=drawdown.index, y=drawdown.values, mode='lines', name='Drawdown'))
    fig.update_layout(title='Drawdown',
                      xaxis_title='Date',
                      yaxis_title='Drawdown')
    fig.show()

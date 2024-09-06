import plotly.graph_objects as go

def plot_profit_loss_table(profit_df, symbol, option_type):
    """Plots the profit/loss table using Plotly."""
    fig = go.Figure()

    fig.add_trace(go.Heatmap(
        z=profit_df.values,
        x=profit_df.columns,
        y=profit_df.index,
        colorscale='RdYlGn',
        hoverongaps=False,
        colorbar=dict(title='Profit / Loss ($)')
    ))

    for i, row in enumerate(profit_df.values):
        for j, val in enumerate(row):
            fig.add_annotation(
                go.layout.Annotation(
                    text=f'{val:.0f}',  # Formatting value as integer
                    x=profit_df.columns[j],
                    y=profit_df.index[i],
                    showarrow=False,
                    font=dict(color="black" if -100 < val < 100 else "white"),
                )
            )

    fig.update_layout(
        title=f'Options Profit and Loss Table for {symbol} ({option_type.title()} Option)',
        xaxis_title="Date",
        yaxis_title="Stock Price",
        xaxis=dict(tickmode='array', tickvals=profit_df.columns),
        yaxis=dict(tickmode='array', tickvals=profit_df.index),
        font=dict(family="Arial", size=12),
        height=800,
        width=900,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(255,255,255,1)',
        margin=dict(l=100, r=100, t=100, b=100),
    )

    fig.show()

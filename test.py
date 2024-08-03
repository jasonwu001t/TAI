import pandas as pd
import numpy as np
import plotly.express as px

def plot_monthly_heatmap(df): 
    """Plot a heatmap with annotations of monthly returns."""
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    df['Month'] = pd.Categorical(df['Month'], categories=month_order, ordered=True)
    pivot_table = df.pivot(index='Year', columns='Month', values='value').fillna(0)
    
    # Reverse the order of the years
    pivot_table = pivot_table[::-1]

    fig = px.imshow(
        pivot_table,
        labels=dict(x="Month", y="Year", color="Returns"),
        x=month_order,
        y=pivot_table.index,
        color_continuous_scale="RdBu",
        aspect="auto",
        text_auto=True,
    )

    fig.update_layout(
        title="Strategy - Monthly Active Returns (%)",
        title_x=0.5,
        font=dict(family="Arial", size=10),
        width=1277,
        height=768,  # Adjust to fit content
        margin=dict(t=50, l=150, r=50, b=50)
    )

    fig.update_yaxes(autorange="reversed")  # Reverse the y-axis order

    fig.show()

# Generate and plot data
df = create_sample_data()
plot_monthly_heatmap(df)


def create_sample_data():
    """Generates a sample DataFrame with random monthly returns for several years."""
    np.random.seed(42)
    date_rng = pd.date_range(start='2020-01-01', end='2024-12-31', freq='M')
    values = np.random.randn(len(date_rng)) * 10  # Random returns
    df = pd.DataFrame({'date': date_rng, 'value': values})
    df['Year'] = df['date'].dt.year
    df['Month'] = df['date'].dt.strftime('%b')  # Short month name
    return df
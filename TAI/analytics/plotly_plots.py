import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

class QuickPlot:
    def __init__(self):
        pass

    def plot_lines(self, dfs, labels=None, title="Line Plot", show_slide = False):
        if labels is None:
            labels = [f"Series {i+1}" for i in range(len(dfs))]
        color_sequence = px.colors.qualitative.Plotly  # Use Plotly's default qualitative color sequence
        fig = go.Figure()
        for i, (df_single, label) in enumerate(zip(dfs, labels)):
            fig.add_trace(go.Scatter(x=df_single.iloc[:, 0], y=df_single.iloc[:, 1], mode='lines', name=label, line=dict(color=color_sequence[i % len(color_sequence)])))
        fig.update_layout(title=title, xaxis_title='Date', yaxis_title='Value')
        fig.update_xaxes(rangeslider=dict(visible=show_slide), showgrid=False, tickformat='%b %Y')
        fig.update_yaxes(showgrid=False)
        fig.update_traces(textposition='top center')
        return fig

    def plot_lines_with_events(self, dfs, events_dict=None, labels=None, title="Line Plot with Events"):
        """ events_dict format
        events_dict = {
        '2020-01-10': 'Event A',
        '2020-02-20': 'Event B',
        '2020-03-30': 'Event C'
        }
        """
        fig = self.plot_lines(dfs, labels=labels, title=title)
        if events_dict:
            for event_date, event_name in events_dict.items():
                event_date = pd.to_datetime(event_date)  # Convert string to datetime
                fig.add_vline(x=event_date, line=dict(color='red', width=2, dash='dash'))
                fig.add_annotation(x=event_date, y=max([df_single.iloc[:, 1].max() for df_single in dfs]),
                                   text=event_name, showarrow=True, arrowhead=1, ax=0, ay=-40)
        return fig
    
    def plot_monthly_heatmap(self, df):
        """Plot a heatmap with annotations of monthly returns.
        DF Input Sample, we have to make sure its already monthly data here (not daily)
                date	value	Year	Month
        2020-01-31	4.967142	2020	Jan
        2020-02-29	-1.382643	2020	Feb
        """
        df['Year'] = df[df.columns[0]].dt.year
        df['Month'] = df[df.columns[0]].dt.strftime('%b')  # Short month name
        
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

    def plot_interest_rates(self, df_dict, hidden_labels=None):
        if hidden_labels is None:
            hidden_labels = []
            
        fig = go.Figure()
        color_sequence = px.colors.qualitative.Plotly
        num_colors = len(color_sequence)
        opacities = [1, 0.4, 0.3, 0.3, 0.2]

        for i, (label, df) in enumerate(df_dict.items()):
            melted_df = df.reset_index().melt(id_vars=['date'], var_name='maturity', value_name='values')
            first_date = melted_df['date'].iloc[0] if 'date' in melted_df.columns else 'Unknown Date'
            is_visible = 'legendonly' if label in hidden_labels else True

            fig.add_trace(go.Scatter(
                x=melted_df['maturity'], y=melted_df['values'], mode='lines+markers',
                name=f"{label}: {first_date.strftime('%Y-%m-%d') if isinstance(first_date, pd.Timestamp) else first_date}",
                line=dict(color=color_sequence[i % num_colors], width=3),
                marker=dict(color='white', size=10, line=dict(color=color_sequence[i % num_colors], width=2)),
                opacity=opacities[i % len(opacities)],
                visible=is_visible
            ))

        fig.update_layout(title='Interest Rate Curve', xaxis_title='Maturity', yaxis_title='Values', plot_bgcolor='white')
        fig.update_xaxes(showgrid=False, showline=True, linewidth=2, linecolor='black', mirror=True, fixedrange=True)
        fig.update_yaxes(showgrid=False, showline=True, linewidth=2, linecolor='black', mirror=True, fixedrange=True)

        return fig
    # fig = plot_interest_rates(df_dict, hidden_labels=['Label1', 'Label3'])


    def plot_bar(self, df, labels=None, title="Bar Plot", x='date', y='value', **layout_kwargs):
        if labels is None:
            labels = [f"Series {i+1}" for i in range(len(df))]
        fig = go.Figure()
        for df_single, label in zip(df, labels):
            fig.add_trace(go.Bar(x=df_single[x], y=df_single[y], name=label))
        fig.update_layout(title=title, xaxis_title=x, yaxis_title=y, **layout_kwargs)
        return fig

    def plot_scatter(self, df, labels=None, title="Scatter Plot", x='date', y='value', **layout_kwargs):
        if labels is None:
            labels = [f"Series {i+1}" for i in range(len(df))]
        fig = go.Figure()
        for df_single, label in zip(df, labels):
            fig.add_trace(go.Scatter(x=df_single[x], y=df_single[y], mode='markers', name=label))
        fig.update_layout(title=title, xaxis_title=x, yaxis_title=y, **layout_kwargs)
        return fig

    def prepare_figure(self, df, title):
        # Get the first and second column names dynamically
        first_col = df.columns[0]
        second_col = df.columns[1]

        # Ensure first column (assumed to be 'date') is the index and convert to datetime
        df[first_col] = pd.to_datetime(df[first_col])
        df.set_index(first_col, inplace=True)

        # Calculate latest rate and YoY changes using the second column
        latest_rate = df[second_col].iloc[-1]
        yoy_change = df[second_col].pct_change(12).iloc[-1]  # change from one year ago
        section_1 = f"Latest Refresh On: {df.index[-1].strftime('%a %b %d, %Y')}<br><b>{title}:</b> {latest_rate:,.2f} \
                <br><b>YoY Change:</b> {yoy_change:,.2f} bps"

        # Calculate Mean, Median, Min, and Max using the second column
        mean_val = df[second_col].mean()
        median_val = df[second_col].median()
        min_val = df[second_col].min()
        min_date = df[second_col].idxmin().strftime('%b %Y')
        max_val = df[second_col].max()
        max_date = df[second_col].idxmax().strftime('%b %Y')
        section_2 = f"<b>Mean:</b> {mean_val:,.2f}<br>" \
                    f"<b>Median:</b> {median_val:,.2f}<br><b>Min:</b> {min_val:,.2f} ({min_date})<br>" \
                    f"<b>Max:</b> {max_val:,.2f} ({max_date})"

        fig = go.Figure()

        # Add a line plot with a customized hover text and line style
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df[second_col],
            mode='lines',
            name=title,  # This is the right panel line name, for this case I do not want to show
            line=dict(color='darkblue', width=2),
            hovertemplate=f'Date: %{x}<br>{title}: %{y:,.2f}'
        ))

        # Add low and high points for each year
        for year in df.index.year.unique():
            df_year = df[df.index.year == year]
            if not df_year.empty:
                min_idx = df_year[second_col].idxmin()
                max_idx = df_year[second_col].idxmax()

                # Low point use Red dot
                fig.add_trace(go.Scatter(
                    x=[min_idx],
                    y=[df_year[second_col][min_idx]],
                    mode='markers',
                    name='Calendar Year Low' if year == df.index.year.unique()[-1] else None,  # Only label the last year
                    marker=dict(color='red'),
                    showlegend=False
                ))
                fig.add_annotation(
                    x=min_idx,
                    y=df_year[second_col][min_idx],
                    text=f'{year} Low',
                    showarrow=True,
                    font=dict(color="black", size=12),
                    align="center",
                    arrowhead=2,
                    arrowsize=1,
                    arrowwidth=2,
                    arrowcolor="#636363",
                    ax=20,
                    ay=-30,
                    bordercolor="#c7c7c7",
                    borderwidth=2,
                    borderpad=4,
                    bgcolor="red",
                    opacity=0.8
                )

                # High point use Green dot
                fig.add_trace(go.Scatter(
                    x=[max_idx],
                    y=[df_year[second_col][max_idx]],
                    mode='markers',
                    name='Calendar Year High' if year == df.index.year.unique()[-1] else None,  # Only label the last year
                    marker=dict(color='green'),
                    showlegend=False
                ))
                fig.add_annotation(
                    x=max_idx,
                    y=df_year[second_col][max_idx],
                    text=f'{year} High',
                    showarrow=True,
                    font=dict(color="black", size=12),
                    align="center",
                    arrowhead=2,
                    arrowsize=1,
                    arrowwidth=2,
                    arrowcolor="#636363",
                    ax=20,
                    ay=-30,
                    bordercolor="#c7c7c7",
                    borderwidth=2,
                    borderpad=4,
                    bgcolor="green",
                    opacity=0.8
                )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=title,
            plot_bgcolor='rgba(0, 0, 0, 0)',  # Transparent background
            xaxis=dict(
                showgrid=True,  # Show a grid
                gridcolor='rgba(200, 200, 200, 0.2)',  # Light grey grid lines
            ),
            yaxis=dict(
                showgrid=True,  # Show a grid
                gridcolor='rgba(200, 200, 200, 0.2)',  # Light grey grid lines
            ),
            legend=dict(  # This is used to position the line name/notes
                x=0.5,  # Horizontally centered
                y=-0.13,  # Position the legend below the plot
                xanchor='center',  # Anchor the legend at its center
            ),
            annotations=[  # This is for the setting within the text box
                dict(
                    x=df.index[1],  # Adjust where to locate text box
                    y=1,  # Text box vertical location
                    xref='x',
                    yref='paper',
                    text=section_1 + "<br><br>" + section_2,
                    showarrow=False,
                    align="left",  # Align text box content to left
                    xanchor='left',  # Adjust if left or right of the text box start point based on x-axis point
                    font=dict(size=12),
                    bgcolor="rgba(255, 255, 255, 1)",  # White background with full opacity
                )
            ]
        )
        return fig


if __name__ == "__main__":
    # Sample data for demonstration
    data1 = {
        'date': pd.date_range(start='1/1/2020', periods=100),
        'value': range(100)
    }
    data2 = {
        'date': pd.date_range(start='1/1/2020', periods=100),
        'value': range(100, 200)
    }
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    # Initialize QuickPlot
    qp = QuickPlot()

    # Plot line, bar, scatter charts
    fig_line = qp.plot_lines(dfs=[df1, df2], labels=["Series 1", "Series 2"], title="Sample Line Plot")
    fig_bar = qp.plot_bar(df=[df1, df2], labels=["Series 1", "Series 2"], title="Sample Bar Plot")
    fig_scatter = qp.plot_scatter(df=[df1, df2], labels=["Series 1", "Series 2"], title="Sample Scatter Plot")

    fig_line.show()
    fig_bar.show()
    fig_scatter.show()

    # Example usage of plot_line_with_events
    events = {
        '2020-01-10': 'Event A',
        '2020-02-20': 'Event B',
        '2020-03-30': 'Event C'
    }
    fig_line_with_events = qp.plot_line_with_events(df=[df1, df2], labels=["Series 1", "Series 2"], title="Line Plot with Events", events=events)
    fig_line_with_events.show()

    # Prepare a specific figure similar to the plotter.py example
    fig_custom = qp.prepare_figure(df1, title="Sample Custom Plot")
    fig_custom.show()

    rates_data = {
        'Today': df1,
        'Last Week': df2,
        'Last Month': df1,
        'Last 3 Months': df2,
        'Last Year': df1
    }
    
    fig_rates = qp.plot_interest_rates(rates_data)
    fig_rates.show()

    # Test plot_monthly_heatmap
    def create_sample_data():
        """Generates a sample DataFrame with random monthly returns for several years."""
        np.random.seed(42)
        date_rng = pd.date_range(start='2020-01-01', end='2024-12-31', freq='M')
        values = np.random.randn(len(date_rng)) * 10  # Random returns
        df = pd.DataFrame({'date': date_rng, 'value': values})
        df['Year'] = df['date'].dt.year
        df['Month'] = df['date'].dt.strftime('%b')  # Short month name
        return df
    
    qp.plot_monthly_heatmap(create_sample_data())

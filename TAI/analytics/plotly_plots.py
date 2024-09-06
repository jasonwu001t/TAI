import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import pandas as pd
import numpy as np
import calendar
import copy
from TAI.analytics import DataAnalytics

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

class QuantStatsPlot:
    def __init__(self):
        self.stats = DataAnalytics()

    def update_layout(self, fig, title, xaxis_title, yaxis_title):
        fig.update_layout(
            title={
                'text': title,
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': {'size': 20, 'family': 'Arial'}
            },
            xaxis_title=xaxis_title,
            yaxis_title=yaxis_title,
            font=dict(family='Arial', size=12),
            paper_bgcolor='white',
            plot_bgcolor='white',
            hovermode="x",
            xaxis=dict(showgrid=False, zeroline=False, linecolor='black'),
            yaxis=dict(showgrid=True, gridcolor='lightgray', zeroline=False, linecolor='black'),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor="rgba(255, 255, 255, 0.5)",
                bordercolor="Black",
                borderwidth=1
            )
        )

    # Reusable method to create line plots
    def create_line_plot(self, data, name, color, title, xaxis_title, yaxis_title):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data.index, y=data, mode='lines', name=name, line=dict(width=2, color=color)))
        self.update_layout(fig, title, xaxis_title, yaxis_title)
        return fig

    # Reusable method for bar plots
    def create_bar_plot(self, x_data, y_data, name, color, title, xaxis_title, yaxis_title):
        fig = go.Figure()
        fig.add_trace(go.Bar(x=x_data, y=y_data, name=name, marker_color=color))
        self.update_layout(fig, title, xaxis_title, yaxis_title)
        return fig

    def plot_rolling_sharpe(self, strategy_returns, window=126, risk_free_rate=0):
        rolling_sharpe = (strategy_returns.rolling(window).mean() - risk_free_rate) / strategy_returns.rolling(window).std()
        return self.create_line_plot(rolling_sharpe, f'Rolling {window}-Day Sharpe', '#d62728', f'Rolling Sharpe Ratio ({window}-Day)', 'Date', 'Sharpe Ratio')

    def plot_rolling_sortino(self, strategy_returns, window=126, risk_free_rate=0):
        downside_risk = strategy_returns[strategy_returns < risk_free_rate].rolling(window).std()
        rolling_sortino = (strategy_returns.rolling(window).mean() - risk_free_rate) / downside_risk
        return self.create_line_plot(rolling_sortino, f'Rolling {window}-Day Sortino', '#9467bd', f'Rolling Sortino Ratio ({window}-Day)', 'Date', 'Sortino Ratio')

    def plot_underwater(self, drawdown_series):
        return self.create_line_plot(drawdown_series, 'Drawdown', '#1f77b4', 'Underwater Plot (Cumulative Drawdowns)', 'Date', 'Drawdown')

    def plot_rolling_volatility(self, strategy_returns, window=126):
        rolling_volatility = strategy_returns.rolling(window).std()
        return self.create_line_plot(rolling_volatility, f'Rolling {window}-Day Volatility', '#2ca02c', f'Rolling Volatility ({window}-Day)', 'Date', 'Volatility')

    def plot_daily_returns(self, strategy_returns):
        return self.create_line_plot(strategy_returns, 'Daily Returns', '#1f77b4', 'Daily Returns', 'Date', 'Returns')

    def plot_distribution_of_monthly_returns(self, strategy_returns):
        monthly_returns = strategy_returns.resample('M').apply(self.stats.comp)
        fig = px.histogram(monthly_returns, nbins=50)
        self.update_layout(fig, 'Distribution of Monthly Returns', 'Monthly Returns', 'Frequency')
        return fig

    def key_performance_metrics(self, strategy_returns, benchmark_returns):
        data = {
            "Metric": ["Sharpe Ratio", "Sortino Ratio", "CAGR", "Max Drawdown"],
            "Strategy": [self.stats.sharpe(strategy_returns), self.stats.sortino(strategy_returns), self.stats.cagr(strategy_returns), self.stats.max_drawdown(strategy_returns.cumsum())],
            "Benchmark": [self.stats.sharpe(benchmark_returns), self.stats.sortino(benchmark_returns), self.stats.cagr(benchmark_returns), self.stats.max_drawdown(benchmark_returns.cumsum())]
        }
        df = pd.DataFrame(data)
        fig = go.Figure(data=[go.Table(
            header=dict(values=list(df.columns), fill_color='paleturquoise', align='left', font=dict(size=12)),
            cells=dict(values=[df.Metric, df.Strategy, df.Benchmark], fill_color='lavender', align='left', font=dict(size=12))
        )])
        fig.update_layout(title='Key Performance Metrics')
        return fig

    def plot_eoy_returns(self, strategy_returns, benchmark_returns):
        strategy_eoy = strategy_returns.resample('Y').apply(self.stats.comp)
        benchmark_eoy = benchmark_returns.resample('Y').apply(self.stats.comp)
        fig = self.create_bar_plot(strategy_eoy.index.year, strategy_eoy, 'Strategy', '#1f77b4', 'EOY Returns vs Benchmark', 'Year', 'Returns')
        fig.add_trace(go.Bar(x=benchmark_eoy.index.year, y=benchmark_eoy, name='Benchmark', marker_color='#ff7f0e'))
        return fig

    def plot_cumulative_returns(self, strategy_returns, benchmark_returns):
        cumulative_strategy = strategy_returns.cumsum()
        cumulative_benchmark = benchmark_returns.cumsum()

        # Linear scale plot
        fig_linear = self.create_line_plot(cumulative_strategy, 'Strategy', '#1f77b4', 'Cumulative Returns vs SPY', 'Date', 'Cumulative Returns')
        fig_linear.add_trace(go.Scatter(x=cumulative_benchmark.index, y=cumulative_benchmark, mode='lines', name='Benchmark', line=dict(width=3, dash='dash', color='#ff7f0e')))
        
        # Deep copy for log scale plot
        fig_log = copy.deepcopy(fig_linear)
        fig_log.update_yaxes(type='log')

        return fig_linear, fig_log

    def plot_volatility_matched_returns(self, strategy_returns, benchmark_returns):
        volatility_ratio = strategy_returns.std() / benchmark_returns.std()
        matched_returns = strategy_returns / volatility_ratio
        cumulative_matched = matched_returns.cumsum()
        cumulative_benchmark = benchmark_returns.cumsum()
        fig = self.create_line_plot(cumulative_matched, 'Volatility Matched Strategy', '#1f77b4', 'Cumulative Returns vs SPY (Volatility Matched)', 'Date', 'Cumulative Returns')
        fig.add_trace(go.Scatter(x=cumulative_benchmark.index, y=cumulative_benchmark, mode='lines', name='Benchmark', line=dict(width=3, dash='dash', color='#ff7f0e')))
        return fig

    def plot_rolling_beta(self, strategy_returns, benchmark_returns, window=126):
        rolling_beta = strategy_returns.rolling(window).cov(benchmark_returns) / benchmark_returns.rolling(window).var()
        return self.create_line_plot(rolling_beta, 'Rolling Beta', '#2ca02c', 'Rolling Beta (6-Months)', 'Date', 'Beta')

    def plot_monthly_returns_heatmap(self, strategy_returns):
        # Resample returns by month
        monthly = strategy_returns.resample('M').apply(self.stats.comp)
        
        # Create a DataFrame with monthly returns
        monthly_df = monthly.to_frame(name='Returns')
        monthly_df['Year'] = monthly_df.index.year
        monthly_df['Month'] = monthly_df.index.month
        heatmap_data = monthly_df.pivot(index='Year', columns='Month', values='Returns')
        
        # Format the numbers to be displayed as text in the heatmap cells
        text_values = heatmap_data.applymap(lambda x: '{:.2f}'.format(x) if pd.notnull(x) else '')
        
        # Define a custom colorscale with adjusted colors
        custom_colorscale = [
            [0.0, '#ff9999'],  # Lighter red for lower values (losses)
            [0.5, '#ffffcc'],  # Light yellow for neutral values
            [1.0, '#99ff99']   # Light green for higher values (gains)
        ]
        
        # Create the heatmap with annotations
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=[calendar.month_abbr[m] for m in heatmap_data.columns],
            y=heatmap_data.index,
            colorscale=custom_colorscale,  # Use the custom light red-yellow-green colorscale
            hoverongaps=False,
            text=text_values.values,  # Add the text annotations
            texttemplate="%{text}",  # Use the text values for the cell labels
            textfont={"size": 10}  # Control the size of the text
        ))
        
        # Update the layout for the heatmap
        fig.update_layout(
            title='Monthly Returns Heatmap',
            xaxis_title='Month',
            yaxis_title='Year',
            yaxis=dict(
                tickmode='array',
                tickvals=heatmap_data.index,  # Ensure only integer years are shown
                ticktext=[str(int(year)) for year in heatmap_data.index]  # Display years as integers
            )
        )
        
        return fig

    def plot_top_5_drawdowns(self, drawdown_series):
        drawdown_periods = self.stats.drawdown_details(drawdown_series)
        top_5 = drawdown_periods.nsmallest(5, 'drawdown')
        fig = go.Figure()
        for _, row in top_5.iterrows():
            fig.add_trace(go.Scatter(x=[row['start'], row['end']], y=[row['drawdown'], row['drawdown']], mode='lines', name=f"{row['start']} - {row['end']}"))
        self.update_layout(fig, 'Top 5 Drawdown Periods', 'Date', 'Drawdown')
        return fig

    def plot_return_quantiles(self, strategy_returns):
        return_periods = {
            'Daily': strategy_returns,
            'Weekly': strategy_returns.resample('W').sum(),
            'Monthly': strategy_returns.resample('M').sum(),
            'Quarterly': strategy_returns.resample('Q').sum(),
            'Yearly': strategy_returns.resample('Y').sum()
        }
        fig = go.Figure()
        for period, returns in return_periods.items():
            fig.add_trace(go.Box(y=returns, name=period))
        self.update_layout(fig, 'Return Quantiles', 'Return Periods', 'Returns')
        return fig

    def generate_report(self, strategy_returns, drawdown_series, benchmark_returns, output_path='quantstats_report.html'):
        html_content = """
        <html>
        <head>
            <title>Strategy Tearsheet Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .container {{ display: flex; }}
                .left {{ width: 70%; padding-right: 20px; }}
                .right {{ width: 30%; background-color: #f9f9f9; padding-left: 20px; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                h2 {{ border-bottom: 1px solid #ddd; padding-bottom: 10px; }}
            </style>
        </head>
        <body>
            <h1>Strategy Tearsheet</h1>
            <h4>Data from {start} to {end}</h4>
            <div class="container">
                <div class="left">
        """.format(
            start=strategy_returns.index.min().strftime('%d %b, %Y'),
            end=strategy_returns.index.max().strftime('%d %b, %Y')
        )

        # Cumulative Returns (Linear and Log)
        cumulative_returns_fig, cumulative_returns_log_fig = self.plot_cumulative_returns(strategy_returns, benchmark_returns)
        html_content += "<h2>Cumulative Returns vs Benchmark</h2>"
        html_content += pio.to_html(cumulative_returns_fig, full_html=False)
        html_content += pio.to_html(cumulative_returns_log_fig, full_html=False)

        # Volatility Matched Returns
        volatility_matched_fig = self.plot_volatility_matched_returns(strategy_returns, benchmark_returns)
        html_content += "<h2>Cumulative Returns vs SPY (Volatility Matched)</h2>"
        html_content += pio.to_html(volatility_matched_fig, full_html=False)

        # End of Year (EOY) Returns
        eoy_returns_fig = self.plot_eoy_returns(strategy_returns, benchmark_returns)
        html_content += "<h2>EOY Returns vs Benchmark</h2>"
        html_content += pio.to_html(eoy_returns_fig, full_html=False)

        # Distribution of Monthly Returns
        distribution_fig = self.plot_distribution_of_monthly_returns(strategy_returns)
        html_content += "<h2>Distribution of Monthly Returns</h2>"
        html_content += pio.to_html(distribution_fig, full_html=False)

        # Daily Returns
        daily_returns_fig = self.plot_daily_returns(strategy_returns)
        html_content += "<h2>Daily Returns</h2>"
        html_content += pio.to_html(daily_returns_fig, full_html=False)

        # Rolling Beta
        rolling_beta_fig = self.plot_rolling_beta(strategy_returns, benchmark_returns)
        html_content += "<h2>Rolling Beta (6-Months)</h2>"
        html_content += pio.to_html(rolling_beta_fig, full_html=False)

        # Rolling Volatility
        rolling_volatility_fig = self.plot_rolling_volatility(strategy_returns)
        html_content += "<h2>Rolling Volatility (6-Months)</h2>"
        html_content += pio.to_html(rolling_volatility_fig, full_html=False)

        # Rolling Sharpe
        rolling_sharpe_fig = self.plot_rolling_sharpe(strategy_returns)
        html_content += "<h2>Rolling Sharpe (6-Months)</h2>"
        html_content += pio.to_html(rolling_sharpe_fig, full_html=False)

        # Rolling Sortino
        rolling_sortino_fig = self.plot_rolling_sortino(strategy_returns)
        html_content += "<h2>Rolling Sortino (6-Months)</h2>"
        html_content += pio.to_html(rolling_sortino_fig, full_html=False)

        # Top 5 Drawdown Periods
        top_5_drawdowns_fig = self.plot_top_5_drawdowns(strategy_returns.cumsum())
        html_content += "<h2>Top 5 Drawdown Periods</h2>"
        html_content += pio.to_html(top_5_drawdowns_fig, full_html=False)

        # Underwater Plot
        underwater_fig = self.plot_underwater(strategy_returns.cumsum())
        html_content += "<h2>Underwater Plot (Cumulative Drawdowns)</h2>"
        html_content += pio.to_html(underwater_fig, full_html=False)

        # Monthly Returns Heatmap
        monthly_heatmap_fig = self.plot_monthly_returns_heatmap(strategy_returns)
        html_content += "<h2>Monthly Returns Heatmap</h2>"
        html_content += pio.to_html(monthly_heatmap_fig, full_html=False)

        # Return Quantiles
        return_quantiles_fig = self.plot_return_quantiles(strategy_returns)
        html_content += "<h2>Return Quantiles</h2>"
        html_content += pio.to_html(return_quantiles_fig, full_html=False)

        # Close the left section
        html_content += "</div>"

        # Open the right section for tables
        html_content += "<div class='right'>"

        # Key Performance Metrics Table
        performance_metrics = {
            "Metric": ["Sharpe", "Sortino", "CAGR", "Max Drawdown", "Volatility", "Calmar"],
            "Strategy": [
                self.stats.sharpe(strategy_returns),
                self.stats.sortino(strategy_returns),
                self.stats.cagr(strategy_returns),
                self.stats.max_drawdown(strategy_returns.cumsum()),
                strategy_returns.std(),
                self.stats.calmar(strategy_returns, drawdown_series)
            ],
            "Benchmark": [
                self.stats.sharpe(benchmark_returns),
                self.stats.sortino(benchmark_returns),
                self.stats.cagr(benchmark_returns),
                self.stats.max_drawdown(benchmark_returns.cumsum()),
                benchmark_returns.std(),
                self.stats.calmar(benchmark_returns, drawdown_series)
            ]
        }
        perf_metrics_df = pd.DataFrame(performance_metrics)
        html_content += "<h2>Key Performance Metrics</h2>"
        html_content += perf_metrics_df.to_html(index=False)

        # Additional Metric Tables
        other_metrics = {
            "Metric": ["Payoff Ratio", "Tail Ratio", "Common Sense Ratio", "Risk of Ruin", "Gain to Pain Ratio"],
            "Value": [
                self.stats.payoff_ratio(strategy_returns),
                self.stats.tail_ratio(strategy_returns),
                self.stats.common_sense_ratio(strategy_returns, drawdown_series),
                self.stats.risk_of_ruin(strategy_returns),
                self.stats.gain_to_pain_ratio(strategy_returns)
            ]
        }
        other_metrics_df = pd.DataFrame(other_metrics)
        html_content += "<h2>Additional Performance Metrics</h2>"
        html_content += other_metrics_df.to_html(index=False)

        # Add EOY Returns Table
        eoy_table = pd.DataFrame({
            "Year": strategy_returns.resample('Y').sum().index.year,
            "Benchmark": benchmark_returns.resample('Y').sum().values,
            "Strategy": strategy_returns.resample('Y').sum().values
        })
        eoy_table_html = eoy_table.to_html(index=False)
        html_content += "<h2>EOY Returns Table</h2>"
        html_content += eoy_table_html

        # Add Drawdown Details Table
        drawdown_table = self.stats.drawdown_details(strategy_returns)
        drawdown_table_html = drawdown_table.to_html(index=False)
        html_content += "<h2>Worst Drawdown Periods</h2>"
        html_content += drawdown_table_html

        # Close the right section and container
        html_content += "</div></div>"

        # End of HTML content
        html_content += "</body></html>"

        # Write to HTML file
        with open(output_path, 'w') as file:
            file.write(html_content)

        print(f"Comprehensive report generated at {output_path}")


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

# quantstats_plotly/report.py

import plotly.io as pio
import pandas as pd
from .stats import *
from .plots import *

def generate_report(returns, drawdown_series, benchmark_returns, output_path='report.html'):
    html_content = "<html><head><title>QuantStats Report</title></head><body>"
    
    # Add performance metrics
    html_content += f"<h2>Statistics</h2>"
    html_content += f"<p><strong>Average Return:</strong> {avg_return(returns):.2f}</p>"
    html_content += f"<p><strong>Sharpe Ratio:</strong> {sharpe(returns):.2f}</p>"
    html_content += f"<p><strong>Max Drawdown:</strong> {max_drawdown(drawdown_series):.2f}</p>"

    # Generate and embed cumulative returns
    cumulative_returns_fig, cumulative_returns_log_fig = plot_cumulative_returns(returns, benchmark_returns)
    html_content += pio.to_html(cumulative_returns_fig, full_html=False)
    html_content += pio.to_html(cumulative_returns_log_fig, full_html=False)

    # Drawdown plot
    drawdown_fig = plot_underwater(drawdown_series)
    html_content += pio.to_html(drawdown_fig, full_html=False)

    # Monthly heatmap
    monthly_heatmap_fig = plot_monthly_returns_heatmap(returns)
    html_content += pio.to_html(monthly_heatmap_fig, full_html=False)

    # Distribution plot
    distribution_fig = plot_distribution_of_monthly_returns(returns)
    html_content += pio.to_html(distribution_fig, full_html=False)

    # End of HTML
    html_content += "</body></html>"

    with open(output_path, 'w') as file:
        file.write(html_content)

    print(f"Report saved to {output_path}")

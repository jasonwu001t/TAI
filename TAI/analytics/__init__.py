# analytics/__init__.py
from .data_analytics import DataAnalytics
from .plotly_plots import QuickPlot,QuantStatsPlot

__all__ = [
    'DataAnalytics',
    'QuickPlot',
    'QuantStatsPlot'
]

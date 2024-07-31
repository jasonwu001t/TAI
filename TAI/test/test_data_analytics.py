import pytest
import pandas as pd
from TAI.analytics.data_analytics import DataAnalytics

def test_data_analytics():
    data = {
        'date_time': ['2023-04-01', '2023-04-02', '2023-04-03', '2023-04-04', '2023-04-05'],
        'value1': [22, 4, 6, 82, 10],
        'value2': [43, 12, 56, 12, 44]
    }
    df = pd.DataFrame(data)
    processor = DataAnalytics(df)

    assert processor.correlation_coefficient(col1='value1', col2='value2') is not None
    assert processor.pvalue(col1='value1', col2='value2') is not None
    assert processor.coefficient_of_variation(col='value1') is not None
    assert processor.z_score(col='value1', value=50) is not None
    assert processor.add_slope_column(window=3) is not None
    assert processor.get_slope() is not None
    assert processor.perc_change(horizon=3, keep_only_perc_change=True) is not None
    assert processor.describe_df() is not None
    assert processor.describe_df_forecast(input_value=50) is not None

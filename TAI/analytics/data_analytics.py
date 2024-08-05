import pandas as pd
import numpy as np
from scipy.stats import linregress
from statsmodels.tsa.stattools import coint
import datetime
import logging

logging.basicConfig(level=logging.INFO)

class DataAnalytics:
    def __init__(self):
        pass

    def process_daily_data(df, fill_missing=False, fill_method='ffill', use_first=True):
        # Ensure there is at least one column
        if df.shape[1] < 2:
            raise ValueError("DataFrame must have at least two columns: date and value.")
        
        # Convert the first column to datetime and set as index
        date_column = df.columns[0]
        value_column = df.columns[1]
        df[date_column] = pd.to_datetime(df[date_column])
        df.set_index(date_column, inplace=True)

        if fill_missing:
            # Fill missing dates if required
            df = df.asfreq('D', method=fill_method)

        # Group by month and calculate monthly percentage change
        monthly_data = df.resample('M').apply(lambda x: x.iloc[0] if use_first else x.iloc[-1])
        monthly_data['pct_change'] = monthly_data[value_column].pct_change()

        # Calculate monthly average value
        monthly_average = df.resample('M').mean()
        monthly_data['monthly_average'] = monthly_average[value_column]

        return df.reset_index(), monthly_data.reset_index()
        """
        # Sample usage
        data = {'date': ['2024-08-01', '2024-08-02', '2024-08-05'], 'value': [100, 102, 104]}
        df = pd.DataFrame(data)

        # Process data without filling missing dates
        daily_data, monthly_data = process_daily_data(df, fill_missing=False, use_first=True)

        print("Daily Data (without filling missing dates):")
        print(daily_data)

        print("\nMonthly Data with Percentage Change and Average (without filling missing dates):")
        print(monthly_data)

        # Process data with filling missing dates
        daily_data_filled, monthly_data_filled = process_daily_data(df, fill_missing=True, fill_method='ffill', use_first=True)

        print("\nDaily Data (with filling missing dates):")
        print(daily_data_filled)

        print("\nMonthly Data with Percentage Change and Average (with filling missing dates):")
        print(monthly_data_filled)
        """

    @staticmethod
    def get_pct_weights(df):
        """
        Calculate percentage weights of numerical columns.
        
        :param df: DataFrame with at least two columns: date/datetime and numerical columns.
        :return: DataFrame with percentage weights of each column.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-04-01 |     22 |     43 |
        | 2023-04-02 |      4 |     12 |
        """
        date_column = df.columns[0]
        df.set_index(date_column, inplace=True)
        non_date_columns = df.columns
        df['row_sum'] = df[non_date_columns].sum(axis=1)
        for column in non_date_columns:
            df[f'{column}_pct'] = (df[column] / df['row_sum']) * 100
        df_pct = df[[f'{column}_pct' for column in non_date_columns]].reset_index()  # only show the _pct columns
        return df_pct.round(2)

    @staticmethod
    def filter_period(df, period='Q'):
        """
        Filter data to show only the last non-null value of each period.
        
        :param df: DataFrame with at least two columns: date/datetime and a numerical column.
        :param period: Resampling period, 'Q' for quarterly, 'A' for yearly.
        :return: DataFrame with resampled data.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-01-01 |     22 |     43 |
        | 2023-04-01 |      4 |     12 |
        """
        date_column = df.columns[0]
        df[date_column] = pd.to_datetime(df[date_column])
        df.set_index(date_column, inplace=True)

        def last_non_null(s):
            return s.dropna().iloc[-1]
        
        period_data = df.resample(period).agg(last_non_null)
        period_data.reset_index(inplace=True)

        period_label = "quarter" if period == 'Q' else "year"
        period_data[period_label] = period_data[date_column].dt.to_period(period).astype(str)
        return period_data

    @staticmethod
    def calculate_category_weights(df):
        """
        Calculate the percentage weight for each category for each datetime.
        
        :param df: DataFrame with columns A (datetime), B (category), C (value).
        :return: DataFrame with percentage weights for each category for each datetime.
        
        Input sample:
        | datetime   | category | value |
        |------------|----------|-------|
        | 2023-04-01 | A        | 100   |
        | 2023-04-01 | B        | 200   |
        sample output:
        datetime	category	value	percentage
    	2023-04-01	A	100	33.333333
    	2023-04-01	B	200	66.666667
    	2023-04-02	A	150	37.500000
    	2023-04-02	B	250	62.500000
    	2023-04-03	A	300	100.000000
        """
        total = df.groupby(df.columns[0])[df.columns[2]].transform('sum')
        # Calculate percentage
        df['percentage'] = (df['value'] / total) * 100
        
        return df
    
    @staticmethod
    def dummy_value():
        """
        Generate a dummy DataFrame for testing purposes.
        
        :return: DataFrame with dummy date and value data.
        """
        dates = pd.date_range(start='2023-04-24', periods=100, freq='D')
        integers = np.random.randint(0, 100, 100)
        df = pd.DataFrame({'Date': dates, 'Value': integers})
        return df

    @staticmethod
    def full_join_multi_tables(dfs):
        """
        Perform a full join on multiple DataFrames.
        
        :param dfs: List of DataFrames to join.
        :return: DataFrame with joined data.
        
        Input sample:
        dfs = [df1, df2]
        df1:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        
        df2:
        | date_time  | value2 |
        |------------|--------|
        | 2023-04-01 |     43 |
        | 2023-04-02 |     12 |
        """
        on_col = dfs[0].columns[0]
        result = dfs[0]
        for df in dfs[1:]:
            result = result.merge(df, on=on_col, how='outer')
        result = result.sort_values(by=on_col)
        result = result.ffill()
        return result

    @staticmethod
    def covariance(df, col1, col2, window=None):
        """
        Calculate covariance between two columns.
        
        :param df: DataFrame containing the data.
        :param col1: First column name.
        :param col2: Second column name.
        :param window: Rolling window size.
        :return: Covariance value or array of values.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-04-01 |     22 |     43 |
        | 2023-04-02 |      4 |     12 |
        """
        if window is not None:
            return df[[col1, col2]].rolling(window=window).cov().iloc[::window].iloc[:, 1].values
        else:
            return df[[col1, col2]].cov().iloc[0, 1]

    @staticmethod
    def correlation_coefficient(df, col1, col2, window=None):
        """
        Calculate correlation coefficient between two columns.
        
        :param df: DataFrame containing the data.
        :param col1: First column name.
        :param col2: Second column name.
        :param window: Rolling window size.
        :return: Correlation coefficient value or array of values.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-04-01 |     22 |     43 |
        | 2023-04-02 |      4 |     12 |
        """
        if window is not None:
            return df[[col1, col2]].rolling(window=window).corr().iloc[::window].iloc[:, 1].values
        else:
            return df[[col1, col2]].corr().iloc[0, 1]

    @staticmethod
    def pvalue(df, col1, col2):
        """
        Calculate p-value for cointegration test between two columns.
        
        :param df: DataFrame containing the data.
        :param col1: First column name.
        :param col2: Second column name.
        :return: P-value.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-04-01 |     22 |     43 |
        | 2023-04-02 |      4 |     12 |
        """
        S1 = df[col1]
        S2 = df[col2]
        score, pvalues, _ = coint(S1, S2)
        return pvalues

    @staticmethod
    def coefficient_of_variation(df, col, window=None):
        """
        Calculate coefficient of variation for a column.
        
        :param df: DataFrame containing the data.
        :param col: Column name.
        :param window: Rolling window size.
        :return: Coefficient of variation value or array of values.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        if window is not None:
            mean = df[col].rolling(window=window).mean()
            std_dev = df[col].rolling(window=window).std()
            return (std_dev / mean).values
        else:
            mean = df[col].mean()
            std_dev = df[col].std()
            return std_dev / mean

    @staticmethod
    def z_score(df, col, value, window=None):
        """
        Calculate z-score for a value in a column.
        
        :param df: DataFrame containing the data.
        :param col: Column name.
        :param value: Value to calculate z-score for.
        :param window: Rolling window size.
        :return: Z-score value or array of values.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        if window is not None:
            mean = df[col].rolling(window=window).mean()
            std_dev = df[col].rolling(window=window).std()
            return ((value - mean) / std_dev).values
        else:
            mean = df[col].mean()
            std_dev = df[col].std()
            return (value - mean) / std_dev

    @staticmethod
    def add_slope_column(df, window):  #Does not work, need to fix
        """
        Add a column with slope values calculated over a rolling window.
        
        :param df: DataFrame with at least two columns: date/datetime and a numerical column.
        :param window: Rolling window size.
        :return: DataFrame with date and slope columns.
        """
        df['date_ordinal'] = pd.to_datetime(df[df.columns[0]]).apply(lambda x: x.toordinal())
        print (df)
        slopes = []
        for i in range(len(df) - window + 1):
            y = df[df.columns[1]].iloc[i:i + window]
            x = df['date_ordinal'].iloc[i:i + window]
            slope, _, _, _, _ = linregress(x, y)
            slopes.append(slope)

        slopes = [None] * (window - 1) + slopes  # Prepend Nones to match the length of the original dataframe
        df['slope'] = slopes
        df.drop(columns=['date_ordinal'], inplace=True)  # Clean up the temporary column

        return df[[df.columns[0], 'slope']]

    @staticmethod
    def get_slope(df):
        """
        Calculate slope and intercept for a column.
        
        :return: Tuple of slope and intercept.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        df.set_index(df.columns[0])
        df['date_ordinal'] = df.index.map(datetime.datetime.toordinal)
        slope, intercept, _, _, _ = linregress(df['date_ordinal'], df.iloc[:, 0])
        return slope, intercept

    @staticmethod
    def perc_change(df, horizon, keep_only_perc_change=True):
        """
        Calculate percentage change over a horizon.
        
        :param df: DataFrame with at least two columns: date/datetime and a numerical column.
        :param horizon: Number of periods to calculate percentage change over.
        :param keep_only_perc_change: If True, keep only percentage change column.
        :return: DataFrame with percentage change.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        horizon = horizon - 1
        df['perc_change'] = df.iloc[:, 0].pct_change(periods=horizon, fill_method='ffill').round(decimals=4)
        if keep_only_perc_change:
            return df.drop(df.columns[0], axis=1).dropna()
        else:
            return df.dropna()

    @staticmethod
    def describe_df(df):
        """
        Describe the DataFrame, including percentiles.
        
        :param df: DataFrame with at least two columns: date/datetime and a numerical column.
        :return: DataFrame with descriptive statistics.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        return df.iloc[:, 1].describe(percentiles=[.001, .01, .05, .1, .15, .25, .5, .75, .85, .9, .95, .99, .999]).round(2).reset_index()

    @staticmethod
    def describe_df_forecast(df, input_value):
        """
        Describe the DataFrame and forecast values based on input value.
        
        :param df: DataFrame with at least two columns: date/datetime and a numerical column.
        :param input_value: Input value to forecast based on descriptive statistics.
        :return: DataFrame with forecast values.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        describe = DataAnalytics.describe_df(df)
        describe['input_value'] = input_value
        describe['forecast_value'] = input_value * (1 + describe.iloc[:, 1] / 100).round(3)
        return describe

# Example usage in __main__ section
if __name__ == "__main__":
    from GTI.analytics.plotly_plots import QuickPlot

    # Dummy data
    data1 = {
        'date': pd.date_range(start='1/1/2020', periods=100),
        'value': np.random.rand(100)
    }
    data2 = {
        'date': pd.date_range(start='1/1/2020', periods=100),
        'value': np.random.rand(100) * 2
    }
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    # Initialize DataAnalytics
    processor = DataAnalytics()

    # Test the methods in DataAnalytics
    print("Correlation coefficient without window:")
    print(DataAnalytics.correlation_coefficient(df1, col1='value', col2='value'))

    print("\nP-value:")
    print(DataAnalytics.pvalue(df1, col1='value', col2='value'))

    print("\nCoefficient of variation without window:")
    print(DataAnalytics.coefficient_of_variation(df1, col='value'))

    print("\nZ-score without window:")
    print(DataAnalytics.z_score(df1, col='value', value=0.5))

    print("\nAdd slope column with window=3:")
    print(DataAnalytics.add_slope_column(df1, window=3))

    print("\nGet slope:")
    print(DataAnalytics.get_slope(df1))

    print("\nPercentage change with horizon=3:")
    print(DataAnalytics.perc_change(df1, horizon=3, keep_only_perc_change=True))

    print("\nDescribe DataFrame:")
    print(DataAnalytics.describe_df(df1))

    print("\nDescribe DataFrame forecast with input value=100:")
    print(DataAnalytics.describe_df_forecast(df1, input_value=50))

    # Initialize QuickPlot
    qp = QuickPlot()

    # Test the methods in QuickPlot
    # Plot line
    fig_line = qp.plot_line([df1, df2], labels=["Series 1", "Series 2"], title="Sample Line Plot")
    fig_line.show()

    # Plot line with events
    events = {
        '2020-01-10': 'Event A',
        '2020-02-20': 'Event B',
        '2020-03-30': 'Event C'
    }
    fig_line_with_events = qp.plot_line_with_events([df1, df2], labels=["Series 1", "Series 2"], title="Line Plot with Events", events=events)
    fig_line_with_events.show()

    # Plot monthly heatmap
    def create_sample_data():
        np.random.seed(42)
        date_rng = pd.date_range(start='2020-01-01', end='2024-12-31', freq='M')
        values = np.random.randn(len(date_rng)) * 10  # Random returns
        df = pd.DataFrame({'date': date_rng, 'value': values})
        df['Year'] = df['date'].dt.year
        df['Month'] = df['date'].dt.strftime('%b')
        return df
    
    df_monthly = create_sample_data()
    qp.plot_monthly_heatmap(df_monthly)

    # Plot interest rates
    rates_data = {
        'Today': df1,
        'Last Week': df2,
        'Last Month': df1,
        'Last 3 Months': df2,
        'Last Year': df1
    }
    fig_rates = qp.plot_interest_rates(rates_data)
    fig_rates.show()

    # Plot bar
    fig_bar = qp.plot_bar([df1, df2], labels=["Series 1", "Series 2"], title="Sample Bar Plot")
    fig_bar.show()

    # Plot scatter
    fig_scatter = qp.plot_scatter([df1, df2], labels=["Series 1", "Series 2"], title="Sample Scatter Plot")
    fig_scatter.show()

    # Prepare custom figure
    fig_custom = qp.prepare_figure(df1, title="Sample Custom Plot")
    fig_custom.show()
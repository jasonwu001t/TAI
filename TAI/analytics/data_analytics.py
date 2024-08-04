# data_analytics.py
import pandas as pd
import numpy as np
from scipy.stats import linregress
from statsmodels.tsa.stattools import coint
import datetime
import logging

logging.basicConfig(level=logging.INFO)

class DataAnalytics:
    def __init__(self, df, ts=True):
        """
        Initialize the DataAnalytics class with a DataFrame.
        
        :param df: DataFrame with at least two columns: date/datetime and a numerical column.
        """
        if ts == True: #apply to time series data only
            self.date_column = df.columns[0]
            self.value_column = df.columns[1]
            df[self.date_column] = pd.to_datetime(df[self.date_column])
            # df.set_index(date_column, inplace=True)
            self.df = df.copy()
        else:
            self.df =df.copy()

    def get_pct_weights(self): #works
        """
        Calculate percentage weights of numerical columns.
        
        :return: DataFrame with percentage weights of each column.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-04-01 |     22 |     43 |
        | 2023-04-02 |      4 |     12 |
        """
        self.df.set_index(self.date_column,inplace=True)
        non_date_columns = self.df.columns
        self.df['row_sum'] = self.df[non_date_columns].sum(axis=1)
        for column in non_date_columns:
            self.df[f'{column}_pct'] = (self.df[column] / self.df['row_sum']) * 100
        df_pct = self.df[[f'{column}_pct' for column in non_date_columns]].reset_index() #only show the _pct columns
        return df_pct.round(2)

    def filter_period(self, period='Q'):  #works, period= Q or A for yearly
        """
        Filter data to show only the last non-null value of each period.
        
        :param period: Resampling period, 'Q' for quarterly, 'A' for yearly.
        :return: DataFrame with resampled data.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-01-01 |     22 |     43 |
        | 2023-04-01 |      4 |     12 |
        """
        # date_column = df.columns[0]
        # df[date_column] = pd.to_datetime(df[date_column])
        # df.set_index(date_column, inplace=True)

        def last_non_null(s):
            return s.dropna().iloc[-1]
        
        self.df.set_index(self.date_column,inplace=True)
        period_data = self.df.resample(period).agg(last_non_null)
        period_data.reset_index(inplace=True)

        period_label = "quarter" if period == 'Q' else "year"
        period_data[period_label] = period_data[self.date_column].dt.to_period(period).astype(str)
        return period_data

    def calculate_category_weights(self): #works
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
        total = self.df.groupby(self.df.columns[0])[self.df.columns[2]].transform('sum')
        # Calculate percentage
        self.df['percentage'] = (self.df['value'] / total) * 100
        
        return self.df
    
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

    def correlation_coefficient(self, col1, col2, window=None):
        """
        Calculate correlation coefficient between two columns.
        
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
            return self.df[[col1, col2]].rolling(window=window).corr().iloc[::window].iloc[:, 1].values
        else:
            return self.df[[col1, col2]].corr().iloc[0, 1]

    def pvalue(self, col1, col2):
        """
        Calculate p-value for cointegration test between two columns.
        
        :param col1: First column name.
        :param col2: Second column name.
        :return: P-value.
        
        Input sample:
        | date_time  | value1 | value2 |
        |------------|--------|--------|
        | 2023-04-01 |     22 |     43 |
        | 2023-04-02 |      4 |     12 |
        """
        S1 = self.df[col1]
        S2 = self.df[col2]
        score, pvalues, _ = coint(S1, S2)
        return pvalues

    def coefficient_of_variation(self, col, window=None):
        """
        Calculate coefficient of variation for a column.
        
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
            mean = self.df[col].rolling(window=window).mean()
            std_dev = self.df[col].rolling(window=window).std()
            return (std_dev / mean).values
        else:
            mean = self.df[col].mean()
            std_dev = self.df[col].std()
            return std_dev / mean

    def z_score(self, col, value, window=None):
        """
        Calculate z-score for a value in a column.
        
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
            mean = self.df[col].rolling(window=window).mean()
            std_dev = self.df[col].rolling(window=window).std()
            return ((value - mean) / std_dev).values
        else:
            mean = self.df[col].mean()
            std_dev = self.df[col].std()
            return (value - mean) / std_dev

    def add_slope_column(self, window):
        """
        Add a column with slope values calculated over a rolling window.
        
        :param window: Rolling window size.
        :return: DataFrame with date, value, and slope columns.
        """
        df = self.df.copy()
        df['date_ordinal'] = pd.to_datetime(df['date']).apply(lambda x: x.toordinal())
        
        slopes = []
        for i in range(len(df) - window + 1):
            y = df['value'].iloc[i:i + window]
            x = df['date_ordinal'].iloc[i:i + window]
            slope, _, _, _, _ = linregress(x, y)
            slopes.append(slope)

        slopes = [None] * (window - 1) + slopes  # Prepend Nones to match the length of the original dataframe
        df['slope'] = slopes
        df.drop(columns=['date_ordinal'], inplace=True)  # Clean up the temporary column

        return df[['date', 'slope']]

    def get_slope(self):
        """
        Calculate slope and intercept for a column.
        
        :return: Tuple of slope and intercept.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        df = self.df.copy()
        df['date_ordinal'] = df.index.map(datetime.datetime.toordinal)
        slope, intercept, _, _, _ = linregress(df['date_ordinal'], df.iloc[:, 0])
        return slope, intercept

    def perc_change(self, horizon, keep_only_perc_change=True):
        """
        Calculate percentage change over a horizon.
        
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
        self.df['perc_change'] = self.df.iloc[:, 0].pct_change(periods=horizon, fill_method='ffill').round(decimals=4)
        if keep_only_perc_change:
            return self.df.drop(self.df.columns[0], axis=1).dropna()
        else:
            return self.df.dropna()

    def describe_df(self):
        """
        Describe the DataFrame, including percentiles.
        
        :return: DataFrame with descriptive statistics.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        return self.df.iloc[:, 1].describe(percentiles=[.001, .01, .05, .1, .15, .25, .5, .75, .85, .9, .95, .99, .999]).round(2).reset_index()

    def describe_df_forecast(self, input_value):
        """
        Describe the DataFrame and forecast values based on input value.
        
        :param input_value: Input value to forecast based on descriptive statistics.
        :return: DataFrame with forecast values.
        
        Input sample:
        | date_time  | value1 |
        |------------|--------|
        | 2023-04-01 |     22 |
        | 2023-04-02 |      4 |
        """
        describe = self.describe_df()
        describe['input_value'] = input_value
        describe['forecast_value'] = input_value * (1 + describe.iloc[:, 1] / 100).round(3)
        return describe

# Example usage in __main__ section
if __name__ == "__main__":
    # Dummy data
    data = {
        'date_time': ['2023-04-01', '2023-04-02', '2023-04-03', '2023-04-04', '2023-04-05'],
        'value1': [22, 4, 6, 82, 10],
        'value2': [43, 12, 56, 12, 44]
    }
    df = pd.DataFrame(data)

    # Instantiate the DataAnalytics class
    processor = DataAnalytics(df)

    # Test the methods
    print("Correlation coefficient without window:")
    print(processor.correlation_coefficient(col1='value1', col2='value2'))

    print("\nP-value:")
    print(processor.pvalue(col1='value1', col2='value2'))

    print("\nCoefficient of variation without window:")
    print(processor.coefficient_of_variation(col='value1'))

    print("\nZ-score without window:")
    print(processor.z_score(col='value1', value=50))

    print("\nAdd slope column with window=3:")
    print(processor.add_slope_column(window=3))

    print("\nGet slope:")
    print(processor.get_slope())

    print("\nPercentage change with horizon=3:")
    print(processor.perc_change(horizon=3, keep_only_perc_change=True))

    print("\nDescribe DataFrame:")
    print(processor.describe_df())

    print("\nDescribe DataFrame forecast with input value=100:")
    print(processor.describe_df_forecast(input_value=50))

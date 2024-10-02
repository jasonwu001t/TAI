import pandas as pd
import numpy as np
from datetime import datetime, date
from TAI.source import Alpaca
pd.set_option('display.max_columns', None)


class OptionBet:
    def __init__(self, ticker, expiry_date, lookback_period):
        self.ap = Alpaca()

        self.ticker = ticker
        self.expiry_date = expiry_date
        self.lookback_period = lookback_period

        self.today = date.today()
        self.today_str = self.today.strftime("%Y-%m-%d")
        self.df_raw = self.ap.get_stock_historical(symbol_or_symbols=self.ticker,
                                                   lookback_period=self.lookback_period,
                                                   timeframe='Day',
                                                   end=None,
                                                   currency='USD',
                                                   limit=None,
                                                   adjustment='all',
                                                   feed=None,
                                                   asof=None,
                                                   sort='asc',
                                                   raw=False)

        self.df = self.df_raw[['timestamp', 'open', 'high',
                               'low', 'close', 'volume']].set_index('timestamp')
        self.current_price = self.ap.get_latest_trade(
            self.ticker).get(self.ticker).price
        self.open = self.df.tail(1)['open'][0]

        self.df_chain = self.ap.get_option_chain(
            underlying_symbol=ticker,
            feed=None,
            type=None,
            strike_price_gte=None,
            strike_price_lte=None,
            expiration_date=expiry_date,
            expiration_date_gte=None,
            expiration_date_lte=None,
            root_symbol=None,
            raw=False,
            chain_table=True)
        self.all_strike_prices = self.df_chain['Strike'].to_list()

    # Function to find the nearest strike price
    def find_nearest_strike(self, value, strike_prices):
        return min(strike_prices, key=lambda x: abs(x - value))

    # Find the nearest absolute value, eg. 560.2 = 560
    def nearest_value(self, input_list, find_value):
        def difference(input_list): return abs(input_list - find_value)
        return min(input_list, key=difference)

    def weekdays_calculator(self, end_str):
        end = datetime.strptime(end_str, '%Y-%m-%d').date()
        return np.busday_count(self.today, end)

    def perc_change(self, df, horizon):
        df['return_perc'] = df.pct_change(
            periods=horizon - 1, fill_method='ffill').round(decimals=4)['close']
        return df.dropna()

    def describe_perc_change(self):
        horizon = self.weekdays_calculator(self.expiry_date)
        df_perc_change = self.perc_change(self.df, horizon)[
            ['close', 'return_perc']]
        select_price = self.current_price
        describe = df_perc_change.describe(
            percentiles=[.001, .01, .05, .1, .15, .25, .5, .75, .85, .9, .95, .99, .999])
        describe['current_price'] = select_price
        describe['projected_price'] = select_price * \
            (1 + describe['return_perc'])
        describe['percentage_change'] = describe['return_perc'] * 100
        describe['chose_strike'] = describe['projected_price'].astype(int)
        # Apply the function to the 'chose_strike' column to find the nearest strike price
        describe['matched_strike'] = describe['chose_strike'].apply(
            self.find_nearest_strike, args=(self.all_strike_prices,))
        df_merged = pd.merge(describe, self.df_chain, left_on='matched_strike',
                             right_on='Strike', how='left', left_index=False)

        df_merged.index = describe.index
        df_merged['percentage_change'] = df_merged['percentage_change'].astype(
            float).round(2)
        df_merged['projected_price'] = df_merged['projected_price'].astype(
            float).round(2)
        cols_to_clear = ['projected_price', 'chose_strike',
                         'matched_strike', 'Call Bid', 'Call Ask', 'Put Bid', 'Put Ask']
        rows_to_clear = ['count', 'mean', 'std', 'min']

        # Set values to NaN for the specified rows and columns
        df_merged.loc[rows_to_clear, cols_to_clear] = np.nan
        return df_merged[['current_price', 'percentage_change', 'projected_price', 'chose_strike', 'matched_strike', 'Call Bid', 'Call Ask', 'Put Bid', 'Put Ask']]


# Example usage
if __name__ == "__main__":
    ticker = 'TSLA'
    chose_expiry_date = '2024-12-20'
    lookback_period = 1000
    option_bet = OptionBet(ticker, chose_expiry_date, lookback_period)
    describe_df = option_bet.describe_perc_change()
    print(describe_df)

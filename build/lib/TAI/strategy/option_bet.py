import pandas as pd
import numpy as np
from datetime import datetime, date
from TAI.broker.ib import IBTrade

class OptionBet:
    def __init__(self, ticker, expiry_date):
        self.ib = IBTrade()
        self.ticker = ticker
        self.expiry_date = expiry_date
        self.today = date.today()
        self.today_str = self.today.strftime("%Y-%m-%d")

    def nearest_value(self, input_list, find_value):
        difference = lambda input_list: abs(input_list - find_value)
        return min(input_list, key=difference)

    def weekdays_calculator(self, end_str):
        today = datetime.today().date()
        end = datetime.strptime(end_str, '%Y-%m-%d').date()
        return np.busday_count(today, end)

    def historical_data(self):
        return self.ib.get_historical_daily_bar(self.ticker, start_date='2020-01-01', end_date=self.today_str)

    def current_price(self):
        return self.ib.get_latest_price(self.ticker)

    def open_price(self):
        return self.ib.get_open_price(self.ticker)

    def perc_change(self, df, horizon):
        df['return_perc'] = df.pct_change(periods=horizon - 1, fill_method='ffill').round(decimals=4)['Close']
        return df.dropna()

    def describe_perc_change(self):
        df = self.historical_data()
        horizon = self.weekdays_calculator(self.expiry_date)
        df_perc_change = self.perc_change(df, horizon)[['Close', 'return_perc']]
        select_price = self.current_price()
        describe = df_perc_change.describe(percentiles=[.001, .01, .05, .1, .15, .25, .5, .75, .85, .9, .95, .99, .999])
        describe['current_price'] = select_price
        describe['projected_price'] = select_price * (1 + describe['return_perc'])
        describe['percentage_change'] = describe['return_perc'] * 100
        describe['chose_strike'] = describe['projected_price'].astype(int)
        describe = describe.rename(columns={
            'Close': "current_price",
            'return_perc': 'percentage_change',
            'projected_price': 'projected_price',
            'chose_strike': 'chose_strike'
        })
        return describe[['current_price', 'percentage_change', 'projected_price', 'chose_strike']]

# Example usage
if __name__ == "__main__":
    ticker = 'TSLA'
    chose_expiry_date = '2024-08-02'
    option_bet = OptionBet(ticker, chose_expiry_date)
    describe_df = option_bet.describe_perc_change()
    print(describe_df)

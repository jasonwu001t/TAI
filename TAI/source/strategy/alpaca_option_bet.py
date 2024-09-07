import pandas as pd
import numpy as np
from datetime import datetime, date
from TAI.source import Alpaca

class AlpacaOptionBet:
    def __init__(self, ticker, expiry_date):
        self.ap = Alpaca()
    
        self.ticker = ticker
        self.expiry_date = expiry_date
        
        self.today = date.today()
        self.today_str = self.today.strftime("%Y-%m-%d")
        self.df_raw = self.ap.get_stock_historical(symbol_or_symbols=self.ticker, 
                                                lookback_period=720, 
                                                timeframe='Day', 
                                                end=None, 
                                                currency='USD', 
                                                limit=None, 
                                                adjustment='all', 
                                                feed=None, 
                                                asof=None, 
                                                sort='asc', 
                                                raw=False)
        
        self.df = self.df_raw[['timestamp','open','high','low','close','volume']].set_index('timestamp')
        self.current_price = self.ap.get_latest_trade(self.ticker).get(self.ticker).price
        self.open=self.df.tail(1)['open'][0]

    def nearest_value(self, input_list, find_value):
        difference = lambda input_list: abs(input_list - find_value)
        return min(input_list, key=difference)

    def weekdays_calculator(self, end_str):
        end = datetime.strptime(end_str, '%Y-%m-%d').date()
        return np.busday_count(self.today, end)

    def perc_change(self, df, horizon):
        df['return_perc'] = df.pct_change(periods=horizon - 1, fill_method='ffill').round(decimals=4)['close']
        return df.dropna()

    def describe_perc_change(self):
        horizon = self.weekdays_calculator(self.expiry_date)
        df_perc_change = self.perc_change(self.df, horizon)[['close', 'return_perc']]
        select_price = self.current_price
        describe = df_perc_change.describe(percentiles=[.001, .01, .05, .1, .15, .25, .5, .75, .85, .9, .95, .99, .999])
        describe['current_price'] = select_price
        describe['projected_price'] = select_price * (1 + describe['return_perc'])
        describe['percentage_change'] = describe['return_perc'] * 100
        describe['chose_strike'] = describe['projected_price'].astype(int)
        return describe[['current_price', 'percentage_change', 'projected_price', 'chose_strike']]


# Example usage
if __name__ == "__main__":
    ticker = 'SPY'
    chose_expiry_date = '2024-09-20'
    option_bet = OptionBet(ticker, chose_expiry_date)
    describe_df = option_bet.describe_perc_change()
    print(describe_df)

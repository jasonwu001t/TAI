from TAI.utils import ConfigLoader
import logging
from ib_insync import IB, Stock, Option, util
import pandas as pd
from datetime import datetime, timedelta
import math
# from GTI.auth_handler import IBAuth

class IBAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        # print (config_loader.get_config('IB', 'host'))
        self.host = config_loader.get_config('IB', 'host')
        self.port = config_loader.get_config('IB', 'port')
        self.client_id = config_loader.get_config('IB', 'client_id')

    def connect(self):
        ib = IB()
        ib.connect(self.host, int(self.port), clientId=int(self.client_id))
        return ib

class IBTrade:
    def __init__(self):
        logging.getLogger('ib_insync').setLevel(logging.ERROR)  # suppress information logs output each time connection, it will show position information
        util.startLoop()
        ib_auth = IBAuth()
        self.ib = ib_auth.connect()

    def get_stock_contract(self, symbol):
        return Stock(symbol, 'SMART', 'USD')

    def get_stock_quote(self, symbol):
        contract = self.get_stock_contract(symbol)
        ticker = self.ib.reqMktData(contract)
        self.ib.sleep(1)  # Let's wait for a moment to make sure the ticker updates
        return ticker

    def get_historical_data(self, contract, duration_str, bar_size_setting, end_date_time='', what_to_show='TRADES', use_rth=True, format_date=1):
        try:
            bars = self.ib.reqHistoricalData(contract, endDateTime=end_date_time, durationStr=duration_str, barSizeSetting=bar_size_setting, whatToShow=what_to_show, useRTH=use_rth, formatDate=format_date)
            df = util.df(bars)
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
            df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            return df.set_index('Date')
        except Exception as e:
            print(f"Error fetching historical data: {e}")
            return pd.DataFrame()

    def get_historical_daily_bar(self, symbol, start_date, end_date):
        contract = self.get_stock_contract(symbol)
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        days_difference = (end_date - start_date).days
        if days_difference > 365:
            years = (days_difference + 364) // 365  # This will always round up
            duration_str = f"{years} Y"
        else:
            duration_str = f"{days_difference} D"
        df = self.get_historical_data(contract, duration_str, '1 day', end_date)
        df = df.loc[start_date:end_date]
        return df

    def get_latest_price(self, symbol):
        ticker = self.get_stock_quote(symbol)
        self.ib.sleep(1)  # Let's wait for a moment to make sure the ticker updates
        latest_price = ticker.last
        if math.isnan(ticker.last):
            today = datetime.today().strftime('%Y-%m-%d')
            seven_days_ago = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
            historical_bars = self.get_historical_daily_bar(symbol, seven_days_ago, today)
            latest_price = historical_bars.tail(1).Close.values[0]
        return latest_price

    def get_open_price(self, symbol):
        ticker = self.get_stock_quote(symbol)
        self.ib.sleep(1)  # Wait to ensure the ticker updates
        open_price = ticker.open  # Getting the open price
        if math.isnan(open_price):  # Check if the open price is NaN
            today = datetime.today().strftime('%Y-%m-%d')
            seven_days_ago = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
            historical_bars = self.get_historical_daily_bar(symbol, seven_days_ago, today)
            open_price = historical_bars.tail(1).Open.values[0]  # Getting the open price from historical data
        return open_price

    def get_option_base(self, symbol):
        contract = self.get_stock_contract(symbol)
        self.ib.qualifyContracts(contract)
        chains = self.ib.reqSecDefOptParams(underlyingSymbol=contract.symbol, futFopExchange='', underlyingSecType=contract.secType, underlyingConId=contract.conId)
        chains_df = util.df(chains)
        return chains_df.loc[chains_df['exchange'] == 'SMART']

    def get_option_dates(self, symbol):
        chains_df = self.get_option_base(symbol)
        expiry_dates = sorted(set(chains_df.expirations.tolist()[0]))
        return expiry_dates

    def get_option_chain(self, symbol, expiry, strike=None, right=None): # Does Not Work
        chains_df = self.get_option_base(symbol)
        strikes = chains_df['strikes'].iloc[0]  # Access the first element of the strikes list

        options = []
        for strike in strikes:
            for right in ['C', 'P']:
                option = Option(symbol, expiry, strike, right, 'SMART')
                self.ib.qualifyContracts(option)
                options.append(option)

        tickers = self.ib.reqTickers(*options)
        data = [{
            'symbol': ticker.contract.symbol,
            'expiry': ticker.contract.lastTradeDateOrContractMonth,
            'strike': ticker.contract.strike,
            'right': ticker.contract.right,
            'bid': ticker.bid,
            'ask': ticker.ask,
            'last': ticker.last,
            'volume': ticker.volume,
            'openInterest': ticker.openInterest
        } for ticker in tickers]

        return pd.DataFrame(data)

    def disconnect(self):
        self.ib.disconnect()

# Example usage in __main__ section
if __name__ == "__main__":
    ticker = 'TSLA'
    ib_handler = IBTrade()

    # Get stock quote for TSLA
    quote = ib_handler.get_stock_quote(ticker)
    print("Stock Quote:", quote)

    # Get current price for TSLA
    latest_price = ib_handler.get_latest_price(ticker)
    print(f"Latest Price of {ticker}: ${latest_price:.2f}")

    # Get open price for TSLA
    # open_price = ib_handler.get_open_price(ticker)
    # print(f"Open Price of {ticker}: ${open_price:.2f}")

    # # Get option expiry dates for TSLA
    # option_dates = ib_handler.get_option_dates(ticker)
    # print('Option Expiry Dates:', option_dates)

    # # Get option chain for TSLA for a specific expiry
    # if option_dates:
    #     option_chain = ib_handler.get_option_chain(ticker, option_dates[0])
    #     print('Option Chain Data:')
    #     print(option_chain.head())

    # Get historical daily bar data for TSLA
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    historical_daily_bar = ib_handler.get_historical_daily_bar(ticker, start_date, end_date)
    print("Historical Daily Bar:")
    print(historical_daily_bar.head())

    # Disconnect
    ib_handler.disconnect()
    print("Disconnected from Interactive Brokers")

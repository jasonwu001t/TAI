from TAI.utils import ConfigLoader
import pandas as pd
import robin_stocks.robinhood.stocks as rs
import robin_stocks.robinhood.options as ro
import robin_stocks.robinhood.markets as rm
import pyotp
import robin_stocks.robinhood.authentication as ra


class RobinhoodAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.rob_username = config_loader.get_config('Robinhood', 'username')
        self.rob_password = config_loader.get_config('Robinhood', 'password')
        self.rob_pyotp = config_loader.get_config('Robinhood', 'pyopt')

    def authenticate(self):
        totp = pyotp.TOTP(self.rob_pyotp).now()
        login = ra.login(
            username=self.rob_username,
            password=self.rob_password,
            store_session=True,
            mfa_code=totp
        )
        return login


class Robinhood:
    def __init__(self):
        robinhood_auth = RobinhoodAuth()
        self.login = robinhood_auth.authenticate()

    def stock_info(self, ticker):
        return rs.find_instrument_data(ticker)

    def get_fundamental(self, ticker):
        return rs.get_fundamentals(ticker, info=None)

    def get_latest_price(self, ticker):
        return float(rs.get_latest_price(ticker)[0])

    def get_open_price(self, ticker):
        return float(rs.get_fundamentals(ticker, info='open')[0])

    def get_news(self, ticker):
        return rs.get_news(ticker)

    def get_quotes(self, ticker):
        return rs.get_quotes(ticker)[0]

    def get_option_dates(self, ticker):
        return ro.get_chains(ticker)['expiration_dates']

    def get_robinhood_top100_symbols(self):
        r = rm.get_top_100(info='symbol')
        return [i.lower() for i in r]

    def get_earnings(self, ticker):
        earnings_json = rs.get_earnings(ticker, info=None)
        if len(earnings_json) == 0:
            raise Exception("No Data Available")
        else:
            final = []
            for i in earnings_json:
                r = {
                    'year': i['year'],
                    'quarter': i['quarter'],
                    'date': i['report']['date'],
                    'actualEPS': i['eps']['actual'],
                    'estimatedEPS': i['eps']['estimate']
                }
                final.append(r)

            df = pd.DataFrame(final).dropna()
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['actualEPS'] = df['actualEPS'].astype(float)
            df['estimatedEPS'] = df['estimatedEPS'].astype(float)
            df['annualEPS'] = df['actualEPS'].rolling(4).sum()
            return df

    def get_eps(self, ticker):
        latest = self.get_earnings(ticker).tail(4)['actualEPS'].to_list()
        result = sum(latest)
        return result

    def get_daily_close(self, ticker, interval='day', span='5year', bounds='regular', info=None):
        daily = rs.get_stock_historicals(
            ticker, interval=interval, span=span, bounds=bounds, info=info)
        df = pd.DataFrame(daily)[['begins_at', 'close_price']]
        df = df.rename(columns={'begins_at': 'date', 'close_price': 'close'})
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['close'] = df['close'].astype(float)
        return df

    def get_pe_history(self, ticker, avg_window=90):
        eps = self.get_earnings(ticker).drop(['year', 'quarter'], axis=1)
        daily_close = self.get_daily_close(ticker)
        daily_close['close_avg'] = daily_close['close'].rolling(
            avg_window).mean()
        latest_daily = daily_close.tail(1)
        result = pd.merge(left=eps, right=daily_close, how='left', on='date')
        add_latest = pd.concat([result, latest_daily],
                               ignore_index=True).fillna(method='ffill')
        add_latest['pe'] = add_latest['close'] / add_latest['annualEPS']
        add_latest['pe_close_avg'] = add_latest['close_avg'] / \
            add_latest['annualEPS']
        r = add_latest.dropna().copy()
        r['pe_pct_change'] = r['pe'].pct_change()
        return r

    def get_pe_rolling(self, ticker):
        eps = self.get_earnings(ticker)
        daily_close = self.get_daily_close(ticker)
        result = pd.merge(left=daily_close, right=eps,
                          how='left', on='date').fillna(method='ffill')
        result['pe_ratio'] = result['close'] / result['annualEPS']
        return result.dropna()


if __name__ == '__main__':
    robinhood = Robinhood()

    ticker = 'AAPL'
    print(f'Stock info for {ticker}:', robinhood.stock_info(ticker))
    print(f'Fundamental data for {ticker}:', robinhood.get_fundamental(ticker))
    print(f'Latest price for {ticker}:', robinhood.get_latest_price(ticker))
    print(f'Opening price for {ticker}:', robinhood.get_open_price(ticker))
    print(f'News for {ticker}:', robinhood.get_news(ticker))
    print(f'Quotes for {ticker}:', robinhood.get_quotes(ticker))
    print(f'Option expiration dates for {ticker}:',
          robinhood.get_option_dates(ticker))
    print('Top 100 symbols on Robinhood:',
          robinhood.get_robinhood_top100_symbols())
    print(f'Earnings data for {ticker}:', robinhood.get_earnings(ticker))
    print(f'Earnings per share for {ticker}:', robinhood.get_eps(ticker))
    print(f'Daily closing prices for {ticker}:',
          robinhood.get_daily_close(ticker))
    print(f'P/E history for {ticker}:', robinhood.get_pe_history(ticker))
    print(f'Rolling P/E for {ticker}:', robinhood.get_pe_rolling(ticker))

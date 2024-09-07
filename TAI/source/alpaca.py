# alpaca.py ; https://alpaca.markets/learn/how-to-trade-options-with-alpaca/
from TAI.utils import ConfigLoader
import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from alpaca.data.historical import (StockHistoricalDataClient, 
                                    OptionHistoricalDataClient)
from alpaca.data.requests import (StockBarsRequest, 
                                  OptionChainRequest,
                                  OptionSnapshotRequest,
                                  StockLatestTradeRequest, 
                                  StockLatestQuoteRequest,
                                  OptionBarsRequest, 
                                  OptionTradesRequest,
                                  OptionLatestQuoteRequest,
                                  OptionLatestTradeRequest)
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (MarketOrderRequest, GetOrdersRequest,
                                     GetAssetsRequest, GetOptionContractsRequest)
from alpaca.trading.enums import OrderSide, TimeInForce, AssetStatus, ContractType

class AlpacaAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('Alpaca', 'api_key')
        self.api_secret = config_loader.get_config('Alpaca', 'api_secret')
        self.base_url = config_loader.get_config('Alpaca', 'base_url')

    def get_trade_client(self):
        return TradingClient(self.api_key, self.api_secret, paper=True)

    def get_stock_md_client(self):
        return StockHistoricalDataClient(self.api_key, self.api_secret)

    def get_option_md_client(self):
        return OptionHistoricalDataClient(self.api_key, self.api_secret)

class Alpaca:
    def __init__(self):
        auth = AlpacaAuth()
        self.now = datetime.now(tz=ZoneInfo("America/New_York"))
        self.trade_client = auth.get_trade_client()
        self.stock_md_client = auth.get_stock_md_client()
        self.option_md_client = auth.get_option_md_client()
    
    def get_option_chain(self,underlying_symbol,  #eg. 'MSFT'
                            strike_price_gte,
                            strike_price_lte,
                            expiration_date_gte,
                            expiration_date_lte,
                            type = None, # call, or put
                            expiration_date = None,
                            feed = None,
                            root_symbol =None,
                            raw = False,
                            join_contracts = True,
                            chain_table=True  # Convert to Tranditional Train table, although we will lose some columns
                            ):  
        """
        Attributes:
            underlying_symbol (str):                        The underlying_symbol for option contracts.
            feed (Optional[OptionsFeed]):                   The source feed of the data. `opra` or `indicative`. Default: `opra` if the user has the options subscription, `indicative` otherwise.
            type (Optional[ContractType]):                  Filter contracts by the type (call or put).
            strike_price_gte (Optional[float]):             Filter contracts with strike price greater than or equal to the specified value.
            strike_price_lte (Optional[float]):             Filter contracts with strike price less than or equal to the specified value.
            expiration_date (Optional[Union[date, str]]):   Filter contracts by the exact expiration date (format: YYYY-MM-DD).
            expiration_date_gte (Optional[Union[date, str]]): Filter contracts with expiration date greater than or equal to the specified date.
            expiration_date_lte (Optional[Union[date, str]]): Filter contracts with expiration date less than or equal to the specified date.
            root_symbol (Optional[str]):                    Filter contracts by the root symbol.

        eg input:
                underlying_symbol='SPY',
                feed=None,
                type=None,
                strike_price_gte=None, #550.0,
                strike_price_lte=None, #560.0,
                expiration_date='2024-12-20',
                expiration_date_gte=None,
                expiration_date_lte=None,
                root_symbol=None,
                raw=False,
                chain_table=True)
        """
        req = OptionChainRequest(underlying_symbol=underlying_symbol,
                                    feed=feed,
                                    type=type,
                                    strike_price_gte=strike_price_gte,
                                    strike_price_lte=strike_price_lte,
                                    expiration_date=expiration_date,
                                    expiration_date_gte=expiration_date_gte,
                                    expiration_date_lte=expiration_date_lte,
                                    root_symbol=root_symbol,)
        res = self.option_md_client.get_option_chain(req)

        if len(res) == 0:
            raise KeyError('Check if the expiration_date is available or other input error')

        if raw == True:
            return res
        else: # Convert to dataframe
            symbols = []
            greeks_data = []
            implied_volatilities = []
            latest_quotes = []
            latest_trades = []

            # Loop through the dictionary and extract relevant fields
            for symbol, details in res.items():
                symbols.append(symbol)
                
                # Extract greeks or handle None by using getattr() if it's an object
                greeks_obj = getattr(details, 'greeks', None)
                if greeks_obj is None:
                    greeks = {'delta': None, 'gamma': None, 'rho': None, 'theta': None, 'vega': None}
                else:
                    greeks = {
                        'delta': getattr(greeks_obj, 'delta', None),
                        'gamma': getattr(greeks_obj, 'gamma', None),
                        'rho': getattr(greeks_obj, 'rho', None),
                        'theta': getattr(greeks_obj, 'theta', None),
                        'vega': getattr(greeks_obj, 'vega', None),
                    }
                greeks_data.append(greeks)
                
                # Extract implied volatility
                implied_volatilities.append(getattr(details, 'implied_volatility', None))
                
                # Extract latest quote details
                latest_quote = getattr(details, 'latest_quote', None)
                latest_quotes.append({
                    'ask_price': getattr(latest_quote, 'ask_price', None),
                    'ask_size': getattr(latest_quote, 'ask_size', None),
                    'bid_price': getattr(latest_quote, 'bid_price', None),
                    'bid_size': getattr(latest_quote, 'bid_size', None),
                    'ask_exchange': getattr(latest_quote, 'ask_exchange', None),
                    'bid_exchange': getattr(latest_quote, 'bid_exchange', None),
                    'timestamp': getattr(latest_quote, 'timestamp', None)
                })
                # Extract latest trade details or handle None
                latest_trade = getattr(details, 'latest_trade', None)
                if latest_trade is None:
                    latest_trades.append({'price': None, 'size': None, 'exchange': None})#, 'timestamp': None})
                else:
                    latest_trades.append({
                        'price': getattr(latest_trade, 'price', None),
                        'size': getattr(latest_trade, 'size', None),
                        'exchange': getattr(latest_trade, 'exchange', None),
                        # 'timestamp': getattr(latest_trade, 'timestamp', None)
                    })
            # Create DataFrames for each part of the data
            greeks_df = pd.DataFrame(greeks_data)
            implied_vol_df = pd.DataFrame(implied_volatilities, columns=['implied_volatility'])
            quotes_df = pd.DataFrame(latest_quotes)
            trades_df = pd.DataFrame(latest_trades)
            # Combine all the extracted DataFrames into one
            combined_df = pd.concat([pd.Series(symbols, name='symbol'), greeks_df, implied_vol_df, quotes_df, trades_df], axis=1)

            if join_contracts == True:
                contracts = self.get_option_contracts(
                                        underlying_symbols = [underlying_symbol],
                                        expiration_date = expiration_date, #'2024-09-20'
                                        expiration_date_gte = expiration_date_gte,
                                        expiration_date_lte = expiration_date_lte,
                                        root_symbol = root_symbol,
                                        type = type, #'call' or 'put'
                                        style = None, #'american', #'American' or 'Europe'?
                                        strike_price_gte = str(strike_price_gte) if strike_price_gte is not None else None, #'550.0', #'550.0', # '550.0'
                                        strike_price_lte = str(strike_price_lte) if strike_price_lte is not None else None, # '560.0', #'560.0', # '560.0'
                                        limit = 10000, #default is 100, max = 10,000
                                        page_token = None,
                                        raw=False)
                merged_table = pd.merge(combined_df, contracts, on='symbol', how='inner')
                if chain_table == True:
                    # Assuming 'data' is the base Pandas DataFrame
                    # Split the data into Calls and Puts
                    calls = merged_table[merged_table['type'] == 'call'].set_index('strike_price')
                    puts = merged_table[merged_table['type'] == 'put'].set_index('strike_price')
                    option_chain = pd.concat([calls, puts], axis=1, keys=['Call', 'Put'])# Join the two DataFrames on the strike price to ensure alignment
                    # Create the option chain table structure
                    option_chain_table = pd.DataFrame({
                        'Call Bid x Ask': option_chain['Call'].apply(lambda x: f"{x['bid_price']} x {x['ask_price']}" if pd.notnull(x['bid_price']) else None, axis=1),
                        'Call Volume': option_chain['Call']['ask_size'],
                        'Call Open Interest': option_chain['Call']['open_interest'],
                        'Call Delta': option_chain['Call']['delta'],
                        'Call Gamma': option_chain['Call']['gamma'],
                        'Call Vega': option_chain['Call']['vega'],
                        'Call Theta': option_chain['Call']['theta'],
                        'Strike': option_chain.index,  # Strike prices come from the index
                        'Put Bid x Ask': option_chain['Put'].apply(lambda x: f"{x['bid_price']} x {x['ask_price']}" if pd.notnull(x['bid_price']) else None, axis=1),
                        'Put Volume': option_chain['Put']['ask_size'],
                        'Put Open Interest': option_chain['Put']['open_interest'],
                        'Put Delta': option_chain['Put']['delta'],
                        'Put Gamma': option_chain['Put']['gamma'],
                        'Put Vega': option_chain['Put']['vega'],
                        'Put Theta': option_chain['Put']['theta']
                        })
                    option_chain_table.dropna(how='all', inplace=True)# Drop rows where both Call and Put data are missing
                    option_chain_table.sort_values(by='Strike', inplace=True)# Sort by Strike price
                    option_chain_table.reset_index(drop=True, inplace=True)# Reset the index without adding it to the table, keeping Strike column in the middle
                    return option_chain_table
                else:
                    return merged_table
            else:
                return combined_df

    def get_option_snapshot(self,symbol_or_symbols,feed):
        """
        Attributes:
        symbol_or_symbols (Union[str, List[str]]):      The option identifier or list of option identifiers.
        feed (Optional[OptionsFeed]):                   The source feed of the data. `opra` or `indicative`. Default: `opra` if the user has the options subscription, `indicative` otherwise.
        
        req = OptionSnapshotRequest(symbol_or_symbols=['SPY240906C00497000'])
        self.option_md_client.get_option_snapshot(req)
        """

        req = OptionSnapshotRequest(symbol_or_symbols=symbol_or_symbols, feed=feed)
        res = self.option_md_client.get_option_snapshot(req)
        return res

    def option_contracts_to_df(self, res):
        """Converts the option chain response to a DataFrame"""
        option_contracts = []
        for contract in res.option_contracts:
            contract_dict = {
                'close_price': contract.close_price,
                'close_price_date': contract.close_price_date,
                'expiration_date': contract.expiration_date,
                'id': contract.id,
                'name': contract.name,
                'open_interest': contract.open_interest,
                'open_interest_date': contract.open_interest_date,
                'root_symbol': contract.root_symbol,
                'size': contract.size,
                'status': contract.status.value,
                'strike_price': contract.strike_price,
                'style': contract.style.value,
                'symbol': contract.symbol,
                'tradable': contract.tradable,
                'type': contract.type.value,
                'underlying_asset_id': str(contract.underlying_asset_id),
                'underlying_symbol': contract.underlying_symbol
            }
            option_contracts.append(contract_dict)
        return pd.DataFrame(option_contracts)
    
    def get_option_contracts(self, underlying_symbols, expiration_date=None, 
                         expiration_date_gte=None, expiration_date_lte=None,
                         root_symbol=None, type=None, style=None, 
                         strike_price_gte=None, strike_price_lte=None, 
                         limit=10000, page_token=None, raw=False):
        """the output is alpaca.trading.models.OptionContractsResponse instead of raw json
         Try quote.prce instead of quote['price']. If you need a raw json access, you can do quote._raw['price']

        Attributes:
        underlying_symbols (Optional[List[str]]):           The underlying symbols for the option contracts to be returned. (e.g. ["AAPL", "SPY"])
        status (Optional[AssetStatus]):                     The status of the asset.
        expiration_date (Optional[Union[date, str]]):       The expiration date of the option contract. (YYYY-MM-DD)
        expiration_date_gte (Optional[Union[date, str]]):   The expiration date of the option contract greater than or equal to. (YYYY-MM-DD)
        expiration_date_lte (Optional[Union[date, str]]):   The expiration date of the option contract less than or equal to. (YYYY-MM-DD)
        root_symbol (Optional[str]):                        The option root symbol.
        type (Optional[ContractType]):                      The option contract type.
        style (Optional[ExerciseStyle]):                    The option contract style.
        strike_price_gte (Optional[str]):                   The option contract strike price greater than or equal to.
        strike_price_lte (Optional[str]):                   The option contract strike price less than or equal to.
        limit (Optional[int]):                              The number of contracts to limit per page (default=100, max=10000).
        page_token (Optional[str]):                         Pagination token to continue from. The value to pass here is returned in specific requests when more data is available than the request limit allows.
        """
        req = GetOptionContractsRequest(
            underlying_symbols=underlying_symbols,
            status=AssetStatus.ACTIVE,
            expiration_date=expiration_date,
            expiration_date_gte=expiration_date_gte,
            expiration_date_lte=expiration_date_lte,
            root_symbol=root_symbol,
            type=type,
            style=style,
            strike_price_gte=strike_price_gte,
            strike_price_lte=strike_price_lte,
            limit=limit,
            page_token=page_token,
        )
        res = self.trade_client.get_option_contracts(req)
        return res if raw else self.option_contracts_to_df(res)

    def get_option_historical(self, option_symbol_or_symbols, lookback_period, 
                              end=None, timeframe='Day', limit=10000, 
                              bars_or_trades='bars', raw=False):
        """
        Fetches historical option bars or trades for the given option symbols.

        Attributes:
        option_symbol_or_symbols (Union[str, List[str]]): Option ticker identifiers.
        lookback_period (int): Number of days to look back for data.
        timeframe (str): Timeframe for the data ('Min', 'Hour', 'Day', 'Week', 'Month').
        bars_or_trades (str): Fetch either 'bars' or 'trades'.
        """
        timeframe_mapping = {
            'Min': TimeFrame(1, TimeFrameUnit.Minute),
            'Hour': TimeFrame(1, TimeFrameUnit.Hour),
            'Day': TimeFrame.Day,
            'Week': TimeFrame.Week,
            'Month': TimeFrame.Month,
        }
        alpaca_timeframe = timeframe_mapping.get(timeframe, TimeFrame.Day)

        start = self.now - timedelta(days=lookback_period)

        if bars_or_trades == 'bars':
            req = OptionBarsRequest(
                symbol_or_symbols=option_symbol_or_symbols,
                timeframe=alpaca_timeframe,
                start=start,
                end=end,
                limit=limit,
            )
            return req if raw else self.option_md_client.get_option_bars(req).df

        elif bars_or_trades == 'trades':
            req = OptionTradesRequest(
                symbol_or_symbols=option_symbol_or_symbols,
                start=start,
                end=end,
                limit=limit,
            )
            return req if raw else self.option_md_client.get_option_trades(req).df

    def get_latest_quote(self, symbol_or_symbols, asset='stock'):
        """Fetches the latest quote for the given stock or option symbol(s)."""
        if asset == 'stock':
            req = StockLatestQuoteRequest(symbol_or_symbols=symbol_or_symbols)
            return self.stock_md_client.get_stock_latest_quote(req)
        elif asset == 'option':
            req = OptionLatestQuoteRequest(symbol_or_symbols=symbol_or_symbols)
            return self.option_md_client.get_option_latest_quote(req)

    def get_latest_trade(self, symbol_or_symbols, asset='stock'):
        """Fetches the latest trade for the given stock or option symbol(s)."""
        if asset == 'stock':
            req = StockLatestTradeRequest(symbol_or_symbols=symbol_or_symbols)
            return self.stock_md_client.get_stock_latest_trade(req)
        elif asset == 'option':
            req = OptionLatestTradeRequest(symbol_or_symbols=symbol_or_symbols)
            return self.option_md_client.get_option_latest_trade(req)

    # Account Handler
    def get_account(self):
        """Fetches account details including equity value and buying power."""
        return self.trade_client.get_account()

    def get_positions(self):
        """Fetches all positions in the account."""
        return self.trade_client.get_all_positions()

    def get_account_configurations(self):
        """Fetches account configurations."""
        return self.trade_client.get_account_configurations()

    # Trading Handler
    def place_order(self, symbol, qty, side, time_in_force):
        """Places a market order for the specified stock symbol."""
        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide(side),
            time_in_force=TimeInForce(time_in_force)
        )
        return self.trade_client.submit_order(order_data=market_order_data)

    def get_orders(self, status=None, limit=None):
        """Fetches orders based on status and limit."""
        orders_request = GetOrdersRequest(status=status, limit=limit)
        return self.trade_client.get_orders(filter=orders_request)

    # Market Data Handler
    def get_stock_historical(self, symbol_or_symbols, lookback_period, 
                             timeframe='Day', end=None, currency='USD', 
                             limit=None, adjustment='all', feed=None, 
                             asof=None, sort='asc', raw=False, ohlc=False):
        """
        Attributes:
        symbol_or_symbols (Union[str, List[str]]): The ticker identifier or list of ticker identifiers.
        timeframe (TimeFrame): The period over which the bars should be aggregated. (i.e. 5 Min bars, 1 Day bars)
        start (Optional[datetime]): The beginning of the time interval for desired data. Timezone naive inputs assumed to be in UTC.
        end (Optional[datetime]): The end of the time interval for desired data. Defaults to now. Timezone naive inputs assumed to be in UTC.
        limit (Optional[int]): Upper limit of number of data points to return. Defaults to None.
        adjustment (Optional[Adjustment]): The type of corporate action data normalization.(RAW = "raw",SPLIT = "split",DIVIDEND = "dividend",ALL = "all")
                    RAW (str): Unadjusted data
                    SPLIT (str): Stock-split adjusted data
                    DIVIDEND (str): Dividend adjusted data
                    ALL (str): Data adjusted for all corporate actions
        feed (Optional[DataFeed]): The stock data feed to retrieve from.
        sort (Optional[Sort]): The chronological order of response based on the timestamp. Defaults to ASC.
        asof (Optional[str]): The asof date of the queried stock symbol(s) in YYYY-MM-DD format.
        currency (Optional[SupportedCurrencies]): The currency of all prices in ISO 4217 format. Default is USD.
        """
        timeframe_mapping = {
            'Min': TimeFrame(1, TimeFrameUnit.Minute),
            'Hour': TimeFrame(1, TimeFrameUnit.Hour),
            'Day': TimeFrame.Day,
            'Week': TimeFrame.Week,
            'Month': TimeFrame.Month,
        }
        alpaca_timeframe = timeframe_mapping.get(timeframe, TimeFrame.Day)

        start = self.now - timedelta(days=lookback_period)

        request_params = StockBarsRequest(
            symbol_or_symbols=symbol_or_symbols,
            timeframe=alpaca_timeframe,
            start=start,
            end=end,
            currency=currency,
            limit=limit,
            adjustment=adjustment,
            feed=feed,
            asof=asof,
            sort=sort,
        )
        res = self.stock_md_client.get_stock_bars(request_params)
        res1 = res if raw else res.df.reset_index()[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']]
        if ohlc == False:
            return res1
        else: #return yfinnace standard output
            trimmed_df = res1[['timestamp','open','high','low','close','volume']].set_index('timestamp')
            return trimmed_df

if __name__ == "__main__":
    alpaca = Alpaca()

    # Example usage
    account = alpaca.get_account()
    print(f"Account: {account}")

    positions = alpaca.get_positions()
    print(f"Positions: {positions}")

    account_configs = alpaca.get_account_configurations()
    print(f"Account Configurations: {account_configs}")

    orders = alpaca.get_orders()
    print(f"Orders: {orders}")
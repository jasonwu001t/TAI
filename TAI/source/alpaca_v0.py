# alpaca.py ; https://alpaca.markets/learn/how-to-trade-options-with-alpaca/
from TAI.utils import ConfigLoader
import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from alpaca.data.historical import (StockHistoricalDataClient, 
                                    OptionHistoricalDataClient)
from alpaca.data.requests import (StockBarsRequest, 
                                StockLatestTradeRequest, 
                                StockLatestQuoteRequest,
                                OptionBarsRequest, 
                                OptionTradesRequest,
                                OptionLatestQuoteRequest,
                                OptionLatestTradeRequest,
                                )
from alpaca.data.timeframe import TimeFrame,TimeFrameUnit

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (MarketOrderRequest, GetOrdersRequest,
                                    GetAssetsRequest,GetOptionContractsRequest
                                    )
from alpaca.trading.enums import OrderSide, TimeInForce, AssetStatus, ContractType


class AlpacaAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('Alpaca', 'api_key')
        self.api_secret = config_loader.get_config('Alpaca', 'api_secret')
        self.base_url = config_loader.get_config('Alpaca', 'base_url')

        self.trade_api_url = None
        self.trade_api_wss = None
        self.data_api_url = None
        self.option_stream_data_wss = None

    def get_trade_client(self):
        return TradingClient(self.api_key, self.api_secret, paper=True)

    def get_stock_md_client(self):
        return StockHistoricalDataClient(self.api_key, self.api_secret)
    
    def get_option_md_client(self):
        return OptionHistoricalDataClient(self.api_key, self.api_secret)

class Alpaca:
    def __init__(self):
        auth = AlpacaAuth()
        self.now = datetime.now(tz = ZoneInfo("America/New_York"))
        self.trade_client = auth.get_trade_client()
        self.stock_md_client = auth.get_stock_md_client()
        self.option_md_client = auth.get_option_md_client()

    def option_chain_to_df(self,res):
        option_contracts = []
        for contract in res.option_contracts:
            contract_dict = {
                'close_price': contract.close_price,
                'close_price_date': contract.close_price_date,#.isoformat(),
                'expiration_date': contract.expiration_date,#.isoformat(),
                'id': contract.id,
                'name': contract.name,
                'open_interest': contract.open_interest,
                'open_interest_date': contract.open_interest_date,#.isoformat(),
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
        df = pd.DataFrame(option_contracts)
        return df
    
    def get_option_chain(self, underlying_symbols, #list only ['SPY']
                        expiration_date = None, #'2024-08-30'
                        expiration_date_gte = None,
                        expiration_date_lte = None,
                        root_symbol = None,
                        type = None, #'call' or 'put'
                        style = None, #'american' or 'europe'?
                        strike_price_get = None, # '550.0'
                        strike_price_lte = None, # '560.0'
                        limit = 10000, #default is 100, max = 10,000
                        page_token = None,
                        raw=False): 
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
        req1 = GetOptionContractsRequest(
                underlying_symbols = underlying_symbols,               # specify underlying symbols
                status = AssetStatus.ACTIVE,                           # specify asset status: active (default)
                expiration_date = expiration_date,                     # specify expiration date (specified date + 1 day range)
                expiration_date_gte = expiration_date_gte,     #Great or Equal To     # we can pass date object
                expiration_date_lte = expiration_date_lte,     #Less or equatl To     # or string (YYYY-MM-DD)
                root_symbol = root_symbol,               # specify root symbol
                type = type,                              # specify option type (ContractType.CALL or ContractType.PUT)
                style = style,                          # specify option style (ContractStyle.AMERICAN or ContractStyle.EUROPEAN)
                strike_price_gte = strike_price_get,                               # specify strike price range
                strike_price_lte = strike_price_lte,                               # specify strike price range
                limit = limit,                                             # specify limit
                page_token = page_token,                                     # specify page token
                )
        res1 = self.trade_client.get_option_contracts(req1)
        if raw == True:
            return res1
        else:
            res_df1 = self.option_chain_to_df(res1)
            return res_df1
        
        # if res1.next_page_token is not None:
        #     req2 = GetOptionContractsRequest(
        #         underlying_symbols = underlying_symbols,               # specify underlying symbols
        #         status = AssetStatus.ACTIVE,                           # specify asset status: active (default)
        #         expiration_date = expiration_date,                                # specify expiration date (specified date + 1 day range)
        #         expiration_date_gte = expiration_date_gte,                            # we can pass date object
        #         expiration_date_lte = expiration_date_lte,                            # or string (YYYY-MM-DD)
        #         root_symbol = root_symbol,                                    # specify root symbol
        #         type = type,                                           # specify option type (ContractType.CALL or ContractType.PUT)
        #         style = style,                                          # specify option style (ContractStyle.AMERICAN or ContractStyle.EUROPEAN)
        #         strike_price_gte = strike_price_get,                               # specify strike price range
        #         strike_price_lte = strike_price_lte,                               # specify strike price range
        #         limit = limit,                                             # specify limit
        #         page_token = self.res1.next_page_token,                      # specify page token
        #     )
        # res2 = self.trade_client.get_option_contracts(req2)
        # res_df2 = self.option_chain_to_df(res2)
        # # Union res_df1 and res_df2

    def get_option_historical(self, 
                              option_symbol_or_symbols,
                              lookback_period,
                              end=None,
                              timeframe= TimeFrame.Day, #TimeFrame(timeframe), .Min, Hour, Day, Week, Month
                              limit=10000,
                              bars_or_trades='bars',
                              raw=False): ## eg ['SPY240830C00550000','SPY240830P00559000'] , for the same contract at different historical date
        if bars_or_trades == 'bars': # get options historical bars by symbol
            """
            Attributes:
            symbol_or_symbols (Union[str, List[str]]):      The ticker identifier or list of ticker identifiers.
            timeframe (TimeFrame):                          The period over which the bars should be aggregated. (i.e. 5 Min bars, 1 Day bars)
            start (Optional[datetime]):                     The beginning of the time interval for desired data. Timezone naive inputs assumed to be in UTC.
            end (Optional[datetime]):                       The end of the time interval for desired data. Defaults to now. Timezone naive inputs assumed to be in UTC.
            limit (Optional[int]):                          Upper limit of number of data points to return. Defaults to None.
            sort (Optional[Sort]):                          The chronological order of response based on the timestamp. Defaults to ASC.
            """ 
            req = OptionBarsRequest(
                symbol_or_symbols = option_symbol_or_symbols,
                timeframe = TimeFrame.timeframe, # TimeFrame(amount = 1, unit = TimeFrameUnit.Hour),   # specify timeframe
                start = self.now - timedelta(days = lookback_period),    #data starts 5 days ago  # specify start datetime, default=the beginning of the current day.
                end=end,                                                # specify end datetime, default=now
                limit = limit,                                                      # specify limit
            )
            if raw == True:
                return req
            else:
                result = self.option_md_client.get_option_bars(req).df
                return result
            
        if bars_or_trades =='trades': # get options historical trades by symbol
            """
            Attributes:
            symbol_or_symbols (Union[str, List[str]]):      The option identifier or list of option identifiers.
            start (Optional[datetime]):                     The beginning of the time interval for desired data. Timezone naive inputs assumed to be in UTC.
            end (Optional[datetime]):                       The end of the time interval for desired data. Defaults to now. Timezone naive inputs assumed to be in UTC.
            limit (Optional[int]):                          Upper limit of number of data points to return. Defaults to None.
            sort (Optional[Sort]):                          The chronological order of response based on the timestamp. Defaults to ASC.
            """
            req = OptionTradesRequest(
                symbol_or_symbols = option_symbol_or_symbols,
                start = self.now - timedelta(days = lookback_period),                              # specify start datetime, default=the beginning of the current day.
                end=end,                                                     # specify end datetime, default=now
                limit = limit,                                                      # specify limit
                )
            if raw ==True:
                return req
            else:
                result = self.option_md_client.get_option_trades(req).df
                return result
        
    def get_lastest_quote(self, symbol_or_symbols, asset='stock'): #bid,ask, mid, if option, need to use option_symbol_or_symbols
        if asset == 'stock':
            req = StockLatestQuoteRequest(symbol_or_symbols=symbol_or_symbols)
            return self.stock_md_client.get_stock_latest_quote(req)
        if asset == 'option': # get option latest quote by symbol
            req = OptionLatestQuoteRequest(
                symbol_or_symbols = symbol_or_symbols,
            )
            req = OptionLatestQuoteRequest(symbol_or_symbols=symbol_or_symbols) #eg. SPY240805P00540000
            return self.option_md_client.get_option_latest_quote(req)
        
    def get_latest_trade(self, symbol_or_symbols, asset='stock'):
        if asset == 'stock':
            req = StockLatestTradeRequest(symbol_or_symbols=symbol_or_symbols)
            return self.stock_md_client.get_stock_latest_trade(req)
        if asset == 'option': # get option latest trade by symbol
            req = OptionLatestTradeRequest(
                symbol_or_symbols = symbol_or_symbols,
            )
            return self.option_md_client.get_option_latest_trade(req)

    # Account Handler
    def get_account(self): #result in json includes current equity value, buying power, 
        return self.trade_client.get_account()

    def get_positions(self):
        return self.trade_client.get_all_positions()

    def get_account_configurations(self):
        return self.trade_client.get_account_configurations()

    # Trading Handler
    def place_order(self, symbol, qty, side, time_in_force):
        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide(side),
            time_in_force=TimeInForce(time_in_force)
        )
        return self.trade_client.submit_order(order_data=market_order_data)

    def get_orders(self, status=None, limit=None):
        orders_request = GetOrdersRequest(status=status, limit=limit)
        return self.trade_client.get_orders(filter=orders_request)

    # Market Data Handler # TimeFrame(amount = 1, unit = TimeFrameUnit.Hour)
    def get_stock_historical(self, 
                            symbol_or_symbols,
                            lookback_period,
                            timeframe= TimeFrame.Day, #TimeFrame(timeframe), .Min, Hour, Day, Week, Month
                            end = None,
                            currency = 'USD',
                            limit= None,
                            adjustment = 'all',
                            feed = None,
                            asof = None,
                            sort = 'asc',
                            raw = False
                            ):
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
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol_or_symbols,
            timeframe= timeframe, #TimeFrame(timeframe), .Min, Hour, Day, Week, Month
            start = self.now - timedelta(days = lookback_period),
            end = end,
            currency = currency,
            limit=limit,
            adjustment = adjustment,
            feed = feed,
            asof = asof,
            sort = sort, 
        )
        res =  self.stock_md_client.get_stock_bars(request_params)
        if raw == True: # raw is json object
            return res
        else:
            return res.df.reset_index()[['symbol','timestamp','open','high','low','close','volume','trade_count','vwap']]

if __name__ == "__main__":
    alpaca = Alpaca()
    
    # Example usage
    account = alpaca.get_account()
    print(f"Account: {account}")

    alpaca.get_barset('spy',2)

    positions = alpaca.get_positions()
    print(f"Positions: {positions}")

    account_configs = alpaca.get_account_configurations()
    print(f"Account Configurations: {account_configs}")

    # order = alpaca.place_order('AAPL', 1, 'buy', 'gtc')
    # print(f"Order: {order}")

    orders = alpaca.get_orders()
    print(f"Orders: {orders}")
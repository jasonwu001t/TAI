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
                                OptionLatestQuoteRequest,
                                OptionLatestTradeRequest)
from alpaca.data.timeframe import TimeFrame,TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (MarketOrderRequest, GetOrdersRequest,
                                    GetAssetsRequest,GetOptionContractsRequest
                                    )
from alpaca.trading.enums import OrderSide, TimeInForce


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

    def option_chain_to_df(res):
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

    def get_option_chain(self): # read from example input
        """the output is alpaca.trading.models.OptionContractsResponse instead of raw json
         Try quote.prce instead of quote['price']. If you need a raw json access, you can do quote._raw['price']
        """
        pass

    def get_last_quote(self, symbol, asset='stock'): #bid,ask, mid?
        if asset == 'stock':
            req = StockLatestQuoteRequest(symbol=symbol)
            return self.stock_md_client.get_stock_latest_quote(req)
        if asset == 'option': # get option latest quote by symbol
            req = OptionLatestQuoteRequest(
                symbol_or_symbols = [symbol],
            )
            req = OptionLatestQuoteRequest(symbol_or_symbols=[symbol]) #eg. SPY240805P00540000
            return self.option_md_client.get_option_latest_quote(req)
        
    def get_latest_trade(self, symbol, asset='stock'):
        if asset == 'stock':
            req = StockLatestTradeRequest(symbol=symbol)
            return self.stock_md_client.get_stock_latest_trade(req)
        if asset == 'option': # get option latest trade by symbol
            req = OptionLatestTradeRequest(
                symbol_or_symbols = [symbol],
            )
            return self.option_md_client.get_option_latest_trade(req)

    def get_option_historical_bars(self, symbol): #for the same contract at different historical date
        # get options historical bars by symbol
        req = OptionBarsRequest(
            symbol_or_symbols = symbol,
            timeframe = TimeFrame(amount = 1, unit = TimeFrameUnit.Hour),   # specify timeframe
            start = self.now - timedelta(days = 5),                              # specify start datetime, default=the beginning of the current day.
            # end_date=None,                                                # specify end datetime, default=now
            limit = 2,                                                      # specify limit
        )
        result = self.option_md_client.get_option_bars(req).df
        return result

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

    # Market Data Handler
    def get_barset(self, symbols, timeframe, limit):
        request_params = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame(timeframe),
            limit=limit
        )
        return self.stock_md_client.get_stock_bars(request_params)

    def get_last_trade(self, symbol):
        request_params = StockLatestTradeRequest(symbol=symbol)
        return self.stock_md_client.get_stock_latest_trade(request_params)


if __name__ == "__main__":
    alpaca = Alpaca()
    
    # Example usage
    account = alpaca.get_account()
    print(f"Account: {account}")

    positions = alpaca.get_positions()
    print(f"Positions: {positions}")

    account_configs = alpaca.get_account_configurations()
    print(f"Account Configurations: {account_configs}")

    # order = alpaca.place_order('AAPL', 1, 'buy', 'gtc')
    # print(f"Order: {order}")

    orders = alpaca.get_orders()
    print(f"Orders: {orders}")

    # barset = alpaca.get_barset(['AAPL'], 'Day', 5)
    # print(f"Barset: {barset}")

    last_trade = alpaca.get_last_trade('AAPL')
    print(f"Last Trade: {last_trade}")

    last_quote = alpaca.get_last_quote('AAPL')
    print(f"Last Quote: {last_quote}")

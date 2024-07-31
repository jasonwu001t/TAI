# alpaca.py
from TAI.utils import ConfigLoader
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce

class AlpacaAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('Alpaca', 'api_key')
        self.api_secret = config_loader.get_config('Alpaca', 'api_secret')
        self.base_url = config_loader.get_config('Alpaca', 'base_url')

    def get_trading_client(self):
        return TradingClient(self.api_key, self.api_secret, paper=True)

    def get_market_data_client(self):
        return StockHistoricalDataClient(self.api_key, self.api_secret)

class Alpaca:
    def __init__(self):
        auth = AlpacaAuth()
        self.trading_client = auth.get_trading_client()
        self.market_data_client = auth.get_market_data_client()

    # Account Handler
    def get_account(self):
        return self.trading_client.get_account()

    def get_positions(self):
        return self.trading_client.get_all_positions()

    def get_account_configurations(self):
        return self.trading_client.get_account_configurations()

    # Trading Handler
    def place_order(self, symbol, qty, side, time_in_force):
        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide(side),
            time_in_force=TimeInForce(time_in_force)
        )
        return self.trading_client.submit_order(order_data=market_order_data)

    def get_orders(self, status=None, limit=None):
        orders_request = GetOrdersRequest(status=status, limit=limit)
        return self.trading_client.get_orders(filter=orders_request)

    # Market Data Handler
    def get_barset(self, symbols, timeframe, limit):
        request_params = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame(timeframe),
            limit=limit
        )
        return self.market_data_client.get_stock_bars(request_params)

    def get_last_trade(self, symbol):
        request_params = StockLatestTradeRequest(symbol=symbol)
        return self.market_data_client.get_stock_latest_trade(request_params)

    def get_last_quote(self, symbol):
        request_params = StockLatestQuoteRequest(symbol=symbol)
        return self.market_data_client.get_stock_latest_quote(request_params)

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

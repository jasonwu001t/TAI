# atom.py
import pandas as pd
from TAI.broker.alpaca import Alpaca as BrokerAlpaca
from TAI.broker.ib import IB as BrokerIB

class Atom:
    def __init__(self, source='alpaca'):
        if source == 'alpaca':
            self.handler = BrokerAlpaca()
        elif source == 'ib':
            self.handler = BrokerIB()
        else:
            raise ValueError("Unsupported source. Choose either 'alpaca' or 'ib'.")

    def get_market_data(self, symbol, start_date, end_date, timeframe='1D'):
        if isinstance(self.handler, BrokerAlpaca):
            return self.handler.get_barset([symbol], timeframe, (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days)
        elif isinstance(self.handler, BrokerIB):
            return self.handler.get_historical_daily_bar(symbol, start_date, end_date)

if __name__ == "__main__":
    atom_handler = Atom(source='alpaca')
    data = atom_handler.get_market_data('AAPL', '2023-01-01', '2023-01-10')
    print(data)
    
    atom_handler = Atom(source='ib')
    data = atom_handler.get_market_data('AAPL', '2023-01-01', '2023-01-10')
    print(data)

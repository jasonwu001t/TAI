"""
Focus on 
1.. user should only call for market data API from atom
2. market data handlering only from various of brokers and market data providers
3. Standardize Output data structure, input can be vary due to broker outputs
"""

# atom.py
import pandas as pd
from TAI.source.alpaca import Alpaca
from TAI.source.ib import IB as BrokerIB

class Atom:
    def __init__(self, source='alpaca'):
        if source == 'alpaca':
            self.handler = Alpaca()
        elif source == 'ib':
            self.handler = BrokerIB()
        else:
            raise ValueError("Unsupported source. Choose either 'alpaca' or 'ib'.")

    def get_market_data(self, symbol, start_date, end_date, timeframe='1D'):
        if isinstance(self.handler, Alpaca):
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

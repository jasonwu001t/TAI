from TAI.utils import ConfigLoader
import pandas as pd
from fredapi import Fred as FredAPI

class FredAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('Fred', 'api_key')

    def get_api_key(self):
        return self.api_key

class Fred:
    def __init__(self):
        auth = FredAuth()
        self.api_key = auth.get_api_key()
        self.fred = FredAPI(self.api_key)
        self.series_codes = {
            'us_30yr_fix_mortgage_rate': 'MORTGAGE30US',
            'consumer_price_index': 'CPIAUCSL',
            'federal_funds_rate': 'DFF',
            'gdp': 'GDP',
            'core_cpi': 'CORESTICKM159SFRBATL',
            'fed_total_assets': 'walcl',
            'm2': 'WM2NS',
            'unemployment_rate': 'UNRATE',
            'sp500': 'SP500',
            'commercial_banks_deposits': 'DPSACBW027SBOG',
            'total_money_market_fund': 'MMMFFAQ027S',
            'us_producer_price_index': 'PPIFIS'
        }

    def get_series(self, item, ffill=True):
        code = self.series_codes.get(item)
        if ffill:
            return self.fred.get_series(code).ffill()
        else:
            return self.fred.get_series(code)

    def get_info(self, item):
        code = self.series_codes.get(item)
        return self.fred.get_series_info(code)

    def search_item(self, item):
        return self.fred.search(item).T

    def get_latest_release(self, item, ffill=True):
        code = self.series_codes.get(item)
        if ffill:
            return self.fred.get_series_latest_release(code).ffill()
        else:
            return self.fred.get_series_latest_release(code)

    def get_all_releases(self, item):
        code = self.series_codes.get(item)
        return self.fred.get_series_all_releases(code)

if __name__ == "__main__":
    client = Fred()

    m2 = client.get_latest_release('m2')
    gdp = client.get_latest_release('gdp')
    core_cpi = client.get_latest_release('core_cpi')
    fed_assets = client.get_latest_release('fed_total_assets')
    federal_funds_rate = client.get_latest_release('federal_funds_rate')

    print(federal_funds_rate)

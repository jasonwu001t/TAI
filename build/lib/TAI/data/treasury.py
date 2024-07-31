# us_treasury_rate.py
import os
import pandas as pd
import requests
import datetime

class Treasury:
    BASE_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={}"

    def __init__(self):
        pass

    @staticmethod
    def get_script_directory():
        return os.path.dirname(os.path.abspath(__file__))

    def get_df(self, year):
        url = self.BASE_URL.format(year)
        response = requests.get(url)
        content = response.content
        try:
            tables = pd.read_html(content)
            df = tables[0][['Date', '1 Mo', '2 Mo', '3 Mo', '4 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr',
                            '7 Yr', '10 Yr', '20 Yr', '30 Yr']]
            return df
        except ValueError:
            return False

    def get_us_treasury_rates(self, start_year=1990, end_year=None):
        if end_year is None:
            end_year = datetime.datetime.now().year
        all_years = list(range(start_year, end_year + 1))
        all_df = [self.get_df(y) for y in all_years]
        df_concatenated = pd.concat([df for df in all_df if df is not False], ignore_index=True)
        return df_concatenated

    def update_yearly_rates(self, year):
        file_path = os.path.join(self.get_script_directory(), f'{year}_treasury_yield.csv')
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path, index_col=0)
        else:
            existing_df = pd.DataFrame()
        new_df = self.get_df(year)
        if new_df is not False:
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            updated_df.drop_duplicates(keep='last', inplace=True)
            return updated_df
        else:
            print('Failed to update data. Using existing data.')
            return existing_df

# Example usage:
if __name__ == "__main__":
    treasury = Treasury()
    rates = treasury.get_us_treasury_rates(1990, 2023)
    print(rates.head())

    updated_rates = treasury.update_yearly_rates(2023)
    print(updated_rates.head())

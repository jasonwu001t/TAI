import pandas as pd
import requests, os
from datetime import datetime 
from TAI.data import DataMaster

class Treasury:
    BASE_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={}"

    def __init__(self):
        self.dm = DataMaster()
        self.current_dir = self.dm.get_current_dir()
        pass

    def get_treasury(self, year):
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
        
    def get_treasury_historical(self, start_year=1990, end_year=None):
        if end_year is None:
            end_year = datetime.now().year
        all_years = list(range(start_year, end_year + 1))
        all_df = [self.get_treasury(y) for y in all_years]
        df_concatenated = pd.concat([df for df in all_df if df is not False], ignore_index=True)
        return df_concatenated

    def update_yearly_yield(self,year, base_data_file='treasury_yield_all.parquet'):
        # Construct a file path relative to the current script's directory
        data_dir_path = os.path.join(self.current_dir, 'data')
        file_path = os.path.join(data_dir_path, base_data_file)#.format(year))

        if os.path.exists(file_path):
            existing_df = self.dm.load_local(data_folder='data', 
                               file_name = base_data_file,
                               use_polars = False,
                               load_all = False, 
                               selected_files = [base_data_file])
            # existing_df = pd.read_csv(file_path, index_col=0)
        else:
            existing_df = pd.DataFrame()

        new_df = self.get_treasury(year)
        if new_df is not False: 
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
            updated_df.drop_duplicates(keep='last', inplace=True)
            print ('Data Refreshed for {}'.format(datetime.today()))
            return updated_df
        else:
            updated_df = existing_df
            print('{} Failed to update data. Using existing data.'.format(datetime.today()))

    def load_all_yield(self,base_data_file='treasury_yield_all.parquet'):  #Need to update
        historical_rates = self.dm.load_local(data_folder='data', 
                                        file_name = base_data_file,
                                        use_polars = False,
                                        load_all = False, 
                                        selected_files = [base_data_file])
        historical_rates = pd.read_csv(os.path.join(self.current_dir,'treasury_yield_all.csv'))
        new_rates = pd.read_csv(os.path.join(self.current_dir,'treasury_yield_{}.csv'.format(datetime.now().year)))
        rates = pd.concat([historical_rates, new_rates])#.sort_values('Date')
        return rates
    
# Example usage:
if __name__ == "__main__":
    treasury = Treasury()
    rates = treasury.get_treasury_historical(start_year =2022, 
                                           end_year = 2023)
    print(rates.head())

    updated_rates = treasury.update_yearly_rates(2023)
    print(updated_rates.head())

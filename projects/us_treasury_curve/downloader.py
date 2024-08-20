from TAI.source import Treasury
from TAI.data import DataMaster
from datetime import datetime

today = datetime.now()
cur_year = today.year 

tr = Treasury()
dm = DataMaster()

def run_historical_data(): #Only need to run the first time
    historical_rates = tr.get_treasury_historical(start_year =1990, 
                                            end_year = cur_year)
    dm.create_dir()
    dm.save_data(df = historical_rates, 
                file_name='treasury_yield_all',
                type='csv',
                how='in_dir')

def daily_refresh(): #update the latest data to the same csv file
    tr.update_yearly_yield(cur_year)

# run_historical_data()
daily_refresh()

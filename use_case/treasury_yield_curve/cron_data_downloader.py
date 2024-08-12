from TAI.data import DataMaster
from TAI.data import Treasury

dm = DataMaster()
tt = Treasury()

def download_historical_rates(): # Run One time
    historical = tt.get_treasury_historical(1990,2023)
    dm.save_data(historical, how='in_dir', file_name='treasury_yield_1990-2023', dir_name="", type='csv')

def cron_daily_update():
    tt.update_yearly_yield(2024)

# download_historical_rates()
cron_daily_update()
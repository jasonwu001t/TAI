"""IF Error, check if added api_key"""
from TAI.utils import ConfigLoader
import requests
import json
import pandas as pd
from datetime import datetime

# from GTI.data.master import DataSaver

class BLSAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('BLS', 'api_key')

    def get_api_key(self):
        return self.api_key

class BLS:
    def __init__(self):
        auth = BLSAuth()
        self.api_key = auth.get_api_key()
        self.lookback_years = 4
        # self.data_saver = DataSaver()

    def fetch_bls_data(self, series_ids, start_year, end_year):
        headers = {'Content-type': 'application/json'}
        data = json.dumps({
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "registrationkey": self.api_key
        })
        response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        json_data = json.loads(response.text)
        all_data = []
        # print (json_data)
        for series in json_data['Results']['series']:
            series_id = series['seriesID']
            for item in series['data']:
                all_data.append({
                    "series_id": series_id,
                    "year": item['year'],
                    "period": item['period'],
                    "value": float(item['value']),
                })
        df = pd.DataFrame(all_data)
        df['date'] = pd.to_datetime(df['year'] + df['period'].str.replace('M', '-'), format='%Y-%m')
        df.sort_values(by=['series_id', 'date'], inplace=True)
        return df

    def us_job_opening(self):
        return self.fetch_bls_data(series_ids=["JTS000000000000000JOL"], 
                                   start_year=datetime.now().year - self.lookback_years, 
                                   end_year=datetime.now().year)

    def nonfarm_payroll(self):
        return self.fetch_bls_data(series_ids=["CES0000000001"], 
                                   start_year=datetime.now().year - self.lookback_years, 
                                   end_year=datetime.now().year)

    def cps_n_ces(self):
        return self.fetch_bls_data(series_ids=["CES0000000001", "LNS12000000"], 
                                   start_year=datetime.now().year - self.lookback_years, 
                                   end_year=datetime.now().year)

    def us_avg_weekly_hours(self):
        return self.fetch_bls_data(series_ids=["CES0500000002"], 
                                   start_year=datetime.now().year - self.lookback_years, 
                                   end_year=datetime.now().year)

    def unemployment_by_demographic(self):
        return self.fetch_bls_data(series_ids=["LNS14000009", "LNS14000006", "LNS14000003", "LNS14000000"], 
                                   start_year=datetime.now().year - self.lookback_years, 
                                   end_year=datetime.now().year)

    # def save_data(self, dataframes, file_format="parquet"):
    #     self.data_saver.save_data(dataframes, file_format)

if __name__ == "__main__":
    bls = BLS()
    
    # Example usage
    df_us_job_opening = bls.us_job_opening()
    df_nonfarm_payroll = bls.nonfarm_payroll()
    df_cps_n_ces = bls.cps_n_ces()
    df_us_avg_weekly_hours = bls.us_avg_weekly_hours()
    df_unemployment_by_demographic = bls.unemployment_by_demographic()

    dataframes = {
        'us_job_opening': df_us_job_opening,
        'nonfarm_payroll': df_nonfarm_payroll,
        'cps_n_ces': df_cps_n_ces,
        'us_avg_weekly_hours': df_us_avg_weekly_hours,
        'unemployment_by_demographic': df_unemployment_by_demographic
    }

    # bls.save_data(dataframes, file_format="parquet")
    # bls.save_data(dataframes, file_format="csv")

from TAI.source import BLS
from TAI.data import DataMaster

dm = DataMaster()
dm.create_dir()

bls = BLS()

df_unemployment_rate = bls.unemployment_rate()
df_nonfarm_payroll = bls.nonfarm_payroll()
df_us_job_opening = bls.us_job_opening()
df_cps_n_ces = bls.cps_n_ces()
df_us_avg_weekly_hours = bls.us_avg_weekly_hours()
df_unemployment_by_demographic = bls.unemployment_by_demographic()

# print (df_us_job_opening, df_nonfarm_payroll)

df_dict = {
    'unemployment_rate': df_unemployment_rate,
    'us_job_opening': df_us_job_opening,
    'nonfarm_payroll': df_nonfarm_payroll,
    'cps_n_ces': df_cps_n_ces,
    'us_avg_weekly_hours': df_us_avg_weekly_hours,
    'unemployment_by_demographic': df_unemployment_by_demographic
}

for key,value in df_dict.items():
    dm.save_data(value, 
                file_name = key,
                type="csv",
                how='in_dir')
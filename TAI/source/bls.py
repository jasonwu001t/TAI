from TAI.utils import ConfigLoader
import requests
import json
import pandas as pd
from datetime import datetime
import os

class BLSAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('BLS', 'api_key')

    def get_api_key(self):
        return self.api_key

class BLS:
    def __init__(self, lookback_years=19, data_directory="bls_data"):
        """
        Initialize the BLS class.

        Parameters:
        - lookback_years (int): Number of years to look back from the current year.
        - data_directory (str): Directory where data files will be stored.
        """
        auth = BLSAuth()
        self.api_key = auth.get_api_key()
        self.lookback_years = lookback_years  # User-defined lookback years
        self.base_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        self.headers = {'Content-type': 'application/json'}
        self.start_year = datetime.now().year - self.lookback_years
        self.end_year = datetime.now().year
        self.data_directory = data_directory

        # Create the data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)

    def fetch_bls_data(self, series_ids, mom_diff=True, start_year=None, end_year=None):
        """
        Fetch BLS data for given series IDs within the specified date range.

        Parameters:
        - series_ids (list): List of BLS series IDs to fetch.
        - mom_diff (bool): Whether to calculate month-over-month differences.
        - start_year (int): Start year for data fetching.
        - end_year (int): End year for data fetching.

        Returns:
        - pd.DataFrame: DataFrame containing the fetched data.
        """
        if start_year is None:
            start_year = self.start_year
        if end_year is None:
            end_year = self.end_year

        max_years_per_request = 20
        total_years = end_year - start_year + 1
        date_ranges = []
        current_start_year = start_year
        while current_start_year <= end_year:
            current_end_year = min(current_start_year + max_years_per_request - 1, end_year)
            date_ranges.append((current_start_year, current_end_year))
            current_start_year = current_end_year + 1

        all_data = []
        for start, end in date_ranges:
            print(f"Fetching data from {start} to {end}")
            payload = {
                "seriesid": series_ids,
                "startyear": str(start),
                "endyear": str(end),
                "registrationkey": self.api_key
            }
            response = requests.post(self.base_url, json=payload, headers=self.headers)
            if response.status_code != 200:
                raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
            json_data = response.json()

            # Check for API errors
            if json_data.get("status") != "REQUEST_SUCCEEDED":
                error_messages = json_data.get("message", [])
                raise Exception(f"BLS API Error: {error_messages}")

            for series in json_data['Results']['series']:
                series_id = series['seriesID']
                for item in series['data']:
                    # Exclude annual averages or non-monthly periods if necessary
                    if item['period'].startswith('M'):
                        all_data.append({
                            "series_id": series_id,
                            "year": int(item['year']),
                            "period": item['period'],
                            "value": float(item['value']),
                        })

        if not all_data:
            print("No data fetched for the given series and date range.")
            return pd.DataFrame()  # Return empty DataFrame if no data

        df = pd.DataFrame(all_data)
        df['month'] = df['period'].str.replace('M', '').astype(int)
        # Format 'date' as 'YYYY-MM-DD' string
        df['date'] = pd.to_datetime(df[['year', 'month']].assign(DAY=1)).dt.strftime('%Y-%m-%d')
        df.sort_values(by=['series_id', 'date'], inplace=True)
        if mom_diff:
            df['mom_diff'] = df.groupby('series_id')['value'].diff()  # Month-over-month difference per series
        return df

    def fetch_and_save_bls_data(self, series_info, file_format="csv", start_year=None, end_year=None, mode='overwrite'):
        """
        Fetch BLS data for multiple series and save them in the specified format.

        Parameters:
        - series_info (dict): Dictionary containing series keys and their respective series IDs and settings.
        - file_format (str): Format to save the data ('csv', 'parquet', or 'json').
        - start_year (int): Start year for data fetching.
        - end_year (int): End year for data fetching.
        - mode (str): Mode of saving ('overwrite' or 'append'). Relevant for JSON append operations.
        """
        dataframes = {}
        for key, value in series_info.items():
            print(f"Fetching data for series: {key}")
            dataframes[key] = self.fetch_bls_data(
                series_ids=value['series_ids'],
                mom_diff=value.get('mom_diff', True),
                start_year=start_year,
                end_year=end_year
            )

        self.save_data(dataframes, file_format, mode=mode)

    def save_data(self, dataframes, file_format="csv", mode='overwrite'):
        """
        Save the fetched dataframes to files.

        Parameters:
        - dataframes (dict): Dictionary of DataFrames to save.
        - file_format (str): Format to save the data ('csv', 'parquet', or 'json').
        - mode (str): Mode of saving ('overwrite' or 'append'). Relevant for JSON append operations.
        """
        for name, df in dataframes.items():
            if df.empty:
                print(f"No data to save for series: {name}")
                continue

            file_path = os.path.join(self.data_directory, f"{name}.{file_format}")
            if file_format == "parquet":
                df.to_parquet(file_path, index=False)
            elif file_format == "csv":
                if mode == 'append' and os.path.exists(file_path):
                    df.to_csv(file_path, mode='a', header=False, index=False)
                else:
                    df.to_csv(file_path, mode='w', header=True, index=False)
            elif file_format == "json":
                if mode == 'append' and os.path.exists(file_path):
                    # Read existing JSON data
                    try:
                        df_existing = pd.read_json(file_path, orient='records')
                        # Ensure 'date' is treated as string
                        df_existing['date'] = df_existing['date'].astype(str)
                        df_combined = pd.concat([df_existing, df], ignore_index=True)
                        # Drop duplicates based on 'series_id' and 'date'
                        before_dedup = len(df_combined)
                        df_combined.drop_duplicates(subset=['series_id', 'date'], inplace=True)
                        after_dedup = len(df_combined)
                        duplicates_removed = before_dedup - after_dedup
                        if duplicates_removed > 0:
                            print(f"Removed {duplicates_removed} duplicate records for series: {name}.")
                        # Save back to JSON with 'date' in 'YYYY-MM-DD' format
                        df_combined.to_json(file_path, orient='records', indent=4)
                        print(f"Appended and updated JSON data for {name} at {file_path}")
                    except ValueError as e:
                        print(f"Error reading existing JSON file {file_path}: {e}")
                        print("Overwriting with new data.")
                        df.to_json(file_path, orient='records', indent=4)
                else:
                    df.to_json(file_path, orient='records', indent=4)
            else:
                print(f"Unsupported file format: {file_format}")
                continue
            print(f"Data saved to {file_path}")

    def fetch_latest_data(self, series_info, file_format="csv"):
        """
        Fetch the latest year's data for daily refresh.

        Parameters:
        - series_info (dict): Dictionary containing series keys and their respective series IDs and settings.
        - file_format (str): Format to save the data ('csv', 'parquet', or 'json').

        Returns:
        - pd.DataFrame: DataFrame containing the latest data.
        """
        latest_year = datetime.now().year
        print(f"Fetching latest data for the year: {latest_year}")
        self.fetch_and_save_bls_data(series_info, file_format=file_format, start_year=latest_year, end_year=latest_year, mode='append')

    def update_historical_data(self, series_info, file_format="csv"):
        """
        Append the latest data to the historical data files and remove duplicates.

        Parameters:
        - series_info (dict): Dictionary containing series keys and their respective series IDs and settings.
        - file_format (str): Format of the main historical data files ('csv', 'parquet', or 'json').
        """
        print("Updating historical data with the latest data...")
        self.fetch_latest_data(series_info, file_format=file_format)
        print("Historical data update completed.")

    def save_data_combined(self, dataframes, file_format="csv"):
        """
        Save combined dataframes to files without duplication logic.

        Parameters:
        - dataframes (dict): Dictionary of DataFrames to save.
        - file_format (str): Format to save the data ('csv', 'parquet', or 'json').
        """
        # This method can be used for initial historical data fetching
        self.save_data(dataframes, file_format=file_format, mode='overwrite')

if __name__ == "__main__":
    # Define lookback years when instantiating the class
    bls = BLS(lookback_years=25)  # User can set any number of years

    series_info = {
        'nonfarm_payroll': {'series_ids': ["CES0000000001"]},
        'unemployment_rate': {'series_ids': ["LNS14000000"], 'mom_diff': False},
        'us_job_opening': {'series_ids': ["JTS000000000000000JOL"]},
        'cps_n_ces': {'series_ids': ["CES0000000001", "LNS12000000"]},
        'us_avg_weekly_hours': {'series_ids': ["CES0500000002"]},
        'unemployment_by_demographic': {'series_ids': ["LNS14000009", "LNS14000006", "LNS14000003", "LNS14000000"]}
    }

    # Directory where data files will be stored
    data_dir = "bls_data"

    # Fetch and save historical data (initial fetch or complete refresh)
    print("Fetching and saving historical data...")
    bls.fetch_and_save_bls_data(series_info, file_format="json", start_year=bls.start_year, end_year=bls.end_year, mode='overwrite')

    # Daily refresh to fetch only the latest data and append to historical data
    print("\nPerforming daily refresh to update historical data with the latest year...")
    bls.update_historical_data(series_info, file_format="json")

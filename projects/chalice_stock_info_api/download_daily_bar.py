import asyncio
import pandas as pd
import time, json
from TAI.source import alpaca
from TAI.data import DataMaster
from datetime import datetime, timedelta
import os

# Set global variables
LOOKBACK_YEARS = 10  # For initial data fetch
PRINT_LOG = False    # Toggle for logging
MAX_CONCURRENT_REQUESTS = 10  # Adjust based on system and API limits
PARQUET_FILE = 'sp500_daily_ohlc.parquet'  # Parquet file name
JSON_FILE =  'sp500_daily_ohlc.json'

dm = DataMaster()

def get_lookback_period_in_days():
    """Calculate the number of days to look back from January 1st of the historical year."""
    today = datetime.now()
    start_year = today.year - LOOKBACK_YEARS
    start_date = datetime(year=start_year, month=1, day=1)
    lookback_period = (today - start_date).days
    return lookback_period, start_date


LOOKBACK_PERIOD_DAYS, START_DATE = get_lookback_period_in_days()


def get_sp500_symbols():
    """Fetches the list of S&P 500 company symbols from Wikipedia."""
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    sp500_table = pd.read_html(sp500_url)
    sp500_df = sp500_table[0]
    symbols = sp500_df['Symbol'].tolist()
    symbols = [symbol.replace('.', '-') for symbol in symbols]
    return symbols

# Fetch S&P 500 symbols
sp500_symbols = get_sp500_symbols()

# Initialize Alpaca class instance
alpaca_handler = alpaca.Alpaca()


async def fetch_stock_data(semaphore, symbol, start_date):
    """Asynchronously fetch historical stock data for a single symbol starting from start_date."""
    async with semaphore:
        if PRINT_LOG:
            print(
                f"Fetching data for {symbol} starting from {start_date.date()}...")
        try:
            # Fetch historical data from start_date to today
            lookback_period = (datetime.now() - start_date).days
            if lookback_period <= 0:
                # No new data to fetch
                return symbol, pd.DataFrame()
            data = alpaca_handler.get_stock_historical(
                symbol_or_symbols=symbol,
                lookback_period=lookback_period,
                timeframe='Day',
                end=datetime.now(),
                ohlc=True
            )
            if data.empty:
                raise ValueError(f"No data returned for {symbol}")
            return symbol, data
        except Exception as e:
            if PRINT_LOG:
                print(f"Error fetching data for {symbol}: {e}")
            return symbol, None


async def fetch_latest_data(symbols, existing_data):
    """Fetch latest data for symbols asynchronously starting from the last date in existing_data."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []

    for symbol in symbols:
        # Determine the start date for each symbol
        if not existing_data.empty and 'symbol' in existing_data.columns:
            if symbol in existing_data['symbol'].unique():
                last_date = existing_data[existing_data['symbol']
                                          == symbol]['timestamp'].max()
                start_date = pd.to_datetime(last_date) + timedelta(days=1)
            else:
                # If the symbol is not in existing data, fetch data from the default start date
                start_date = START_DATE
        else:
            # existing_data is empty or does not have 'symbol' column
            start_date = START_DATE

        # Skip if start_date is after today
        if start_date.date() > datetime.now().date():
            if PRINT_LOG:
                print(f"No new data for {symbol}.")
            continue

        tasks.append(fetch_stock_data(semaphore, symbol, start_date))

    results = await asyncio.gather(*tasks)
    successful_results = []
    failed_symbols = []

    for symbol, data in results:
        if data is not None and not data.empty:
            successful_results.append(data)
        elif data is None:
            failed_symbols.append(symbol)
        # No need to add symbols with empty data (no new data)

    return successful_results, failed_symbols

def update_parquet_with_latest_data():
    """Update the Parquet file with the latest data for all tickers."""
    # Record the start time
    start_time = time.time()

    # Check if Parquet file exists
    if os.path.exists(PARQUET_FILE):
        # Read existing data
        # existing_data = pd.read_parquet(PARQUET_FILE)
        existing_data = dm.load_local(data_folder='data', 
                                    file_name =PARQUET_FILE,
                                    use_polars = False,
                                    load_all = False, 
                                    selected_files = [PARQUET_FILE])
    else:
        # If file doesn't exist, create an empty DataFrame
        existing_data = pd.DataFrame()

    # Use the full list of S&P 500 symbols
    symbols = sp500_symbols

    # Run async fetch for latest data
    loop = asyncio.get_event_loop()
    successful_results, failed_symbols = loop.run_until_complete(
        fetch_latest_data(symbols, existing_data))

    if successful_results:
        # Combine new data
        new_data = pd.concat(successful_results, ignore_index=True)
        # Combine with existing data
        combined_df = pd.concat([existing_data, new_data], ignore_index=True)
        # Remove duplicates
        combined_df.drop_duplicates(
            subset=['symbol', 'timestamp'], inplace=True)
        # Sort the data
        combined_df.sort_values(by=['symbol', 'timestamp'], inplace=True)
        # Store the data in Parquet
        store_data_in_parquet(combined_df, PARQUET_FILE)
    else:
        print("No new data was fetched.")

    # Print failed symbols if there were any errors
    if failed_symbols:
        print(
            f"Failed to fetch data for the following symbols: {failed_symbols}")

    # Record the end time
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Update completed in {total_time:.2f} seconds")


def store_data_in_parquet(df, file_name):
    """Store the DataFrame in Parquet format."""
    if df.empty:
        print("No data to store.")
        return

    try:
        dm.create_dir()
        dm.save_local(df, 'data', PARQUET_FILE,
                    delete_local=False)
        # df.to_parquet(file_name, index=False)
        if PRINT_LOG:
            print(f"Data successfully stored in {file_name}")
    except Exception as e:
        print(f"Error storing data to Parquet: {e}")

def convert_to_json(data):
    def convert_timestamp(item):
        if isinstance(item, dict):
            # Convert 'timestamp' key to 'date' and format the value to 'YYYY-MM-DD'
            if 'timestamp' in item and isinstance(item['timestamp'], pd.Timestamp):
                item['date'] = item['timestamp'].date().isoformat()  # Only the date part
                del item['timestamp']  # Remove the 'timestamp' key
        return item

    # Apply the conversion to each dictionary in the list
    cleaned_data = [convert_timestamp(entry) for entry in data]
    
    # Convert to JSON format (as Python object, not a string)
    return cleaned_data

def main():
    print('PROCESS STARTS AT : {}'.format(datetime.now()))
    # update_parquet_with_latest_data() # Update the Parquet file with the latest data, will save or update local parquet file

    updated_df = dm.load_local(data_folder='data', 
                    file_name = PARQUET_FILE,
                    use_polars = False,
                    load_all = False, 
                    selected_files = [PARQUET_FILE])
    
    ###################################
    # API Can timeout if the output json data is too large, therefore, breakdown to chunches
    # df_dict = updated_df.to_dict(orient='records')
    # df_json = convert_to_json(df_dict)

    # dm.save_local(df_json, 'data', JSON_FILE,
    #             delete_local=False)
    
    # dm.save_s3(df_json,
    #             'jtrade1-dir', 
    #             'api',
    #             JSON_FILE, 
    #             use_polars=False, 
    #             delete_local=True)
    ###################################

    grouped = updated_df.groupby('symbol') # Group the data by symbol

    for symbol, group in grouped:
        df_dict = group.to_dict(orient='records')
        df_json = convert_to_json(df_dict)

        json_file_name = f'{symbol}.json'
        dm.save_local(df_json, 'data/stock_daily_bar', json_file_name, delete_local=False)

        dm.save_s3(
            df_json,
            'jtrade1-dir',
            'api/stock_daily_bar',
            json_file_name,
            use_polars=False,
            delete_local=True
        )
    print('PROCESS ENDS AT : {}'.format(datetime.now()))

if __name__ == "__main__":
    main()

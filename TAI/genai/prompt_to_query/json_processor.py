import os
import json
import polars as pl

class JsonProcessor:
    def __init__(self, data_catalog_path, data_folder):
        self.data_catalog_path = data_catalog_path
        self.data_folder = data_folder

    def load_sample_data(self):
        dataframes = {}
        with open(self.data_catalog_path, 'r') as f:
            catalog = json.load(f)
        
        for table_name in catalog.keys():
            csv_file = os.path.join(self.data_folder, f'{table_name}.csv')
            parquet_file = os.path.join(self.data_folder, f'{table_name}.parquet')
            if os.path.exists(csv_file):
                dataframes[table_name] = pl.read_csv(csv_file)
            elif os.path.exists(parquet_file):
                dataframes[table_name] = pl.read_parquet(parquet_file)
            else:
                print(f"No data file found for table '{table_name}' in folder '{self.data_folder}'.")
        return dataframes

    def process_data_catalog(self):
        with open(self.data_catalog_path, 'r') as f:
            catalog = json.load(f)
        
        dataframes = self.load_sample_data()

        for table_name, df in dataframes.items():
            if table_name in catalog:
                catalog[table_name]['columns'] = list(df.columns)
            else:
                print(f"Table '{table_name}' not found in data catalog.")
        
        processed_catalog_path = self.data_catalog_path.replace('.json', '_processed.json')
        with open(processed_catalog_path, 'w') as f:
            json.dump(catalog, f, indent=4)

        print(f"Processed data catalog saved to {processed_catalog_path}")

# Usage example
if __name__ == "__main__":
    processor = JsonProcessor('data_catalog.json', 'data')
    processor.process_data_catalog()

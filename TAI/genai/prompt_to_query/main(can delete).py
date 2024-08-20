import os
import polars as pl
from data_catalog import CentralDataCatalog
from materialized_view_manager import MaterializedViewManager
from query_executor import QueryExecutor
from sql_generator import SQLGenerator
from logging_config import init_logger
from TAI.genai.genai_v0_pending_delete import AWSBedrock  # Import your AWSBedrock class here

def load_sample_data(catalog, data_folder='data'):
    """
    Load sample data into Polars DataFrame based on the catalog information.

    Args:
    catalog (CentralDataCatalog): The data catalog containing table information.
    data_folder (str): The folder where the data files are stored.

    Returns:
    dict: A dictionary of DataFrames where keys are table names.
    """
    dataframes = {}
    for table_name in catalog.list_tables():
        csv_file = os.path.join(data_folder, f'{table_name}.csv')
        parquet_file = os.path.join(data_folder, f'{table_name}.parquet')

        if os.path.exists(csv_file):
            dataframes[table_name] = pl.read_csv(csv_file)
        elif os.path.exists(parquet_file):
            dataframes[table_name] = pl.read_parquet(parquet_file)
        else:
            raise FileNotFoundError(f"No data file found for table '{table_name}' in folder '{data_folder}'.")
    # print (dataframes)
    return dataframes

if __name__ == "__main__":
    # Initialize logging
    init_logger()

    # Initialize CentralDataCatalog and load from JSON
    data_catalog = CentralDataCatalog()
    data_catalog.load_from_json('data_catalog.json')

    # Load sample data from the 'data' folder
    dataframes = load_sample_data(data_catalog, data_folder='data')

    # Initialize the AWSBedrock instance
    aws_bedrock = AWSBedrock()

    # Initialize SQLGenerator with dataframes and data catalog
    sql_generator = SQLGenerator(aws_bedrock, dataframes, data_catalog)

    # Initialize MaterializedViewManager
    materialized_view_manager = MaterializedViewManager()

    # Initialize QueryExecutor
    query_executor = QueryExecutor(dataframes, sql_generator)

    # Example user prompt
    user_prompt = "who are our customer names and their"

    # Validate and execute the query or generate a direct response
    sql_query, result = query_executor.validate_and_execute(user_prompt)

    # Generate a natural language response based on the query result, schema, and descriptions
    text_response = query_executor.result_to_text(result, user_prompt, sql_query)
    print(text_response)

# from text_to_sql_agent import TextToSQLAgent

# agent = TextToSQLAgent()
# user_prompt = "join the orders to the customer table then tell me how much sells"
# result = agent.process_prompt(user_prompt)
# print(result)
import polars as pl
import os
import json
import logging
import uuid
from TAI.genai import AWSBedrock

def load_sample_data(data_folder):
    dataframes = {}
    for table_name in list_tables():
        csv_file = os.path.join(data_folder, f'{table_name}.csv')
        parquet_file = os.path.join(data_folder, f'{table_name}.parquet')
        if os.path.exists(csv_file):
            dataframes[table_name] = pl.read_csv(csv_file)
        elif os.path.exists(parquet_file):
            dataframes[table_name] = pl.read_parquet(parquet_file)
        else:
            raise FileNotFoundError(f"No data file found for table '{table_name}' in folder '{data_folder}'.")
    return dataframes

with open("data_catalog.json", 'r') as f:
    data = json.load(f)
    print (data)
    # print ("-------------------------------")
    # for table_name, info in data.items():
            # print (table_name)
            # print (info.get('description', ''))
            # print (info.get('embedding', None))

table_names = [k for k,v in data.items()]


for table_name, df in load_sample_data('data').items():
    table_name
    table_info = data.items().keys
    columns = ", ".join(df.columns)

catalog = {}
schema_embeddings = {}
table_embeddings = {}

def load_from_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
        for table_name, info in data.items():
            add_table(
                table_name = table_name,
                description = info.get('description', ''),
                columns = info.get('columns', None),
                embedding = info.get('embedding', None)
            )

def add_table(table_name, description, columns, embedding=None):
    catalog[table_name] = {
        'description': description,
        'columns' : columns,
        'embedding': embedding
    }

def get_catalog_info(table_name):
    return catalog.get(table_name, None)

def list_tables():
    return list(catalog.keys())


def generate_schema_description():
    descriptions = []
    for table_name, df in load_sample_data('data').items():
        table_info = get_catalog_info(table_name)
        table_description = table_info['description'] if table_info else "No description available."
        columns = ", ".join(df.columns)
        descriptions.append(f"Table {table_name} ({columns}): {table_description}")
    return "; ".join(descriptions)

# print (generate_schema_description())    

def generate_embeddings(aws_bedrock):
    # Generate schema embeddings
    for keyword in ["schema", "tables", "description", "records", "loaded tables", "table description"]:
        schema_embeddings[keyword] = aws_bedrock.generate_embedding(keyword)
    
    # Generate table embeddings
    for table_name in list_tables():
        table_info = get_catalog_info(table_name) # returned json description and embedding items
        if table_info: #if table_info exist, 
            table_text = f"{table_name}: {table_info['description']}"
            print ("TABLE TEXTTTTTTTT: ", table_text)
            table_embeddings[table_name] = aws_bedrock.generate_embedding(table_text) #converted json table and description to Array
            for col in table_info.get('columns', []):  #THE COLUMNS ARE NOT DEFINED IN JSON FILE YET, Need to read from file directly
                col_text = f"{table_name}.{col}"
                print ("COLLLLLL TEXT : ", col_text)
                table_embeddings[col_text] = aws_bedrock.generate_embedding(col_text)

aws_bedrock = AWSBedrock()
# print ('USERE TABLE, ', get_catalog_info('users'))
# print ('aaaaaaaaaa', get_catalog_info('users').get('columns', []))
# generate_embeddings(aws_bedrock)
# dataframes = load_sample_data('data')

# print ('---------------------')
# print (table_embeddings)


# print (get_catalog_info('users'))

# print (catalog.get('users', None))
# print (list_tables())

# print (schema_embeddings)
# print ('---------------------')
# print (table_embeddings)

# prompt_embedding = aws_bedrock.generate_embedding("tell me more about the table")
# # print ('PMTOPMT EMBEDDING', prompt_embedding)

# max_similarity = 0
# for keyword, embedding in schema_embeddings.items():
#     similarity = aws_bedrock.calculate_similarity(prompt_embedding, embedding)
#     # print ('KEYWORKKKKKKKKKKK', keyword)
#     # print ('EMBEDDDINNGNNGNGNGNGN', embedding)
#     # print (similarity)
#     if similarity > max_similarity:
#         max_similarity = similarity

# print ('MAXXXX', max_similarity)

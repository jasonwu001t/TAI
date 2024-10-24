import polars as pl
import os
import json
import logging
import uuid
from TAI.genai import AWSBedrock
from .utils import init_logger, get_sql_generation_prompt, get_direct_response_prompt


class CentralDataCatalog:
    def __init__(self):
        self.catalog = {}
        self.embeddings = {}  # Unified embeddings dictionary

    def load_from_json(self, json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)
            for table_name, info in data.items():
                self.add_table(
                    table_name=table_name,
                    description=info.get('description', ''),
                    embedding=info.get('embedding', None),
                    columns=info.get('columns', [])
                )

    def add_table(self, table_name, description, embedding=None, columns=None):
        self.catalog[table_name] = {
            'description': description,
            'embedding': embedding,
            'columns': columns
        }

    def get_catalog_info(self, table_name):
        return self.catalog.get(table_name, None)

    def list_tables(self):
        return list(self.catalog.keys())

    def generate_embeddings(self, aws_bedrock):
        schema_keywords = ["schema", "tables", "description",
                           "records", "loaded tables", "table description"]
        for keyword in schema_keywords:
            self.embeddings[f"schema:{keyword}"] = aws_bedrock.generate_embedding(
                keyword)

        for table_name, table_info in self.catalog.items():
            table_text = f"{table_name}: {table_info['description']}"
            self.embeddings[f"table:{table_name}"] = aws_bedrock.generate_embedding(
                table_text)
            for col in table_info.get('columns', []):
                col_text = f"{table_name}.{col}"
                self.embeddings[f"table:{table_name}.{col}"] = aws_bedrock.generate_embedding(
                    col_text)


class SQLGenerator:
    def __init__(self, aws_bedrock, dataframes, data_catalog, schema_threshold=0.7, table_threshold=0.3):
        self.aws_bedrock = aws_bedrock
        self.dataframes = dataframes
        self.data_catalog = data_catalog
        self.schema_description = self.generate_schema_description()
        self.logger = logging.getLogger('TextToSQL')
        self.logger.info("SQLGenerator initialized successfully.")

        self.schema_threshold = schema_threshold  # Set schema threshold
        self.table_threshold = table_threshold  # Set table threshold

        self.session_id = str(uuid.uuid4())
        self.data_catalog.generate_embeddings(self.aws_bedrock)

    def chatbot(self, prompt):
        response = self.aws_bedrock.generate_text(prompt)
        return response['response']

    def check_prompt_relation(self, user_prompt, context="schema", threshold=0.7):
        prompt_embedding = self.aws_bedrock.generate_embedding(user_prompt)
        max_similarity = max(
            self.aws_bedrock.calculate_similarity(prompt_embedding, embedding)
            for key, embedding in self.data_catalog.embeddings.items()
            if key.startswith(context)
        )
        return max_similarity > threshold, max_similarity

    def generate_schema_description(self):
        descriptions = [
            f"Table {table_name} ({', '.join(df.columns)}): {self.data_catalog.get_catalog_info(table_name)['description']}"
            for table_name, df in self.dataframes.items()
        ]
        return "; ".join(descriptions)

    def is_prompt_related(self, user_prompt, context="schema"):
        threshold = self.schema_threshold if context == "schema" else self.table_threshold
        related, max_similarity = self.check_prompt_relation(
            user_prompt, context=context, threshold=threshold
        )
        self.logger.info(f"Maximum {context} similarity: {max_similarity}")
        return related

    def handle_schema_query(self, user_prompt):
        response = []
        if "all tables" in user_prompt.lower() or "loaded tables" in user_prompt.lower():
            response.append(
                f"The following tables have been loaded: {', '.join(self.data_catalog.list_tables())}.")
        for table_name, df in self.dataframes.items():
            table_info = self.data_catalog.get_catalog_info(table_name)
            record_count = df.shape[0]
            response.append(
                f"Table '{table_name}' has {record_count} records. Description: {table_info['description']}.")
        return "\n".join(response)

    def generate_sql_query(self, user_prompt):
        self.logger.info(f"Received user prompt: '{user_prompt}'")

        if self.is_prompt_related(user_prompt, context="schema"):
            self.logger.info(
                f"The user prompt '{user_prompt}' is related to schema. Handling as schema query.")
            return None

        if not self.is_prompt_related(user_prompt, context="table"):
            self.logger.info(
                f"The user prompt '{user_prompt}' is not related to the tables. Returning direct response.")
            return None

        prompt = get_sql_generation_prompt(
            self.schema_description, user_prompt)
        response = self.chatbot(prompt)
        self.logger.info(f"Generated SQL query for prompt: {user_prompt}")

        return response

    def generate_direct_response(self, user_prompt):
        prompt = get_direct_response_prompt(user_prompt)
        response = self.chatbot(prompt)
        self.logger.info(
            f"Generated direct response for prompt: {user_prompt}")
        return response


class QueryExecutor:
    def __init__(self, dataframes, sql_generator, max_retries=3):
        self.dataframes = dataframes
        self.sql_generator = sql_generator
        self.max_retries = max_retries
        self.logger = logging.getLogger('QueryExecutor')

    def execute_query(self, sql_query):
        context = pl.SQLContext(self.dataframes)
        try:
            result = context.execute(sql_query).collect()
            self.logger.info(f"Executed SQL query: {sql_query}")
            return result
        except Exception as e:
            self.logger.error(f"Query execution error: {e}")
            return None

    def validate_and_execute(self, user_prompt):
        if self.sql_generator.is_prompt_related(user_prompt, context="schema"):
            schema_response = self.sql_generator.handle_schema_query(
                user_prompt)
            return None, schema_response

        sql_query = self.sql_generator.generate_sql_query(user_prompt)
        if sql_query is None:
            direct_response = self.sql_generator.generate_direct_response(
                user_prompt)
            return None, direct_response

        retries = 0
        while retries < self.max_retries:
            self.logger.info(f"Attempting to execute SQL query: {sql_query}")
            result = self.execute_query(sql_query)
            if result is not None:
                return sql_query, result

            feedback = f"The SQL query '{sql_query}' failed to execute. Please regenerate a valid SQL query."
            self.logger.info(f"Providing feedback to model: {feedback}")
            sql_query = self.sql_generator.generate_sql_query(feedback)
            retries += 1

        self.logger.error(
            "Max retries reached. Failed to generate a valid SQL query.")
        return sql_query, None

    def result_to_text(self, result, user_prompt, sql_query=None):
        if isinstance(result, str):
            return result
        if result is None or result.is_empty():
            return f"No results were found based on the query: {sql_query}"
        text_response = f"Based on your request, '{user_prompt}', the results are:\n\n"
        text_response += f"Executed Query: {sql_query}\n\n"
        for row in result.iter_rows(named=True):
            row_description = ", ".join(
                [f"{col}: {val}" for col, val in row.items()])
            text_response += f"{row_description}\n"
        return text_response


class TextToSQLAgent:
    def __init__(self, data_catalog_path='data_catalog_processed.json', data_folder='data', max_retries=3, schema_threshold=0.7, table_threshold=0.3):
        self.logger = init_logger()
        self.logger.info("Initializing TextToSQLAgent...")
        self.data_catalog = CentralDataCatalog()
        self.data_catalog.load_from_json(data_catalog_path)
        self.dataframes = self.load_sample_data(data_folder)
        self.aws_bedrock = AWSBedrock()
        self.sql_generator = SQLGenerator(
            self.aws_bedrock, self.dataframes, self.data_catalog, schema_threshold, table_threshold)
        self.query_executor = QueryExecutor(
            self.dataframes, self.sql_generator, max_retries)
        self.logger.info("TextToSQLAgent initialized successfully.")

    def load_sample_data(self, data_folder):
        dataframes = {}
        for table_name in self.data_catalog.list_tables():
            csv_file = os.path.join(data_folder, f'{table_name}.csv')
            parquet_file = os.path.join(data_folder, f'{table_name}.parquet')
            if os.path.exists(csv_file):
                dataframes[table_name] = pl.read_csv(csv_file)
            elif os.path.exists(parquet_file):
                dataframes[table_name] = pl.read_parquet(parquet_file)
            else:
                raise FileNotFoundError(
                    f"No data file found for table '{table_name}' in folder '{data_folder}'.")
        return dataframes

    def process_prompt(self, user_prompt):
        sql_query, result = self.query_executor.validate_and_execute(
            user_prompt)
        text_response = self.query_executor.result_to_text(
            result, user_prompt, sql_query)
        return text_response


if __name__ == "__main__":
    agent = TextToSQLAgent()
    user_prompt = "who are the tables"
    result = agent.process_prompt(user_prompt)
    print(result)

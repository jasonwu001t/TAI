import logging

class SQLGenerator:
    def __init__(self, aws_bedrock, dataframes, data_catalog):
        self.aws_bedrock = aws_bedrock
        self.dataframes = dataframes
        self.data_catalog = data_catalog
        self.schema_description = self.generate_schema_description()
        self.logger = logging.getLogger('SQLGenerator')

    def generate_schema_description(self):
        """
        Automatically generate the schema description by reading the loaded tables and including their descriptions.

        Returns:
        str: The generated schema description.
        """
        descriptions = []
        for table_name, df in self.dataframes.items():
            table_info = self.data_catalog.get_catalog_info(table_name)
            table_description = table_info['description'] if table_info else "No description available."
            columns = ", ".join(df.columns)
            descriptions.append(f"Table {table_name} ({columns}): {table_description}")
        return "; ".join(descriptions)
    
    def is_prompt_related_to_schema(self, user_prompt):
        """
        Check if the user prompt is related to schema-level questions.

        Args:
        user_prompt (str): The original user prompt.

        Returns:
        bool: True if the prompt is related to schema-level questions, False otherwise.
        """
        schema_keywords = ["schema", "tables", "description", "records", "loaded tables", "table description"]
        return any(keyword in user_prompt.lower() for keyword in schema_keywords)


    def is_prompt_related_to_tables(self, user_prompt):
        """
        Check if the user prompt is related to the existing tables.

        Args:
        user_prompt (str): The original user prompt.

        Returns:
        bool: True if the prompt is related to the tables, False otherwise.
        """
        # Dynamically gather all table names and column names from the data catalog
        table_keywords = set(self.data_catalog.list_tables())
        for table_name, df in self.dataframes.items():
            for col in df.columns:
                table_keywords.add(col.lower())
        
        # Check if any of these keywords are in the user prompt
        return any(keyword in user_prompt.lower() for keyword in table_keywords)

    def handle_schema_query(self, user_prompt):
        """
        Handle schema-level queries by providing information about the loaded tables.

        Args:
        user_prompt (str): The original user prompt.

        Returns:
        str: A response containing schema-level information.
        """
        response = []

        # Listing all tables
        if "all tables" in user_prompt.lower() or "loaded tables" in user_prompt.lower():
            table_list = self.data_catalog.list_tables()
            response.append(f"The following tables have been loaded: {', '.join(table_list)}.")

        # Describing tables
        for table_name, df in self.dataframes.items():
            table_info = self.data_catalog.get_catalog_info(table_name)
            table_description = table_info['description'] if table_info else "No description available."
            record_count = df.shape[0]
            response.append(f"Table '{table_name}' has {record_count} records. Description: {table_description}.")

        return "\n".join(response)
    
    def generate_sql_query(self, user_prompt):
        if self.is_prompt_related_to_schema(user_prompt):
            self.logger.info(f"The user prompt '{user_prompt}' is related to schema. Handling as schema query.")
            return None

        if not self.is_prompt_related_to_tables(user_prompt):
            self.logger.info(f"The user prompt '{user_prompt}' is not related to the tables. Returning direct response.")
            return None

        prompt = f"""
        Given the following schema: {self.schema_description}, generate an SQL query that fulfills this request: {user_prompt}.
        Make sure the query is valid SQL and can be run against the schema provided.
        Please do not include any information other than the exact structured sql query that can be executed
        """
        response = self.aws_bedrock.generate_text(prompt)
        self.logger.info(f"Generated SQL query for prompt: {user_prompt}")
        return response['response']


    def generate_direct_response(self, user_prompt):
        """
        Generate a direct response for prompts that are not related to the tables.

        Args:
        user_prompt (str): The original user prompt.

        Returns:
        str: The direct response from the AI model.
        """
        prompt = f"Respond to the following request: {user_prompt}"
        response = self.aws_bedrock.generate_text(prompt)
        self.logger.info(f"Generated direct response for prompt: {user_prompt}")
        return response['response']

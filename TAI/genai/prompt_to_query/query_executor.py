import polars as pl
import logging

class QueryExecutor:
    def __init__(self, dataframes, sql_generator, max_retries=3): #default retry 3 times, then run regular chatbot
        self.dataframes = dataframes
        self.sql_generator = sql_generator
        self.max_retries = max_retries
        self.logger = logging.getLogger('QueryExecutor')

    def execute_query(self, sql_query):
        """
        Execute an SQL query using Polars SQL context.

        Args:
        sql_query (str): The SQL query string.

        Returns:
        Polars DataFrame: The result of the SQL query or None if execution failed.
        """
        context = pl.SQLContext(self.dataframes)
        try:
            result = context.execute(sql_query).collect()
            self.logger.info(f"Executed SQL query: {sql_query}")
            return result
        except Exception as e:
            self.logger.error(f"Query execution error: {e}")
            return None

    def validate_and_execute(self, user_prompt):
        """
        Validate and execute the SQL query. If execution fails, regenerate the query and try again.
        If the user prompt is related to schema or is not related to the tables, return a direct response.

        Args:
        user_prompt (str): The original user prompt.

        Returns:
        tuple: A tuple containing the final SQL query (or None) and the result of the SQL query or direct response.
        """
        if self.sql_generator.is_prompt_related_to_schema(user_prompt):
            # Handle schema-level query
            schema_response = self.sql_generator.handle_schema_query(user_prompt)
            return None, schema_response

        sql_query = self.sql_generator.generate_sql_query(user_prompt)
        if sql_query is None:
            # If not related to tables or schema, generate and return a direct response
            direct_response = self.sql_generator.generate_direct_response(user_prompt)
            return None, direct_response

        retries = 0
        while retries < self.max_retries:
            self.logger.info(f"Attempting to execute SQL query: {sql_query}")

            # Try to execute the SQL query
            result = self.execute_query(sql_query)

            # If the result is valid, return the query and result
            if result is not None:
                return sql_query, result

            # If execution failed, provide feedback to the model and retry
            feedback = f"The SQL query '{sql_query}' failed to execute. Please regenerate a valid SQL query."
            self.logger.info(f"Providing feedback to model: {feedback}")
            self.sql_generator.provide_feedback(feedback)

            retries += 1

        self.logger.error("Max retries reached. Failed to generate a valid SQL query.")
        return sql_query, None
    
    def result_to_text(self, result, user_prompt, sql_query=None):
        """
        Convert the DataFrame result into a descriptive text format or return the direct response.

        Args:
        result (Polars DataFrame or str): The result of the SQL query or direct response.
        user_prompt (str): The original user prompt.
        sql_query (str): The SQL query string, if applicable.

        Returns:
        str: A descriptive text based on the query result or the direct response.
        """
        if isinstance(result, str):
            # If the result is a direct response (string), return it directly
            return result

        if result is None or result.is_empty():
            return f"No results were found based on the query: {sql_query}"

        text_response = f"Based on your request, '{user_prompt}', the results are:\n\n"
        text_response += f"Executed Query: {sql_query}\n\n"
        for row in result.iter_rows(named=True):
            row_description = ", ".join([f"{col}: {val}" for col, val in row.items()])
            text_response += f"{row_description}\n"
        
        return text_response
    

    def result_to_text_v2(self, result, user_prompt, sql_query=None):
        """
        Generate a natural language response using the AI model, incorporating the query results, schema, and descriptions.

        Args:
        result (Polars DataFrame or str): The result of the SQL query or direct response.
        user_prompt (str): The original user prompt.
        sql_query (str): The SQL query string, if applicable.

        Returns:
        str: A dynamically generated response based on the query result.
        """
        if isinstance(result, str):
            # If the result is a direct response (string), return it directly
            return result

        if result is None or result.is_empty():
            return f"I'm sorry, I couldn't find any data matching your request based on the query: '{sql_query}'."

        # Prepare the result data for the AI model
        result_text = "\n".join([", ".join([f"{col}: {val}" for col, val in row.items()]) for row in result.iter_rows(named=True)])

        # Prepare the schema and descriptions
        schema_descriptions = "\n".join([
            f"Table {table_name}: {info['description']}"
            for table_name, info in self.sql_generator.data_catalog.catalog.items()
        ])

        # Create a prompt for the AI model to generate a natural language response
        prompt = f"""
        The user asked: "{user_prompt}"
        The data schema is as follows:
        {schema_descriptions}
        I executed the following SQL query: "{sql_query}"
        The query returned the following data:
        {result_text}
        Please generate a natural language response summarizing the query results, incorporating the schema and its description.
        """

        # Generate the AI response
        response = self.sql_generator.aws_bedrock.generate_text(prompt)
        
        return response['response']

import logging

def init_logger():
    logger = logging.getLogger('TextToSQL')
    handler = logging.FileHandler('text_to_sql.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def get_sql_generation_prompt(schema_description, user_prompt):
    return f"""
    Given the following schema: {schema_description}, generate an SQL query that fulfills this request: {user_prompt}.
    Make sure the query is valid SQL and can be run against the schema provided.
    Please do not include any information other than the exact structured sql query that can be executed.
    """

def get_direct_response_prompt(user_prompt):
    return f"""
    Respond to the following request: {user_prompt}
    """

def get_result_summary_prompt(user_prompt, sql_query, result_text, schema_descriptions):
    return f"""
    The user asked: "{user_prompt}"
    The data schema is as follows:
    {schema_descriptions}
    I executed the following SQL query: "{sql_query}"
    The query returned the following data:
    {result_text}
    Please generate a natural language response summarizing the query results, incorporating the schema and its description.
    """

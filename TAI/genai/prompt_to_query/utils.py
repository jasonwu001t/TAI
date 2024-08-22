import logging

def init_logger():
    logger = logging.getLogger('TextToSQL')
    
    # Check if the logger already has handlers to avoid adding multiple handlers
    if not logger.hasHandlers():
        handler = logging.FileHandler('text_to_sql.log')
        console_handler = logging.StreamHandler()  # Also log to the console
 
        formatter = logging.Formatter('%(asctime)s- %(message)s') # - %(name)s - %(levelname)s 
        handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)
    
    return logger


def get_sql_generation_prompt(schema_description, user_prompt):
    """
    Generate a prompt to instruct the AI to create an SQL query based on the schema and user request.
    
    Args:
    schema_description (str): Description of the schema.
    user_prompt (str): The user request or query.

    Returns:
    str: The formatted prompt for SQL generation.
    """
    return f"""
    Given the following schema: {schema_description}, generate an SQL query that fulfills this request: {user_prompt}.
    Make sure the query is valid SQL and can be run against the schema provided.
    Please do not include any information other than the exact structured SQL query that can be executed.
    """

def get_direct_response_prompt(user_prompt):
    """
    Generate a prompt for the AI to provide a direct response to the user's request.
    
    Args:
    user_prompt (str): The user request or query.

    Returns:
    str: The formatted prompt for a direct response.
    """
    return f"""
    Respond to the following request: {user_prompt}
    """

def get_result_summary_prompt(user_prompt, sql_query, result_text, schema_descriptions):
    """
    Generate a prompt to instruct the AI to summarize the SQL query results in natural language.
    
    Args:
    user_prompt (str): The original user request.
    sql_query (str): The SQL query that was generated and executed.
    result_text (str): The text summary of the query results.
    schema_descriptions (str): Description of the schema.

    Returns:
    str: The formatted prompt for summarizing the query results.
    """
    return f"""
    The user asked: "{user_prompt}"
    The data schema is as follows:
    {schema_descriptions}
    I executed the following SQL query: "{sql_query}"
    The query returned the following data:
    {result_text}
    Please generate a natural language response summarizing the query results, incorporating the schema and its description.
    """

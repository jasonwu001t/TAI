# prompt_template.py

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



TABLE_DETAILS = {
    "customers": "Customers purchase products from employees",
    "employees": "Employees sell products to customers",
    "orders": "Events of customers purchasing from employees",
    "products": "Products are supplied by vendors. Products belong to subcategories",
    "vendors": "Vendors supply products",
    "vendorproduct": "Use this table exclusively when joining with the 'vendors' table. Avoid using it in any other scenarios.",
    "productcategories": "Product categories are made up of multiple product subcategories",
    "productsubcategories": "Each product belongs to a product subcategory",
}

SQL_TEMPLATE_STR = """Given an input question, first create a syntactically correct {dialect} query to run, 
    then look at the results of the query and return the answer.
    You can order the results by a relevant column to return the most interesting examples in the database.\n\n
    Never query for all the columns from a specific table, only ask for a few relevant columns given the question.\n\n
    Pay attention to use only the column names that you can see in the schema description. Be careful to not query for columns that do not exist.
    Qualify column names with the table name when needed.

    If a column name contains a space, always wrap the column name in double quotes.

    You are required to use the following format, each taking one line:\n\nQuestion: Question here\nSQLQuery: SQL Query to run\n
    SQLResult: Result of the SQLQuery\nAnswer: Final answer here\n\nOnly use tables listed below.\n{schema}\n\n
    Do not under any circumstance use SELECT * in your query.
    If the user is asking about purchasing—interpret it as ordering.

    Here are some useful examples:
    {few_shot_examples}

    Question: {query_str}\nSQLQuery: """

RESPONSE_TEMPLATE_STR = """If the <SQL Response> below contains data, then given an input question, synthesize a response from the query results.
    If the <SQL Response> is empty, then you should not synthesize a response and instead respond that no data was found for the quesiton..\n

    \nQuery: {query_str}\nSQL: {sql_query}\n<SQL Response>: {context_str}\n</SQL Response>\n

    Do not make any mention of queries or databases in your response, instead you can say 'according to the latest information' .\n\n
    Please make sure to mention any additional details from the context supporting your response.
    If the final answer contains <dollar_sign>$</dollar_sign>, ADD '\' ahead of each <dollar_sign>$</dollar_sign>.

    Response: """
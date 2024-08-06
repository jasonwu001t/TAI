import json
import yfinance
from TAI.utils import ConfigLoader
import logging
import boto3
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_aws import ChatBedrock

# Define functions
def get_stock_price(symbol):
    """Retrieves stock price from yfinance API"""
    price = yfinance.get_stock_price(symbol)  # Assumed to be a valid method in yfinance
    return price

def get_interest_rate(product, date):
    """Get interest rate for the product based on user-defined date"""
    rate = function.get_rate(product, date)  # Assumed to be a valid method in a defined function module
    return rate

def get_gdp():
    """Retrieves the latest US GDP data"""
    gdp = function.gdp()  # Assumed to be a valid method in a defined function module
    return gdp

# Define the AWSBedrock class
class AWSBedrock:
    def __init__(self, region_name='us-east-1', endpoint_url='https://bedrock.us-east-1.amazonaws.com'):
        self.bedrock = boto3.client(service_name='bedrock', region_name=region_name, endpoint_url=endpoint_url)
        self.bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")
        self.available_models = self.list_available_models()
        self.active_models = self.get_active_models()
            
    def list_available_models(self):
        return self.bedrock.list_foundation_models()['modelSummaries']
    
    def get_active_models(self):
        active_models = {}
        for model in self.available_models:
            if model.get('modelLifecycle')['status'] == 'ACTIVE':
                active_models[model.get('modelId')] = model
        return active_models
    
    def get_model_info(self, model_id):
        if model_id in self.active_models:
            return self.active_models[model_id]
        else:
            return None
        
    def generate_text(self, model_id, prompt, **kwargs):
        """
        Generate text using the specified AWS Bedrock model.
        Supported kwargs:
        -   max tokens (int): Maximum number of tokens to generate.
        -   temperature (float): Temperature for text generation.
        -   top_k (int): Number of top-k tokens to consider.
        -   top_P (float): Top-p for nucleus sampling.
        -   stop_sequences (list): List of stop sequences.
        """
        model_kwargs = {
            "max_tokens": kwargs.get("max_tokens", 2048),
            "temperature": kwargs.get("temperature", 0.1),
            "top_k": kwargs.get("top_k", 250),
            "top_p": kwargs.get("top_p", 1),
            "stop_sequences": kwargs.get("stop_sequences", ["\n\nHuman"]),
        }

        model = ChatBedrock(
            client=self.bedrock_runtime, 
            model_id=model_id,
            model_kwargs=model_kwargs,
        )

        messages = [
            ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."),
            ("human", prompt)
        ]

        prompt_template = ChatPromptTemplate.from_messages(messages)
        chain = prompt_template | model | StrOutputParser()
        response = chain.invoke({"question": prompt_template})
        return response

# Function dictionary with descriptions
function_dict = {
    "get_stock_price": {
        "function": get_stock_price,
        "description": "Fetches the current stock price for a given symbol."
    },
    "get_interest_rate": {
        "function": get_interest_rate,
        "description": "Retrieves the interest rate for a specified product and date."
    },
    "get_gdp": {
        "function": get_gdp,
        "description": "Retrieves the latest US GDP data."
    }
}

# Function to get the function name and parameters from the AI
def get_function_and_params(bedrock, user_input, model_id):
    descriptions = "\n".join([f"- {name}: {details['description']}" for name, details in function_dict.items()])
    prompt = f"""
    User input: {user_input}
    Please choose the appropriate function to call from the following list and provide the necessary parameters:
    {descriptions}
    Respond with the function name and parameters in JSON format.
    """
    response = bedrock.generate_text(model_id=model_id, prompt=prompt)
    return response.strip()

# Function to validate the AI response
def validate_function_and_params(response):
    try:
        response_dict = json.loads(response)
        function_name = response_dict.get("function_name")
        params = response_dict.get("parameters", {})
        if function_name in function_dict:
            return function_name, params
        else:
            return None, {}
    except json.JSONDecodeError:
        return None, {}

# Function to execute the selected function
def execute_function(bedrock, user_input, model_id):
    response = get_function_and_params(bedrock, user_input, model_id)
    function_name, params = validate_function_and_params(response)
    
    if function_name:
        function_to_call = function_dict[function_name]["function"]
        if function_name == "get_stock_price" and "symbol" in params:
            return function_to_call(params["symbol"])
        elif function_name == "get_interest_rate" and "product" in params and "date" in params:
            return function_to_call(params["product"], params["date"])
        elif function_name == "get_gdp":
            return function_to_call()
        elif function_name == "get_interest_rate":
            return "Please provide the date for the interest rate."
        else:
            return "Missing required parameters."
    else:
        return "No matching function found."

# Example usage
if __name__ == "__main__":
    bedrock = AWSBedrock()
    user_input = "Can you tell me the stock price of Tesla?"  # Example input
    model_id = "your_model_id"  # Replace with your actual model ID
    print(execute_function(bedrock, user_input, model_id))

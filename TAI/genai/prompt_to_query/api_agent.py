import json
import logging
import uuid
import re
from typing import Dict, List, Any
import requests
from datetime import datetime
import pytz
from TAI.genai import AWSBedrock
from .utils import init_logger


class APICatalog:
    def __init__(self):
        self.catalog: Dict[str, Dict[str, Any]] = {}
        self.embeddings: Dict[str, List[float]] = {}

    def load_from_json(self, json_file: str):
        with open(json_file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                # New structure (list of endpoints)
                for endpoint_info in data:
                    self.add_endpoint(
                        endpoint=endpoint_info['endpoint'],
                        method=endpoint_info['method'],
                        description=endpoint_info['description'],
                        parameters=endpoint_info.get('parameters', []),
                        response_structure=endpoint_info.get(
                            'response_structure', {})
                    )
            elif isinstance(data, dict):
                # Original structure (dictionary of endpoints)
                for endpoint, info in data.items():
                    self.add_endpoint(
                        endpoint=endpoint,
                        method=info.get('method', 'GET'),
                        description=info.get('description', ''),
                        parameters=info.get('parameters', []),
                        response_structure=info.get('response_structure', {})
                    )
            else:
                raise ValueError("Invalid JSON structure in api_catalog.json")

    def add_endpoint(self, endpoint: str, method: str, description: str, parameters: List[Dict[str, Any]], response_structure: Any):
        self.catalog[endpoint] = {
            'method': method,
            'description': description,
            'parameters': parameters,
            'response_structure': response_structure
        }

    def get_endpoint_info(self, endpoint: str) -> Dict[str, Any]:
        return self.catalog.get(endpoint, None)

    def list_endpoints(self) -> List[str]:
        return list(self.catalog.keys())

    def search_endpoints(self, query: str) -> List[str]:
        return [endpoint for endpoint in self.catalog.keys() if query.lower() in endpoint.lower() or query.lower() in self.catalog[endpoint]['description'].lower()]

    def generate_embeddings(self, aws_bedrock: AWSBedrock):
        for endpoint, info in self.catalog.items():
            endpoint_text = f"{endpoint}: {info['description']}"
            self.embeddings[endpoint] = aws_bedrock.generate_embedding(
                endpoint_text)


class APIHandler:
    def __init__(self, aws_bedrock: AWSBedrock, api_catalog: APICatalog, threshold: float = 0.4):
        self.aws_bedrock = aws_bedrock
        self.api_catalog = api_catalog
        self.threshold = threshold
        self.logger = logging.getLogger('APIHandler')
        # Set to DEBUG for more detailed logs
        self.logger.setLevel(logging.DEBUG)
        self.session_id = str(uuid.uuid4())
        self.api_catalog.generate_embeddings(self.aws_bedrock)
        self.last_called_url = None  # Add this line

    def chatbot(self, prompt: str) -> str:
        response = self.aws_bedrock.generate_text(prompt)
        return response['response']

    def find_relevant_endpoint(self, user_prompt: str) -> tuple:
        prompt_embedding = self.aws_bedrock.generate_embedding(user_prompt)
        similarities = {}
        max_similarity = 0
        relevant_endpoint = None

        for endpoint, embedding in self.api_catalog.embeddings.items():
            similarity = self.aws_bedrock.calculate_similarity(
                prompt_embedding, embedding)
            similarities[endpoint] = similarity
            if similarity > max_similarity:
                max_similarity = similarity
                relevant_endpoint = endpoint

        self.logger.info(f"Maximum similarity: {max_similarity}")
        return relevant_endpoint, similarities

    def extract_stock_symbol(self, user_prompt: str) -> str:
        extraction_prompt = f"Extract only the stock (eg. AAPL, TSLA, MSFT) or ETF ticker symbol(eg. SPY, QQQ) from this text: '{user_prompt}'. Respond with just the ticker symbol in uppercase, or 'NONE' if no valid ticker is found."
        response = self.chatbot(extraction_prompt)
        extracted_symbol = response.strip().upper()
        return extracted_symbol if extracted_symbol != 'NONE' else None

    def extract_economy_indicator(self, user_prompt: str) -> str:
        extraction_prompt = f"Extract only the economy indicator (e.g., m2, gdp, cpi, ppi) from this text: '{user_prompt}'. Respond with just the indicator in lowercase, or 'none' if no valid indicator is found."
        response = self.chatbot(extraction_prompt)
        extracted_indicator = response.strip().lower()
        return extracted_indicator if extracted_indicator != 'none' else None

    def execute_api_request(self, endpoint: str, user_prompt: str) -> Dict[str, Any]:
        self.logger.debug(f"Executing API request for endpoint: {endpoint}")
        endpoint_info = self.api_catalog.get_endpoint_info(endpoint)
        if not endpoint_info:
            self.logger.error(f"No endpoint info found for: {endpoint}")
            return None

        if endpoint == "/latest_stock_trade/{symbol_or_symbols}":
            symbol = self.extract_stock_symbol(user_prompt)
            if not symbol:
                self.logger.error("No valid stock symbol found in the prompt")
                return None
            url = f"https://api.nativequant.com{endpoint.replace('{symbol_or_symbols}', symbol)}"
        elif endpoint == "/economy_short/{indicator}":
            indicator = self.extract_economy_indicator(user_prompt)
            if not indicator:
                self.logger.error(
                    "No valid economy indicator found in the prompt")
                return None
            url = f"https://api.nativequant.com{endpoint.replace('{indicator}', indicator)}"
        else:
            # For other endpoints, use the chatbot to generate parameters
            params_prompt = f"Based on the user request '{user_prompt}', generate appropriate parameters for the API endpoint '{endpoint}' with the following available parameters: {endpoint_info['parameters']}. Return the result as a JSON object with only the required parameters."
            self.logger.debug(
                f"Generating parameters with prompt: {params_prompt}")
            params_response = self.chatbot(params_prompt)
            self.logger.debug(
                f"Generated parameters response: {params_response}")
            try:
                params = json.loads(params_response)
                self.logger.debug(f"Parsed parameters: {params}")
                url = f"https://api.nativequant.com{endpoint}"
                for param, value in params.items():
                    url = url.replace(f"{{{param}}}", str(value).lower())
            except json.JSONDecodeError:
                self.logger.error(
                    f"Failed to parse parameters: {params_response}")
                # Attempt to extract the JSON object from the response
                match = re.search(r'\{.*\}', params_response, re.DOTALL)
                if match:
                    try:
                        params = json.loads(match.group())
                        self.logger.debug(
                            f"Extracted and parsed parameters: {params}")
                        url = f"https://api.nativequant.com{endpoint}"
                        for param, value in params.items():
                            url = url.replace(
                                f"{{{param}}}", str(value).lower())
                    except json.JSONDecodeError:
                        self.logger.error("Failed to parse extracted JSON")
                        return None
                else:
                    return None

        # Log the full endpoint URL
        self.logger.info(f"Calling API endpoint: {url}")
        self.last_called_url = url  # Add this line

        try:
            response = requests.get(url)
            self.logger.info(f"Full URL called: {response.url}")
            response.raise_for_status()
            api_response = response.json()
            self.logger.debug(f"API response: {api_response}")
            return api_response
        except requests.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            return None

    def process_api_response(self, response: Dict[str, Any], endpoint: str) -> Dict[str, Any]:
        self.logger.debug(f"Processing API response for endpoint: {endpoint}")
        self.logger.debug(f"Raw API response: {response}")

        endpoint_info = self.api_catalog.get_endpoint_info(endpoint)
        if not endpoint_info or not response:
            self.logger.error(
                f"No endpoint info or empty response for: {endpoint}")
            return None

        if endpoint == "/latest_stock_trade/{symbol_or_symbols}":
            symbol = list(response.keys())[0]
            utc_timestamp = datetime.fromisoformat(
                response[symbol]["timestamp"].replace('Z', '+00:00'))
            est_timezone = pytz.timezone('US/Eastern')
            est_timestamp = utc_timestamp.astimezone(est_timezone)
            formatted_timestamp = est_timestamp.strftime(
                "%Y-%m-%d %I:%M:%S %p %Z")
            processed_response = {
                "symbol": symbol,
                "price": response[symbol]["price"],
                "timestamp": formatted_timestamp
            }
            self.logger.debug(f"Processed response: {processed_response}")
            return processed_response

        self.logger.debug(
            f"Returning unprocessed response for endpoint: {endpoint}")
        return response

    def generate_text_response(self, user_prompt: str, endpoint: str, processed_response: Dict[str, Any]) -> str:
        if endpoint == "/latest_stock_trade/{symbol_or_symbols}":
            return f"The latest stock price for {processed_response['symbol']} is ${processed_response['price']} as of {processed_response['timestamp']}."

        response_prompt = f"Based on the user request '{user_prompt}' and the API response {json.dumps(processed_response)}, generate a human-readable response."
        return self.chatbot(response_prompt)

    def list_all_endpoints(self) -> str:
        endpoints = self.api_catalog.list_endpoints()
        return "Available endpoints:\n" + "\n".join(endpoints)

    def search_endpoints(self, query: str) -> str:
        results = self.api_catalog.search_endpoints(query)
        if results:
            return f"Matching endpoints for '{query}':\n" + "\n".join(results)
        else:
            return f"No endpoints found matching '{query}'."

    def describe_endpoint(self, endpoint: str) -> str:
        info = self.api_catalog.get_endpoint_info(endpoint)
        if info:
            description = f"Endpoint: {endpoint}\n"
            description += f"Method: {info['method']}\n"
            description += f"Description: {info['description']}\n"
            description += "Parameters:\n"
            for param in info['parameters']:
                description += f"  - {param['name']} ({param['type']}): {'Required' if param.get('required', False) else 'Optional'}\n"
            description += f"Response structure: {json.dumps(info['response_structure'], indent=2)}"
            return description
        else:
            return f"No information found for endpoint: {endpoint}"


class APIAgent:
    def __init__(self, api_catalog_path: str = 'api_catalog.json', threshold: float = 0.4):
        self.logger = init_logger()
        self.logger.info("Initializing APIAgent...")
        self.api_catalog = APICatalog()
        self.api_catalog.load_from_json(api_catalog_path)
        self.aws_bedrock = AWSBedrock()
        self.api_handler = APIHandler(
            self.aws_bedrock, self.api_catalog, threshold)
        self.logger.info("APIAgent initialized successfully.")

    def process_prompt(self, user_prompt: str, detailed_output: bool = False) -> str:
        self.logger.debug(f"Processing user prompt: {user_prompt}")

        # Handle special commands first
        if user_prompt.lower() == "list all endpoints":
            return self.api_handler.list_all_endpoints()

        if user_prompt.lower().startswith("search endpoints "):
            query = user_prompt[17:].strip()
            return self.api_handler.search_endpoints(query)

        if user_prompt.lower().startswith("describe endpoint "):
            endpoint = user_prompt[18:].strip()
            return self.api_handler.describe_endpoint(endpoint)

        # Check if the prompt is asking for a stock price
        if any(word in user_prompt.lower() for word in ['stock', 'price']):
            relevant_endpoint = "/latest_stock_trade/{symbol_or_symbols}"
            # Set similarity to 1 for stock price queries
            similarities = {relevant_endpoint: 1.0}
        else:
            relevant_endpoint, similarities = self.api_handler.find_relevant_endpoint(
                user_prompt)

        # Log embedding correlations
        correlation_info = "Prompt and API endpoints embedding correlations:\n"
        correlation_info += f"User prompt: '{user_prompt}'\n"
        for endpoint, similarity in similarities.items():
            correlation_info += f"{endpoint}: {similarity:.4f}\n"
        self.logger.info(correlation_info)

        if not relevant_endpoint or max(similarities.values()) < self.api_handler.threshold:
            self.logger.warning(
                "No relevant endpoint found or similarity below threshold")
            ai_response = self.api_handler.chatbot(
                f"The following prompt does not seem to relate to any available API endpoints. Please provide a general response: {user_prompt}")
            return f"I'm sorry, but your prompt doesn't seem to relate to any available API endpoints. Here's a general response:\n\n{ai_response}\n\nYou can use the following commands to explore available endpoints:\n" \
                   "- 'list all endpoints' to see all available endpoints\n" \
                   "- 'search endpoints <query>' to search for specific endpoints\n" \
                   "- 'describe endpoint <endpoint>' to get detailed information about a specific endpoint"

        api_response = self.api_handler.execute_api_request(
            relevant_endpoint, user_prompt)
        if not api_response:
            self.logger.error("API request failed or returned no response")
            full_url = self.api_handler.last_called_url  # Assume this attribute exists
            return f"I'm sorry, but there was an error while trying to fetch the information you requested. The API call to {full_url} failed. Please try again later."

        processed_response = self.api_handler.process_api_response(
            api_response, relevant_endpoint)
        self.logger.debug(f"Processed response: {processed_response}")

        text_response = self.api_handler.generate_text_response(
            user_prompt, relevant_endpoint, processed_response)
        self.logger.debug(f"Generated text response: {text_response}")

        if detailed_output:
            return f"{correlation_info}\n\nSelected endpoint: {relevant_endpoint}\n\nAPI Response: {api_response}\n\nProcessed Response: {processed_response}\n\nText Response: {text_response}"
        else:
            return text_response


if __name__ == "__main__":
    agent = APIAgent()
    print("Welcome to the API Agent! Here are some sample commands you can try:")
    print("1. List all endpoints: 'list all endpoints'")
    print("2. Search endpoints: 'search endpoints <query>'")
    print("3. Describe an endpoint: 'describe endpoint <endpoint>'")
    print("4. Ask a question: '<your question>'")
    print("5. Exit: 'exit'")
    print("\nSample usage:")

    sample_commands = [
        "list all endpoints",
        "search endpoints stock",
        "describe endpoint /describe_perc_change/{ticker}/{expiry_date}",
        "What's the latest stock price for Apple?",
        "Get me the economic data for GDP growth",
        "Show me the option chain for AAPL expiring next month",
        "exit"
    ]

    for command in sample_commands:
        print(f"\nCommand: {command}")
        if command.lower() == 'exit':
            print("Exiting the API Agent.")
            break
        result = agent.process_prompt(command)
        print("Response:")
        print(result)
        print("\n" + "-"*50)

    print("\nNow it's your turn! Enter your questions or commands:")
    while True:
        user_prompt = input("Enter your question (or 'exit' to quit): ")
        if user_prompt.lower() == 'exit':
            print("Thank you for using the API Agent. Goodbye!")
            break
        result = agent.process_prompt(user_prompt)
        print("Response:")
        print(result)
        print("\n" + "-"*50)

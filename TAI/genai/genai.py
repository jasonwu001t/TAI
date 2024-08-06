# genai.py
from TAI.utils import ConfigLoader
import openai
import logging
import boto3
# from GTI.auth_handler import OpenAIAuth, AWSAuth
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_aws import ChatBedrock

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
            "top_p": kwargs. get("top_p", 1),
            "stop_sequences": kwargs.get("stop_sequences", ["\n\nHuman"]),
            }

        model = ChatBedrock(
            client=self.bedrock_runtime, 
            model_id= model_id,
            model_kwargs=model_kwargs,
        )

        messages = [
            ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."),
            ("human", prompt)
        ]

        prompt = ChatPromptTemplate.from_messages(messages)
        chain = prompt | model | StrOutputParser()
        response = chain.invoke({"question": prompt})
        return response

class OpenAIAuth:
    def __init__(self):
        config_loader = ConfigLoader()
        self.api_key = config_loader.get_config('OpenAI', 'api_key')

    def get_api_key(self):
        return self.api_key
    
class GenAI:
    def __init__(self):
        auth = OpenAIAuth()
        openai.api_key = auth.get_api_key()

    def generate_text(self, prompt, max_tokens=150):
        response = openai.Completion.create(
            engine="davinci",
            prompt=prompt,
            max_tokens=max_tokens
        )
        return response.choices[0].text.strip()


if __name__ == "__main__":

    bedrock = AWSBedrock()
    print (bedrock.available_models)
    print (bedrock.active_models)
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    model_info = bedrock.get_model_info(model_id)
    print (model_info)

    prompt = "Please tell me a joke"
    response = bedrock.generate_text(model_id, prompt, max_token=1024, temprature = 0.5)
    print (response)

    
    genai = GenAI()
    # Example usage
    prompt = "Explain the concept of artificial intelligence."
    generated_text = genai.generate_text(prompt)
    print(generated_text)

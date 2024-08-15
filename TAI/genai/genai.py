import boto3
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_aws import ChatBedrock

class AWSBedrock:
    def __init__(self, region_name='us-east-1', endpoint_url='https://bedrock.us-east-1.amazonaws.com'):
        self.bedrock = boto3.client(service_name='bedrock', region_name=region_name, endpoint_url=endpoint_url)
        self.bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")
        self.conversation_history = []

    @property
    def available_models(self):
        if not hasattr(self, '_available_models'):
            self._available_models = self.list_available_models()
        return self._available_models

    @property
    def active_models(self):
        if not hasattr(self, '_active_models'):
            self._active_models = self.get_active_models()
        return self._active_models

    def list_available_models(self):
        try:
            return self.bedrock.list_foundation_models()['modelSummaries']
        except Exception as e:
            print(f"Error listing models: {e}")
            return []

    def get_active_models(self):
        active_models = {}
        for model in self.available_models:
            if model.get('modelLifecycle')['status'] == 'ACTIVE':
                active_models[model.get('modelId')] = model
        return active_models
    
    def get_model_info(self, model_id):
        return self.active_models.get(model_id)

    def _generate_response(self, model_id, messages, **kwargs):
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

        # Create the prompt template directly from the messages
        prompt = "".join([f"{role.capitalize()}: {message}\n" for role, message in messages])
        
        try:
            # Use invoke instead of __call__
            response = model.invoke(prompt)
            # Extract the content assuming response is an object with a .content attribute
            content = response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            print(f"Error generating response: {e}")
            content = "I'm sorry, something went wrong."
        return content
    
    def generate_text(self, model_id, prompt, **kwargs):
        messages = [
            ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."),
            ("human", prompt)
        ]
        return self._generate_response(model_id, messages, **kwargs)

    def generate_conversational_response(self, model_id, user_input, **kwargs):
        self.conversation_history.append(("human", user_input))

        if len(self.conversation_history) == 1:
            self.conversation_history.insert(0, ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."))

        response_content = self._generate_response(model_id, self.conversation_history, **kwargs)

        self.conversation_history.append(("assistant", response_content))
        return response_content

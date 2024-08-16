import boto3
from langchain_aws import BedrockLLM
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain

class AWSBedrock:
    def __init__(self, 
                 region_name='us-east-1', 
                 endpoint_url='https://bedrock.us-east-1.amazonaws.com',
                 model_id="anthropic.claude-v2:1", 
                 model_kwargs=None, 
                 max_token_limit=512):
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.model_id = model_id
        self.model_kwargs = model_kwargs if model_kwargs is not None else {
                                                                            "temperature": 0.9,
                                                                            "top_p": 0.5,
                                                                            "max_tokens_to_sample": 512
                                                                        }
        self.max_token_limit = max_token_limit
        self.conversation_history = []

        self.bedrock = boto3.client(service_name='bedrock', region_name=self.region_name, endpoint_url=self.endpoint_url)
        self.bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")

        self.llm = self.init_llm()
        self.memory = self.init_memory()

    def init_llm(self):
        try:
            demo_llm = BedrockLLM(
                client=self.bedrock_runtime,
                model_id=self.model_id,
                model_kwargs=self.model_kwargs
            )
            return demo_llm
        except Exception as e:
            print(f"Failed to initialize the LLM: {e}")
            return None

    def init_memory(self):
        if self.llm is not None:
            try:
                memory = ConversationBufferMemory(
                    llm=self.llm,
                    max_token_limit=self.max_token_limit
                )
                return memory
            except Exception as e:
                print(f"Failed to initialize memory: {e}")
        return None

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

        model = BedrockLLM(
            client=self.bedrock_runtime,
            model_id=model_id,
            model_kwargs=model_kwargs,
        )

        # Create the prompt template directly from the messages
        prompt = "".join([f"{role.capitalize()}: {message}\n" for role, message in messages])
        
        try:
            response = model.invoke(prompt)
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

    def conversation(self, input_text):
        if self.llm is not None and self.memory is not None:
            try:
                llm_conversation = ConversationChain(
                    llm=self.llm,
                    memory=self.memory,
                    verbose=True
                )
                chat_reply = llm_conversation.invoke(input=input_text)
                return chat_reply
            except Exception as e:
                print(f"Error during conversation: {e}")
        return None

# Usage example
if __name__ == "__main__":
    chatbot = AWSBedrock()
    user_input = "Hello, how can you help me today?"
    response = chatbot.conversation(input_text=user_input)
    print(response)

import boto3
from botocore.exceptions import ClientError
from langchain_aws import BedrockLLM
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain
from langchain_aws import ChatBedrock

# Makesure you have configured bedrock at the right region, url, and model
class AWSBedrock:
    def __init__(self, 
                 region_name='us-west-2', 
                 endpoint_url='https://bedrock.us-west-2.amazonaws.com',
                 model_id= 'anthropic.claude-instant-v1', #"anthropic.claude-v2:1", 
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
        self.bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name=self.region_name) #"us-west-2") for conversational chatbot
        self.bedrock_agent = boto3.client(service_name='bedrock-agent-runtime',region_name=self.region_name) #for knowledge based chatbot

        self.llm = self.init_llm()
        self.memory = self.init_memory()

     # CHATBOT for knowledge based agent, require configuration in bedrock, model id already selected in bedrock config
    def invoke_agent(self, agent_id, agent_alias_id, session_id, prompt):
        try: # See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-agent-runtime/client/invoke_agent.html
            response = self.bedrock_agent.invoke_agent(
                agentId=agent_id,
                agentAliasId=agent_alias_id,
                enableTrace=True,
                sessionId=session_id,
                inputText=prompt,
            )

            output_text = ""
            citations = []
            trace = {}
            for event in response.get("completion"):
                # Combine the chunks to get the output text
                if "chunk" in event:
                    chunk = event["chunk"]
                    output_text += chunk["bytes"].decode()
                    if "attribution" in chunk:
                        citations = citations + chunk["attribution"]["citations"]
                # Extract trace information from all events
                if "trace" in event:
                    for trace_type in ["preProcessingTrace", "orchestrationTrace", "postProcessingTrace"]:
                        if trace_type in event["trace"]["trace"]:
                            if trace_type not in trace:
                                trace[trace_type] = []
                            trace[trace_type].append(event["trace"]["trace"][trace_type])
        except ClientError as e:
            raise
        return {
            "response": output_text,
            "citations": citations,
            "trace": trace
        }

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
    
    # CHATBOT for conversational chat
    def conversation(self, prompt):
        if self.llm is not None and self.memory is not None:
            try:
                llm_conversation = ConversationChain(
                    llm=self.llm,
                    memory=self.memory,
                    verbose=True
                )
                chat_reply = llm_conversation.invoke(input=prompt)
                return chat_reply
            except Exception as e:
                print(f"Error during conversation: {e}")
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

    def _generate_response(self, messages, **kwargs):
        model_kwargs = {
            "max_tokens": kwargs.get("max_tokens", 2048),
            "temperature": kwargs.get("temperature", 0.1),
            "top_k": kwargs.get("top_k", 250),
            "top_p": kwargs.get("top_p", 1),
            "stop_sequences": kwargs.get("stop_sequences", ["\n\nHuman"]),
        }
        model = ChatBedrock(
            client=self.bedrock_runtime, 
            model_id=self.model_id,
            model_kwargs=model_kwargs,
        )
        # Create the prompt template directly from the messages
        prompt = "".join([f"{role.capitalize()}: {message}\n" for role, message in messages])
        content_dict = {}
        try:
            response = model.invoke(prompt)
            content_dict['response']= response.content
            # content = response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            print(f"Error generating response: {e}")
            content_dict['response'] = "I'm sorry, something went wrong."
        # print ('pppppp', content)
        return content_dict 
    
    #CHATBOT for immediate prompt, this does not cache chat history, good to use as function handler
    def generate_text(self, prompt, **kwargs):
        messages = [
            ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."),
            ("human", prompt)
        ]
        return self._generate_response(messages, **kwargs)

    #CHATBOT for conversationsl, OVERLAPPED with def conversation(self, prompt), THIS ONE CAN DEPRECATE
    def generate_conversational_response(self, prompt, **kwargs):
        self.conversation_history.append(("human", prompt))

        if len(self.conversation_history) == 1:
            self.conversation_history.insert(0, 
                                             ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."))

        response_content = self._generate_response(self.conversation_history, model_id=self.model_id, **kwargs)

        self.conversation_history.append(("ai", response_content))
        return response_content

# Usage example
if __name__ == "__main__":
    chatbot = AWSBedrock()
    user_input = "Hello, how can you help me today?"
    response = chatbot.conversation(input_text=user_input)
    print(response)

import boto3, json
from botocore.exceptions import ClientError
from langchain_aws import BedrockLLM
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain
from langchain_aws import ChatBedrock
import numpy as np
import pandas as pd

# Make sure you have configured Bedrock with the correct region, URL, and model in AWS Console
class AWSBedrock:
    def __init__(self, 
                 region_name='us-west-2', 
                 endpoint_url='https://bedrock.us-west-2.amazonaws.com',
                 model_id='anthropic.claude-instant-v1', # 'anthropic.claude-3-5-sonnet-20240620-v1:0'
                 embedding_model_id='amazon.titan-embed-text-v1', #'amazon.titan-embed-text-v1', 'amazon.titan-embed-g1-text-02', 'amazon.titan-embed-text-v2:0'
                 model_kwargs=None, 
                 max_tokens=512):
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.model_id = model_id
        self.embedding_model_id = embedding_model_id
        # Default model_kwargs with max_tokens consolidated
        self.model_kwargs = model_kwargs if model_kwargs is not None else {
            "temperature": 0.9,
            "top_p": 0.5,
            "max_tokens": max_tokens,
            "top_k": 250,
            "stop_sequences": ["\n\nHuman"]
        }
        self.max_tokens = max_tokens
        self.conversation_history = []

        self.bedrock = boto3.client(service_name='bedrock', region_name=self.region_name, endpoint_url=self.endpoint_url)
        self.bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name=self.region_name)
        self.bedrock_agent = boto3.client(service_name='bedrock-agent-runtime', region_name=self.region_name)

        self.llm = self.init_llm()
        self.memory = self.init_memory()

    # Initialize the language model (LLM) with provided parameters
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

    # Initialize memory to store conversation history
    def init_memory(self):
        if self.llm is not None:
            try:
                memory = ConversationBufferMemory(
                    llm=self.llm,
                    max_token_limit=self.max_tokens
                )
                return memory
            except Exception as e:
                print(f"Failed to initialize memory: {e}")
        return None

    # Conversational chatbot with history
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

    # List available models from Bedrock
    def list_available_models(self):
        try:
            return self.bedrock.list_foundation_models()['modelSummaries']
        except Exception as e:
            print(f"Error listing models: {e}")
            return []

    # Get active models from the available models list
    def get_active_models(self):
        active_models = {}
        for model in self.available_models:
            if model.get('modelLifecycle')['status'] == 'ACTIVE':
                active_models[model.get('modelId')] = model
        return active_models.keys()

    # Retrieve information about a specific model
    def get_model_info(self, model_id):
        return self.active_models.get(model_id)

    # Generate a response using the LLM with the stored model_kwargs
    def _generate_response(self, messages, **kwargs):
        model = ChatBedrock(
            client=self.bedrock_runtime, 
            model_id=self.model_id,
            model_kwargs=self.model_kwargs,
        )
        # Create the prompt template directly from the messages
        prompt = "".join([f"{role.capitalize()}: {message}\n" for role, message in messages])
        content_dict = {}
        try:
            response = model.invoke(prompt)
            content_dict['response'] = response.content
        except Exception as e:
            print(f"Error generating response: {e}")
            content_dict['response'] = "I'm sorry, something went wrong."
        return content_dict

    # Generate text response while maintaining conversation history
    def generate_text(self, prompt, **kwargs):
        self.conversation_history.append(("human", prompt))

        if len(self.conversation_history) == 1:
            self.conversation_history.insert(0, ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."))

        response_content = self._generate_response(self.conversation_history, **kwargs)
        self.conversation_history.append(("ai", response_content['response']))

        return response_content
    
    # for immediate prompt, this does not cache chat history, good to use as function handler
    def direct_response(self, prompt, **kwargs):
        messages = [
            ("system", "You are an honest and helpful bot. You reply to the question in a concise and direct way."),
            ("human", prompt)
        ]
        return self._generate_response(messages, **kwargs)
    
    # Embedding generation using Amazon Titan Embedding model
    def generate_embedding(self, input_data):
        """
        Generate an embedding for the provided input data, which can be a string or a pandas DataFrame.
        
        Args:
        input_data (str or pd.DataFrame): The input text or DataFrame to generate an embedding for.

        Returns:
        np.ndarray: The generated embedding as a numpy array.
        """
        try:
            # Check if the input is a DataFrame and convert it to a string representation if necessary
            if isinstance(input_data, pd.DataFrame):
                input_data = self.dataframe_to_string(input_data)
            
            # Prepare the input payload as required by the model
            body = json.dumps({
                "inputText": input_data,
            })

            # Define the necessary headers for the request
            accept = 'application/json'
            content_type = 'application/json'

            # Invoke the model with the correct parameters
            response = self.bedrock_runtime.invoke_model(
                body=body,
                modelId=self.embedding_model_id,
                accept=accept,
                contentType=content_type
            )
            
            # Parse the response body to get the embedding
            response_body = json.loads(response['body'].read())
            embedding = response_body.get('embedding')
            
            if embedding:
                # Convert embedding to numpy array for further use
                return np.array(embedding, dtype=np.float32)
            else:
                print("Embedding not found in the response.")
                return None
        except Exception as e:
            print(f"Error generating embedding: {e}")
            return None

    def dataframe_to_string(self, df):
        """
        Convert a pandas DataFrame to a string representation.
        
        Args:
        df (pd.DataFrame): The DataFrame to convert to a string.

        Returns:
        str: The string representation of the DataFrame.
        """
        # Example: Convert the schema (column names) to a string
        schema_string = ", ".join(df.columns)
        # Optionally, you can include the first few rows or other data if needed
        # schema_string += "\n" + df.head().to_string(index=False)
        return schema_string


    # Calculate cosine similarity between two embeddings
    def calculate_similarity(self, embedding1, embedding2):
        try:
            dot_product = np.dot(embedding1, embedding2)
            norm_embedding1 = np.linalg.norm(embedding1)
            norm_embedding2 = np.linalg.norm(embedding2)
            similarity = dot_product / (norm_embedding1 * norm_embedding2)
            return similarity
        except Exception as e:
            print(f"Error calculating similarity: {e}")
            return None

    # CHATBOT for knowledge-based agent, require configuration in Bedrock, model id already selected in Bedrock config
    def invoke_agent(self, agent_id, agent_alias_id, session_id, prompt):
        try:
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
                if "chunk" in event:
                    chunk = event["chunk"]
                    output_text += chunk["bytes"].decode()
                    if "attribution" in chunk:
                        citations = citations + chunk["attribution"]["citations"]
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

# Usage example
if __name__ == "__main__":
    chatbot = AWSBedrock()
    user_input = "Hello, how can you help me today?"
    response = chatbot.conversation(prompt=user_input)
    print(response)

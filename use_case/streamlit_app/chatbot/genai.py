import os
import boto3
from langchain.llms.bedrock import Bedrock
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain

class DemoChatbot:
    def __init__(self, model_id="anthropic.claude-v2:1", model_kwargs=None, max_token_limit=512):
        self.model_id = model_id
        self.model_kwargs = model_kwargs if model_kwargs is not None else {
            "temperature": 0.9,
            "top_p": 0.5,
            "max_tokens_to_sample": 512
        }
        self.max_token_limit = max_token_limit
        self.llm = self.init_llm()
        self.memory = self.init_memory()

    def init_llm(self):
        try:
            demo_llm = Bedrock(
                credentials_profile_name="default",
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

    def demo_conversation(self, input_text):
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
    chatbot = DemoChatbot()
    user_input = "Hello, how can you help me today?"
    response = chatbot.demo_conversation(input_text=user_input)
    print(response) 
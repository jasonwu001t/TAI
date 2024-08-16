import os
# from langchain_community.llms import Bedrock
from langchain_aws import BedrockLLM
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain

def demo_chatbot():
    demo_llm = BedrockLLM(
        # credentials_profile_name = 'default',
        model_id = 'anthropic.claude-instant-v1',
        model_kwargs={
            "temperature" : 0.9,
            "top_p": 0.5,
            "max_gen_len": 512
        })
    return demo_llm

def demo_memory():
    llm_data = demo_chatbot()
    memory = ConversationBufferMemory(llm=llm_data, max_token_limit =512)
    return memory

def demo_conversation(input_text, memory):
    llm_chain_data = demo_chatbot()
    llm_conversation=ConversationChain(llm=llm_chain_data, memory=memory, verbose=True)

    chat_reply = llm_conversation.predict(input=input_text)
    return chat_reply

demo_chatbot()
demo_memory()
demo_conversation("hdfasd", "dfas")
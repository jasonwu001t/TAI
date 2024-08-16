import streamlit as st
import uuid
from genai_v2 import AWSBedrock  # Adjusted import to match the new backend structure

# https://github.com/acwwat/amazon-bedrock-agent-test-ui

client = 'conversation' # agent, conversation,  function_handler
agent_id = ''
agent_alias_id = ''

ui_title = '🌷🌸🌹🌺🌻🌼⭐️ Agents for Amazon Bedrock Test UI' # Title chatbot
ui_icon = 'BEDROCK_AGENT_TEST_UI_ICON'
st.set_page_config(page_title=ui_title, page_icon=ui_icon, layout='wide')
st.title(ui_title)

def init_state():
    st.session_state.session_id = str(uuid.uuid4())
    st.session_state.citations = []
    st.session_state.trace = {}
    st.session_state.chat_history = []  # Ensuring chat_history is initialized

# Initialize chatbot instance & history if not already in session
if 'chatbot' not in st.session_state:
    st.session_state.chatbot = AWSBedrock()
if 'chat_history' not in st.session_state:
    init_state() # Ensuring chat_history is initialized

if st.button("Reset Session"): # Clear chat button positioned below the text input box
    init_state()

# Render chat history
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"]) # unsafe_allow_html=True

if prompt := st.chat_input('Say something'):
    st.session_state.chat_history.append({'role':'user','content':prompt})
    with st.chat_message('human'):
        st.markdown(prompt)

    # Select which agent client to use
    if client =='agent':  # or parse from prompt then select the right client,eg . if agent in prompt
        chat_response = st.session_state.chatbot.invoke_agent(agent_id, 
                                                              agent_alias_id,
                                                              st.session_state.session_id,
                                                              prompt)
    if client =='conversation': 
        chat_response = st.session_state.chatbot.conversation(prompt)
    if client =='function_handler':
        chat_response = st.session_state.chatbot.generate_text(prompt)
    
    # Display the chat response
    with st.chat_message('ai'):
        st.markdown(chat_response["response"])

    st.session_state.chat_history.append({"role": "ai", "content": chat_response["response"]})
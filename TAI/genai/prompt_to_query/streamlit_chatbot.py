import streamlit as st
import uuid
from text_to_sql_agent import TextToSQLAgent

# Initialize the TextToSQLAgent
agent = TextToSQLAgent()

# UI Configuration
ui_title = '🌷🌸🌹🌺🌻🌼⭐️ SQL Querying Chatbot'
ui_icon = '💬'
st.set_page_config(page_title=ui_title, page_icon=ui_icon, layout='wide')
st.title(ui_title)

def init_state():
    st.session_state.session_id = str(uuid.uuid4())
    st.session_state.chat_history = []  # Ensuring chat_history is initialized

# Initialize session state variables if not already done
if 'chat_history' not in st.session_state:
    init_state()

if st.button("Reset Session"):  # Clear chat button positioned below the text input box
    init_state()

# Render chat history
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input('Ask me anything about the database...'):
    st.session_state.chat_history.append({'role': 'user', 'content': prompt})
    with st.chat_message('human'):
        st.markdown(prompt)

    # Combine all previous chat messages into a single context
    conversation_context = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state.chat_history])
    
    # Process the user prompt with TextToSQLAgent including conversation context
    result = agent.process_prompt(f"{conversation_context}\nuser: {prompt}")

    # Display the chat response
    with st.chat_message('ai'):
        st.markdown(result)

    st.session_state.chat_history.append({"role": "ai", "content": result})

import streamlit as st
from genai import DemoChatbot  # Adjusted import to match the new backend structure

# Initialize chatbot instance & history if not already in session
if 'chatbot' not in st.session_state:
    st.session_state.chatbot = DemoChatbot()
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []  # Ensuring chat_history is initialized

# Title chatbot
st.title("⭐️ Efficient Chatbot")  # Title with an emoji

# Render chat history
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["text"])

# Input text box for chatbot
input_text = st.chat_input("Powered by Claude 2")
if input_text:
    with st.chat_message("user"):
        st.markdown(input_text)

    # Append user input to chat history
    st.session_state.chat_history.append({"role": "user", "text": input_text})

    # Generate chat response using the chatbot instance
    chat_response = st.session_state.chatbot.demo_conversation(input_text)

    # Display the chat response
    with st.chat_message("assistant"):
        st.markdown(chat_response["response"])

    # Append assistant's response to chat history
    st.session_state.chat_history.append({"role": "assistant", "text": chat_response["response"]})
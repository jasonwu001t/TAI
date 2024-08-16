import streamlit as st
from genai_v2 import AWSBedrock  # Adjusted import to match the new backend structure

# Initialize chatbot instance & history if not already in session
if 'chatbot' not in st.session_state:
    st.session_state.chatbot = AWSBedrock()
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []  # Ensuring chat_history is initialized

# Title chatbot
st.title("🌷🌸🌹🌺🌻🌼⭐️ Utilization Chatbot")  # Title with an emoji

# Render chat history
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["text"])

# Input text box for chatbot
input_text = st.chat_input("Say something")
if input_text:
    with st.chat_message("human"):
        st.markdown(input_text)

    # Append user input to chat history
    st.session_state.chat_history.append({"role": "human", "text": input_text})

    # Generate chat response using the chatbot instance
    chat_response = st.session_state.chatbot.conversation(input_text)

    # Display the chat response
    with st.chat_message("ai"):
        st.markdown(chat_response["response"])

    # Append assistant's response to chat history
    st.session_state.chat_history.append({"role": "ai", "text": chat_response["response"]})

# Clear chat button positioned below the text input box
if st.button("Clear Chat"):
    st.session_state.chat_history = []  # Clear the chat history
    st.session_state.chatbot.conversation_history = []  # Clear the chatbot's conversation history

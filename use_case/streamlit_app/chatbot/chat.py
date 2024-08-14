import streamlit as st
import asyncio
from genai import AWSBedrock  # Assuming your AWSBedrock class is in a file named genai.py

# Function to get or create the event loop safely
def get_or_create_event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # No running loop, let's create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop

# Initialize the event loop at the start
get_or_create_event_loop()

# Initialize the AWSBedrock class
if 'aws_bedrock' not in st.session_state:
    st.session_state.aws_bedrock = AWSBedrock()

# Streamlit app title
st.title("Conversational Chatbot")

# Select a model ID from active models
model_id =  "anthropic.claude-instant-v1"
# st.selectbox("Select a Model", 
#                         list(st.session_state.aws_bedrock.get_active_models().keys()))

# User input
user_input = st.text_input("You: ", "")

# Button to submit the input
if st.button("Send"):
    if user_input:
        response = st.session_state.aws_bedrock.generate_conversational_response(model_id, user_input)
        st.session_state.conversation.append(f"You: {user_input}")
        st.session_state.conversation.append(f"Bot: {response}")
        user_input = ""  # Clear input after submission

# Button to clear chat history
if st.button("Clear Chat History"):
    st.session_state.aws_bedrock.conversation_history.clear()
    st.session_state.conversation = []

# Display the conversation
if 'conversation' not in st.session_state:
    st.session_state.conversation = []

for message in st.session_state.conversation:
    st.write(message)

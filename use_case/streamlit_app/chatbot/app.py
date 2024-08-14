import streamlit as st
import boto3

# Initialize the AWS Bedrock client
bedrock_client = boto3.client('bedrock-runtime')

# Function to send a message to AWS Bedrock
def send_message_to_bedrock(message):
    try:
        # Add user message to the conversation history
        st.session_state.conversation.append(f"You: {message}")

        # Combine the conversation history into a single string
        conversation_history = "\n".join(st.session_state.conversation)

        # Send the conversation history to the Bedrock model
        response = bedrock_client.invoke_model(
            modelId='your-model-id',  # Replace with your specific model ID
            body=conversation_history,
            accept='application/json',
            contentType='application/json'
        )

        # Assuming the response returns a single message from the bot
        bot_response = response['body']  # Adjust based on the response format

        # Add bot response to the conversation history
        st.session_state.conversation.append(f"Bot: {bot_response}")

        return bot_response
    except Exception as e:
        return f"An error occurred: {str(e)}"

# Function to display the chatbot interface
def chatbot_interface():
    st.title("Conversational Chatbot Interface")
    
    # Input for user message
    user_input = st.text_input("You:", "")
    
    # Button to send the message
    if st.button("Send"):
        if user_input:
            response = send_message_to_bedrock(user_input)
            st.write(f"Bot: {response}")
        else:
            st.write("Please enter a message.")

    # Display the entire conversation history
    if st.session_state.conversation:
        st.write("### Conversation History")
        for line in st.session_state.conversation:
            st.write(line)

    # Button to clear the chat history
    if st.button("Clear Chat History"):
        st.session_state.conversation = []
        st.write("Chat history cleared.")

# Main execution
if __name__ == "__main__":
    # Initialize conversation history in session state
    if 'conversation' not in st.session_state:
        st.session_state.conversation = []

    # Display the chatbot interface
    chatbot_interface()

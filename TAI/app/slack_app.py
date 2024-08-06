import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import pandas as pd

class SlackApp:
    def __init__(self, bot_token, app_token):
        self.app = App(token=bot_token)
        self.app_token = app_token
        self.register_events()

    def register_events(self):
        # Listens to incoming messages that mention the bot
        @self.app.event("app_mention")
        def handle_app_mention_events(body, say):
            event = body["event"]
            user_id = event["user"]
            message_text = event["text"]
            
            # Print the message text
            print(f"Received message: {message_text}")

            # Save the message to a variable
            user_message = message_text
            
            # Process the message
            processed_message = self.process_message(user_message)
            
            # Respond to the mention with the processed message
            say(f"Hello <@{user_id}>, here's your processed message: {processed_message}")

    def process_message(self, message):
        # This function processes the user's message and returns a new output
        # For example, let's reverse the message as a simple processing step
        return message[::-1]

    def start(self):
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()

if __name__ == "__main__":
    # Set your environment variables or replace them with your actual tokens
    bot_token = os.environ['SLACK_BOT_TOKEN']
    app_token = os.environ['SLACK_APP_TOKEN']

    slack_app_handler = SlackApp(bot_token, app_token)
    slack_app_handler.start()

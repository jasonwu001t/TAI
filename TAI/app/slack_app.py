import os
import pandas as pd
import plotly.express as px
from io import BytesIO
import plotly.io as pio
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
#https://www.youtube.com/watch?v=Luujq0t0J7A

"""
App registration step by step
1. go to api.slack.com/apps to create new app
2. click app in slackchat window to add the add
3. api.slack.com go to OAuth & Permissions -> Scopes -> add scope (chat:write, app_mention) 
    -> install to workspace -> Bot User OAuth Tokens for your workspace will be generated, copy that
4. api.slack.com go to Basic Information -> App-Level Tokens Generate Tokens (add scope : connections:write) -> save generated App token
5. api.slack.com go to Socket Mode -> Turn on Enable Socket Mode
6. api.slack.com go to Interactive & Shortcuts -> Turn on Interactivity
7. api.slack.com go to Event Subscriptions -> Turn on Enable Events -> and in Subscribe to bot events section (add bot user event : app_mention, )
    -> Save Changes -> Reinstall the App to workspace
8. api.slack.com go to App Home -> Turn on Messages Ta0. 
9. add the App to the channel 
10. if the app should be only allowed by channel, create member_joined_channel event for the app
https://forums.slackcommunity.com/s/question/0D53a00008QI1eiCAD/is-there-a-way-to-allow-the-slack-app-to-be-only-added-to-specific-channels?language=en_US

"""

class SlackAppHandler:
    def __init__(self, bot_token, app_token):
        self.app = App(token=bot_token)
        self.client = WebClient(token=bot_token)
        self.app_token = app_token
        self.register_events()

    def register_events(self):
        @self.app.event("app_mention")
        def handle_app_mention_events(body, say):
            event = body["event"]
            user_id = event["user"]
            message_text = event["text"]

            # Remove the mention part of the message
            mention = f"<@{self.app.client.auth_test()['user_id']}>"
            user_message = message_text.replace(mention, "").strip()
            
            print(f"Received message: {user_message}")

            # Process the message
            processed_message = self.process_message(user_message)
            
            # Respond to the mention with the processed message
            if isinstance(processed_message, pd.DataFrame):
                # Convert DataFrame to an image and upload to Slack
                buffer = self.dataframe_to_image(processed_message)
                self.upload_file(buffer, 'dataframe.png', event["channel"])
                say(f"Hello <@{user_id}>, here's your processed DataFrame:")
            elif isinstance(processed_message, pio.BaseFigure):
                # Convert Plotly figure to an image and upload to Slack
                buffer = self.figure_to_image(processed_message)
                self.upload_file(buffer, 'plot.png', event["channel"])
                say(f"Hello <@{user_id}>, here's your processed plot:")
            else:
                say(f"Hello <@{user_id}>, here's your processed message: {processed_message}")

    def process_message(self, message):
        # This function processes the user's message and returns a new output
        if "dataframe" in message.lower():
            # Create a sample dataframe
            data = {'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']}
            df = pd.DataFrame(data)
            return df
        elif "plot" in message.lower():
            # Create a sample plotly plot
            fig = px.line(x=[1, 2, 3], y=[1, 4, 9], title="Sample Plot")
            return fig
        else:
            return message[::-1]

    def dataframe_to_image(self, df):
        # Convert DataFrame to an image
        fig = self.create_dataframe_plot(df)
        return self.figure_to_image(fig)

    def create_dataframe_plot(self, df):
        # Create a Plotly figure from the DataFrame
        fig = px.imshow(df, text_auto=True, aspect="auto", title="DataFrame")
        return fig

    def figure_to_image(self, fig):
        # Convert Plotly figure to a PNG image in a BytesIO object
        buffer = BytesIO()
        fig.write_image(buffer, format='png')
        buffer.seek(0)
        return buffer

    def upload_file(self, file_buffer, filename, channel):
        # Upload the file to Slack
        self.client.files_upload(
            channels=channel,
            file=file_buffer,
            filename=filename,
            filetype='png',
            title=filename
        )

    def start(self):
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()

if __name__ == "__main__":
    # Set your environment variables or replace them with your actual tokens
    bot_token = os.environ['SLACK_BOT_TOKEN']
    app_token = os.environ['SLACK_APP_TOKEN']

    slack_app_handler = SlackAppHandler(bot_token, app_token)
    slack_app_handler.start()

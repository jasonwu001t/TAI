import requests
import json

class SlackApp:
    def __init__(self, webhook_url, bot_token, channel_id):
        self.webhook_url = webhook_url
        self.bot_token = bot_token
        self.channel_id = channel_id

    def send_message(self, message):
        """
        Send a message to a Slack channel using the Webhook URL.

        :param message: The message to send as a string.
        :return: Response from the Slack API.
        """
        headers = {'Content-Type': 'application/json'}
        data = json.dumps({'text': message})
        response = requests.post(self.webhook_url, headers=headers, data=data)
        return response.json()

    def read_messages(self, count=10):
        """
        Read messages from a Slack channel using the Slack API.

        :param count: The number of messages to retrieve.
        :return: List of messages from the Slack channel.
        """
        url = 'https://slack.com/api/conversations.history'
        headers = {'Authorization': f'Bearer {self.bot_token}'}
        params = {
            'channel': self.channel_id,
            'limit': count
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json().get('messages', [])
        else:
            response.raise_for_status()

if __name__ == '__main__':
    # User-defined values
    webhook_url = 'your_webhook_url_here'
    bot_token = 'your_bot_token_here'
    channel_id = 'your_channel_id_here'

    slack_handler = SlackApp(webhook_url, bot_token, channel_id)

    # Test sending a message
    message = "Hello, this is a test message from the GTI library!"
    send_response = slack_handler.send_message(message)
    print("Send message response:", send_response)

    # Test reading messages
    read_response = slack_handler.read_messages(count=5)
    print("Read messages response:", read_response)

import slack
from sapgui_flow.secrets.get_token import get_secret
from sapgui_flow.resources.config import SLACK_TOKEN


def slackAlerta(msg):
    """
    Sends an alert message to a Slack channel.

    Args:
        msg (str): The message to be sent.

    Raises:
        Exception: If there is an error sending the Slack message.
    """
    token = (get_secret(SLACK_TOKEN))
    client = slack.WebClient(token=token)
    client.chat_postMessage(channel='alertas_engenharia',text=msg)
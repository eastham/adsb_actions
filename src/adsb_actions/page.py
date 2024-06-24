#!/usr/bin/python
"""Paging/webhook support."""

import sys
import logging
import json
import requests
from .config import Config
from .adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

CONFIG = Config()

SEND_SLACK = False
SEND_PAGE = True

# TODO need to clean up the webhook actions in rules.py to
# use these.

def send_slack(text):
    print(f"Slack msg: {text}")
    if SEND_SLACK:
        webhook = CONFIG.private_vars['slack_nearby_webhook']  # specifies channel to send to
        payload = {"text": text}   # human-readable short message
        response = requests.post(webhook, json.dumps(payload))
        print(response)
    else:
        print("Skipping slack send")

def send_page(recipient: str, msg: str):
    """Send a page using a JSON payload over HTTPS.

    Args:
        recipient (str): The recipient of the page.
        msg (str): The message to send.

    Returns:
        bool: True if the page was sent successfully, False otherwise.
    """
    if not SEND_PAGE:
        print("Skipping page send")
        return False

    # lookup the recipient's id from the config
    try:
        recipient_id = CONFIG.private_vars['page_recipients'][recipient.lower()]
    except KeyError:
        print(f"Failed: Recipient \"{recipient}\" not found in config")
        return False

    body = {
    "username": CONFIG.private_vars['page_user'],
    "password": CONFIG.private_vars['page_pw'],
    "sendpage": {
        "recipients": {
            "people":
                [str(recipient_id)]
        },
        "message":
            msg
    }}

    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    response = requests.Session().post(CONFIG.private_vars['page_url'],
                                       json = body)
    success = "success" in response.json().get('status')

    print("Page success: " + str(success))
    print("Page response: " + response.text)
    return success

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: page.py <recipient> <message ...>")
        print("Example: page.py deputy The network is down!")
        sys.exit(1)

    ret = send_page(sys.argv[1], " ".join(sys.argv[2:]))

    sys.exit(0 if ret else 1)

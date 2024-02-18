"""Paging/webhook support."""

import logging
import requests
import sys
from config import Config

CONFIG = Config()

SEND_SLACK = False
SEND_PAGE = True

# TODO make this more generic -- the "webhook" action ideally could
# cover the functionality of either of these methods.
# maybe change the format of the webhook action to be something like:
#   webhook: [ payload_filename ]
# ...and then a way to regexp-replace the body text into the payload?
#
# (ideally the file would be read in one time and cached for efficiency.)

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
    """Send a page with a JSON payload.

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
        print(f"Recipient {recipient} not found in config")
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
                                       json=body)
    success = "success" in response.json().get('status')

    print("Page success: " + str(success))
    print("Page response: " + response.text)
    return success

if __name__ == "__main__":
    # first arg: recipient, remaining args: message
    ret = send_page(sys.argv[1], " ".join(sys.argv[2:]))

    sys.exit(0 if ret else 1)

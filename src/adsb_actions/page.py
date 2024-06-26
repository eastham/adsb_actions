#!/usr/bin/python
"""Paging/webhook support."""

import sys
import logging
import json
import requests
from adsb_actions.adsb_logger import Logger
from adsb_actions.config import Config

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

CONFIG = Config()

SEND_SLACK = True
SEND_PAGE = True

def send_slack(channel: str, text: str):
    """Send a message to a slack channel.

    Args:
        channel (str): The name of the private config var identifying the
                       slack channel to send to.
        text (str): The message to send.
    """
    if not SEND_SLACK:
        logger.warning("Skipping slack send")
        return

    logger.info(f"Sending slack msg to channel {channel}: {text}")
    try:
        webhook = CONFIG.private_vars[channel]
    except:     # pylint: disable=bare-except
        logger.error(f"Failed: Channel \"{channel}\" not found in config")
        return

    payload = {"text": text}
    response = requests.post(webhook, json.dumps(payload), timeout=10)
    logger.debug(f"Slack response: {response}")


def send_page(recipients: str, msg: str):
    """Send a page using a JSON payload over HTTPS.

    Args:
        recipients (str): The recipients of the page, space-delimited
        msg (str): The message to send.

    Returns:
        bool: True if the page was sent successfully, False otherwise.
    """

    assert recipients != ""
    result = True
    for recipient in recipients.split():
        if not send_one_page(recipient, msg):
            result = False
    return result

def send_one_page(recipient: str, msg: str):
    """Send a page to a single recipient."""

    if not SEND_PAGE:
        logger.warning("Skipping page send")
        return False

    # lookup the recipient's id from the config
    try:
        recipient_id = CONFIG.private_vars['page_recipients'][recipient.lower()]
    except KeyError:
        logger.error(f"Failed: Recipient \"{recipient}\" not found in config")
        return False

    logger.info(f"Sending page to {recipient_id}: {msg}")
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

    response = requests.Session().post(CONFIG.private_vars['page_url'],
                                       json = body)

    success = "success" in response.json().get('status')
    logger.info("Page success: %s, response: %s", success, response.text)
    return success

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: page.py <recipient> <message ...>")
        print("Example: page.py deputy The network is down!")
        sys.exit(1)

    ret = send_one_page(sys.argv[1], " ".join(sys.argv[2:]))

    sys.exit(0 if ret else 1)

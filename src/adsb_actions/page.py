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

def send_slack(channel: str, text: str) -> bool:
    """Send a message to a slack channel.

    Args:
        channel (str): The name of the private config var identifying the
                       slack channel to send to.
        text (str): The message to send.

    Returns:
        bool: True if message was sent successfully, False otherwise.
    """
    if not SEND_SLACK:
        logger.warning("Skipping slack send of: " + text)
        return False

    if not CONFIG.private_vars:
        logger.debug("Slack skipped - no private.yaml configured")
        return False

    if channel not in CONFIG.private_vars:
        logger.warning(f"Slack channel '{channel}' not found in private.yaml")
        return False

    logger.info(f"Sending slack msg to channel {channel}: {text}")
    try:
        webhook = CONFIG.private_vars[channel]
        payload = {"text": text}
        response = requests.post(webhook, json.dumps(payload), timeout=10)
        logger.debug(f"Slack response: {response}")
        return response.status_code == 200
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Slack send failed: {e}")
        return False


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

def send_one_page(recipient: str, msg: str) -> bool:
    """Send a page to a single recipient.

    Returns:
        bool: True if page was sent successfully, False otherwise.
    """
    if not SEND_PAGE:
        logger.warning("Skipping page send")
        return False

    if not CONFIG.private_vars:
        logger.debug("Paging skipped - no private.yaml configured")
        return False

    # Check for required paging configuration
    required_keys = ['page_recipients', 'page_user', 'page_pw', 'page_url']
    for key in required_keys:
        if key not in CONFIG.private_vars:
            logger.warning(f"Paging not configured - '{key}' missing from private.yaml")
            return False

    # lookup the recipient's id from the config
    try:
        recipient_id = CONFIG.private_vars['page_recipients'][recipient.lower()]
    except KeyError:
        logger.warning(f"Recipient '{recipient}' not found in page_recipients config")
        return False

    logger.info(f"Sending page to {recipient_id}: {msg}")
    try:
        body = {
            "username": CONFIG.private_vars['page_user'],
            "password": CONFIG.private_vars['page_pw'],
            "sendpage": {
                "recipients": {
                    "people": [str(recipient_id)]
                },
                "message": msg
            }
        }

        response = requests.Session().post(CONFIG.private_vars['page_url'],
                                           json=body, timeout=30)

        success = "success" in response.json().get('status', '')
        logger.info("Page success: %s, response: %s", success, response.text)
        return success
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Page send failed: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: page.py <recipients-comma-separated> <message ...>")
        print("Example: page.py deputy The network is down!")
        sys.exit(1)

    retval = 0
    for recip in sys.argv[1].split(','):
        if recip not in CONFIG.private_vars['page_recipients']:
            print(f"Recipient {recip} not found in config")
        else:
            success = send_one_page(recip, " ".join(sys.argv[2:]))
            if success:
                print(f"Sent page to {recip}")
            else:
                print(f"Failed to send page to {recip}")
            retval += not success

    sys.exit(1 if retval else 0)

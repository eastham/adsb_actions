import logging
import requests
import playapage
from config import Config
CONFIG = Config()

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

def send_page(msg):
    body = {
    "username": CONFIG.private_vars['page_user'],
    "password": CONFIG.private_vars['page_pw'],
    "sendpage": {
        "recipients": {
            "people":
                ["488"]
        },
        "message":
            "Test message"
    }}

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    response = requests.Session().post(CONFIG.private_vars['page_url'], json=body)
    print(response)
    print(response.text)
    return response.text

if __name__ == "__main__":
    send_page("test")

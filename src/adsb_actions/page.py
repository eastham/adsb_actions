import logging
import requests
from config import Config
CONFIG = Config()

def send_page():
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
    print(body)
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    response = requests.post(CONFIG.private_vars['page_url'], json=body)
    print(response)
    print(response.text)
    return response.text

if __name__ == "__main__":
    send_page()

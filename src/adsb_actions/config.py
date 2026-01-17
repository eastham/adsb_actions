"""Read/store local configuration yamls.

These yamls specify user-specific constants such as credentials and
preferences, not rules.  Rule yamls are usually specified on the
command line or an explicit API variable."""

import os
import yaml
from .util import safe_path

# Config paths can be overridden via environment variables
PRIVPATH = os.environ.get('ADSB_PRIVATE_PATH', safe_path("../../private.yaml"))

class Config:
    def __init__(self):

        self.private_vars = {}
        try:
            with open(PRIVPATH, "r", encoding="utf-8") as f:
                self.private_vars = yaml.safe_load(f)
        except Exception as e:  # pylint: disable=broad-except
            print("Note: private.yaml not found (optional - only needed for "
                  "Slack/paging/database integrations): " + str(e))

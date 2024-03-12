"""Read/store local configuration yamls.  

These yamls specifcy user-specific constants such as credentials and 
preferences, not rules.  Rule yamls are usually specific on the 
command line or an explicit API variable."""

import yaml
import util

# don't rely on the cwd to find the config files
CONFIGPATH = util.safe_path("../../config.yaml")
PRIVPATH = util.safe_path("../../private.yaml")

class Config:
    def __init__(self):
        with open(CONFIGPATH, "r") as f:
            self.vars = yaml.safe_load(f)

        self.private_vars = {}
        try:
            with open(PRIVPATH, "r") as f:
                self.private_vars = yaml.safe_load(f)
        except Exception as e:
            print("No private.yaml found, or parse fail: " +str(e))

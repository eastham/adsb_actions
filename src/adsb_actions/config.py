"""Read/store local configuration yamls."""
import yaml

CONFIGPATH = "config.yaml"
PRIVPATH = "private.yaml"

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

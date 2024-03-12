import os

def safe_path(relative_path):
    """Return an absolute path to a file in the same directory as this module.
    Removes dependency on the current working directory."""

    return os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        relative_path))

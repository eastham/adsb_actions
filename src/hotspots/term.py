"""Tiny terminal display helpers for the v2 CLI / status output.

Raw ANSI color (matching src/adsb_actions/rules.py), auto-disabled when stdout is
not a TTY (pipes, redirects, pytest capture) so logs and test assertions stay
plain. Plus a path-shortening helper so output shows WHERE artifacts land.
"""

import sys
from pathlib import Path

_USE_COLOR = sys.stdout.isatty()
_BLUE, _GREEN, _RED, _YELLOW, _RESET = (
    ("\033[94m", "\033[92m", "\033[91m", "\033[93m", "\033[0m")
    if _USE_COLOR else ("", "", "", "", "")
)

CHECK, CROSS, WARN, ARROW = ("✓", "✗", "⚠", "→")


def stage(msg: str) -> str:
    """Blue stage header."""
    return f"{_BLUE}{msg}{_RESET}"


def ok(msg: str) -> str:
    """Green text with a leading checkmark."""
    return f"{_GREEN}{CHECK} {msg}{_RESET}"


def fail(msg: str) -> str:
    """Red text with a leading cross."""
    return f"{_RED}{CROSS} {msg}{_RESET}"


def warn(msg: str) -> str:
    """Yellow text with a leading warning sign."""
    return f"{_YELLOW}{WARN} {msg}{_RESET}"


def rel(path) -> str:
    """Path relative to the current directory when it's under cwd, else absolute.
    Lets dry-run / status output show WHERE outputs land without dumping long
    absolute paths for in-project artifacts.

    Does NOT resolve symlinks: `data/` is a symlink to the network mount, and we
    want to show 'data/v2/...' (the in-project relative path), not the resolved
    mount location it points at."""
    p = Path(path)
    try:
        return str(p.relative_to(Path.cwd()))
    except ValueError:
        return str(p)

"""Tiny terminal-color helpers for the v2 CLI / status output.

Raw ANSI (matching src/adsb_actions/rules.py), auto-disabled when stdout is not a
TTY (pipes, redirects, pytest capture) so logs and test assertions stay plain.
"""

import sys

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

"""Tests for case/dash-insensitive aircraft_list matching and its caching.

Covers the pure normalization helpers, the standalone flight_id_in_list(),
and the cached Rules._flight_id_in_named_list() path.
"""

import pytest

from adsb_actions.rules import (
    Rules,
    _normalize_flight_id,
    flight_id_in_list,
)


# --- _normalize_flight_id -------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("N123AB", "N123AB"),
    ("n123ab", "N123AB"),        # case folded
    ("N123-AB", "N123AB"),       # single dash stripped
    ("n-1-2-3", "N123"),         # multiple dashes stripped
    ("  n123ab  ", "N123AB"),    # surrounding whitespace trimmed
    ("", ""),
    (None, ""),
])
def test_normalize(raw, expected):
    assert _normalize_flight_id(raw) == expected


def test_normalize_is_cached():
    # Distinct object, equal value -> should be an lru_cache hit.
    _normalize_flight_id.cache_clear()
    _normalize_flight_id("N999ZZ")
    before = _normalize_flight_id.cache_info()
    _normalize_flight_id("N999" + "ZZ")  # equal string, not interned identity
    after = _normalize_flight_id.cache_info()
    assert after.hits == before.hits + 1


# --- flight_id_in_list (standalone) --------------------------------------

AC_LIST = ["N123AB", "n456-cd", "N-789-EF"]

@pytest.mark.parametrize("flight_id,expected", [
    ("N123AB", True),
    ("n123ab", True),            # case-insensitive vs list
    ("n123-ab", True),           # dash-insensitive vs list
    ("N456CD", True),            # list entry itself has case+dash
    ("N789EF", True),
    ("N000XX", False),
    ("", False),
    (None, False),
])
def test_flight_id_in_list(flight_id, expected):
    assert flight_id_in_list(flight_id, AC_LIST) is expected


# --- Rules cached membership ---------------------------------------------

def _rules_with_lists():
    return Rules({
        "aircraft_lists": {
            "watched": ["N123AB", "n456-cd"],
        },
        "rules": {},
    })


def test_named_list_match_normalization():
    r = _rules_with_lists()
    assert r._flight_id_in_named_list("n123-ab", "watched") is True
    assert r._flight_id_in_named_list("N456CD", "watched") is True
    assert r._flight_id_in_named_list("N000XX", "watched") is False
    assert r._flight_id_in_named_list(None, "watched") is False


def test_named_list_unknown_list_returns_none():
    r = _rules_with_lists()
    assert r._normalized_ac_list("does_not_exist") is None
    # membership test on an unknown list is simply False, not an error
    assert r._flight_id_in_named_list("N123AB", "does_not_exist") is False


def test_named_list_is_cached_and_reused():
    r = _rules_with_lists()
    first = r._normalized_ac_list("watched")
    second = r._normalized_ac_list("watched")
    assert first is second  # same cached set object
    assert first == {"N123AB", "N456CD"}


def test_named_list_drops_empty_entries():
    r = Rules({
        "aircraft_lists": {"L": ["N1", "", "  ", None]},
        "rules": {},
    })
    assert r._normalized_ac_list("L") == {"N1"}

"""Provenance tagging for v2 pipeline outputs.

Records WHICH code version produced each per-cell output so that a regional map
assembled from many days/cells can be checked for algorithm-version mixing (the
case where some cells were computed by an old detector and some by a new one).

Storage: one manifest per date directory, keyed by cell, e.g.

    data/v2/events/20250501/_provenance.json
    {
      "36_-122": {"git_sha": "bbb", "git_dirty": false,
                  "config_hash": "c2", "written_utc": "2026-06-12T18:30:00Z"},
      "36_-116": {"git_sha": "aaa", ...}, ...
    }

Why per-cell-keyed and not one SHA per directory: a smaller region re-run (e.g.
WVI) writes its cells into the *same* date directory a prior CONUS run filled.
A single directory SHA would be stamped by the re-run while the other cells were
still produced by the old code. So we merge per-cell entries instead of
overwriting the whole manifest.

All git lookups degrade gracefully (git_sha=None) and never raise into a run.
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path

MANIFEST_NAME = "_provenance.json"

# Repo root for git invocations (this file lives at src/hotspots/provenance.py).
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _git(*args: str) -> str | None:
    """Run a git command in the repo, returning stripped stdout or None on any
    failure (not a repo, git missing, detached, etc.). Never raises."""
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=_REPO_ROOT,
            capture_output=True, text=True, timeout=10,
        )
        if out.returncode != 0:
            return None
        return out.stdout.strip()
    except Exception:
        return None


def git_sha() -> str | None:
    """Current short HEAD SHA, or None if git is unavailable. Public so status
    can compare on-disk provenance against HEAD."""
    return _git("rev-parse", "--short", "HEAD")


def git_dirty() -> bool | None:
    """True if the working tree has uncommitted changes. None if git unavailable.

    A bare SHA lies when you ran with local edits (common in research), so we
    record dirtiness explicitly. Public so the traffic-tile tool can record it."""
    porcelain = _git("status", "--porcelain")
    if porcelain is None:
        return None
    return porcelain != ""


# Back-compat alias for internal callers.
_git_dirty = git_dirty


def config_hash(config) -> str:
    """Short stable hash of the run-affecting config values. Lets status flag
    'same SHA but different parameters' (e.g. an edited alt ceiling)."""
    payload = {
        "alt_ceiling_ft": config.alt_ceiling_ft,
        "data_root": str(config.data_root),
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return sha1(blob).hexdigest()[:8]


def current_provenance(config) -> dict:
    """Provenance record for an output written right now by the current code."""
    return {
        "git_sha": git_sha(),
        "git_dirty": _git_dirty(),
        "config_hash": config_hash(config),
        "written_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def read_provenance(date_dir) -> dict | None:
    """Return the cell-keyed manifest for a date dir, or None if untagged
    (pre-provenance outputs — backward compatible)."""
    p = Path(date_dir) / MANIFEST_NAME
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def write_provenance(directory, record: dict) -> None:
    """Write a flat `_provenance.json` into `directory` (atomically). Used for
    whole-artifact provenance like a traffic-tile build, where the record isn't
    keyed by cell. Overwrites any existing manifest."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    dest = directory / MANIFEST_NAME
    tmp = dest.with_suffix(".json.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, sort_keys=True, indent=2)
        os.replace(tmp, dest)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def merge_cell_provenance(date_dir, cell_records: dict) -> None:
    """Merge per-cell provenance into the date dir's manifest, then write atomically.

    `cell_records` maps cell_tag (e.g. '36_-122') -> provenance dict. Only the
    given cells are updated; existing entries for other cells are preserved.

    Call this ONCE per stage run after any worker pool has joined, so there is a
    single writer and no locking is needed.
    """
    date_dir = Path(date_dir)
    date_dir.mkdir(parents=True, exist_ok=True)
    manifest = read_provenance(date_dir) or {}
    manifest.update(cell_records)

    dest = date_dir / MANIFEST_NAME
    tmp = dest.with_suffix(".json.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, sort_keys=True, indent=0)
        os.replace(tmp, dest)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise

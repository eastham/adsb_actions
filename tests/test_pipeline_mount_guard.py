"""Tests for the pre-run network-mount guard in cli.py.

Regression cover for the failure that motivated it: when the SMB share drops,
its mountpoint reverts to an ordinary *writable* directory, so a full run
completes "successfully" with every byte landing on local disk. The day-at-a-time
gate can't see this (it counts cells, not destination), so the guard has to run
as a precondition, before any writes.

All fast — no mounts are touched; os.path.ismount is patched.
"""

from pathlib import Path

import pytest

from hotspots import cli


class FakeConfig:
    """Minimal stand-in for the bits of Config the guard reads."""

    remount_cmd = "mount_smbfs //pi@raspi5/data ~/raspi5-data"

    def __init__(self, data_root, conus_dir):
        self.data_root = Path(data_root)
        self.conus_dir = Path(conus_dir)


@pytest.fixture
def mount(monkeypatch):
    """Control what os.path.ismount() reports for the network mountpoint."""
    real = Path(cli.NETWORK_MOUNT).expanduser()

    def set_mounted(is_mounted):
        monkeypatch.setattr(cli.os.path, "ismount",
                            lambda p: is_mounted and Path(p) == real)
    return set_mounted


# --- _mount_root: which paths are in scope ---------------------------------

@pytest.mark.parametrize("path", [
    "~/raspi5-data",
    "~/raspi5-data/v2",
    "~/raspi5-data/v2/grid/20260624",
])
def test_paths_under_share_are_guarded(path):
    assert cli._mount_root(Path(path).expanduser().resolve()) is not None


@pytest.mark.parametrize("path", [
    ".test_data/v2",        # smoke-test sandbox
    "data_local/v2_exp",    # experimental config
    "/tmp/scratch",
])
def test_local_disk_paths_are_not_guarded(path):
    assert cli._mount_root(Path(path).expanduser().resolve()) is None


def test_tilde_is_expanded_before_matching():
    """A literal ~ must not resolve to a bogus cwd-relative path and escape the
    check — that would silently disable the guard for tilde-style configs."""
    assert cli._mount_root(Path("~/raspi5-data/v2").expanduser().resolve())


# --- the guard itself ------------------------------------------------------

def test_aborts_when_share_is_not_mounted(mount):
    """The core case: path is under the share, share is down, dir is writable."""
    mount(False)
    with pytest.raises(SystemExit) as e:
        cli._assert_network_mounted(FakeConfig("~/raspi5-data/v2",
                                               "~/raspi5-data"))
    msg = str(e.value)
    assert "NOT mounted" in msg
    assert "mount_smbfs" in msg  # actionable recovery command


def test_passes_when_share_is_mounted(mount):
    mount(True)
    cli._assert_network_mounted(FakeConfig("~/raspi5-data/v2", "~/raspi5-data"))


def test_conus_dir_is_checked_too(mount):
    """Reads matter as well as writes — a missing source dir on an unmounted
    share yields an empty/short run rather than a loud failure."""
    mount(False)
    with pytest.raises(SystemExit, match="conus_dir"):
        cli._assert_network_mounted(FakeConfig(".test_data/v2", "~/raspi5-data"))


@pytest.mark.parametrize("data_root,conus_dir", [
    (".test_data/v2", ".test_data"),
    ("data_local/v2_exp", "data_local"),
])
def test_local_configs_run_with_share_down(mount, data_root, conus_dir):
    """test/exp sandboxes deliberately write to local disk; they must not be
    blocked just because the network share happens to be unmounted."""
    mount(False)
    cli._assert_network_mounted(FakeConfig(data_root, conus_dir))

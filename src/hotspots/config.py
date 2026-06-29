"""Loader for the v2 pipeline configuration (pipeline_config.yaml).

Single source of truth for the run/status CLI: paths, named regions, workflow
profiles, runtime knobs, and the default date window. See pipeline_config.yaml
for the annotated schema.

This module is also the single source of the v2 output-path constants
(GRID_DIR / EVENTS_DIR / REGIONAL_DIR / MAPS_DIR / ANIMATIONS_DIR). The stage
modules (stage2_shard, stage3_analyze, stage4_aggregate, stage5_visualize,
stage5b_trips) import them from here instead of declaring their own
`Path("data/v2")`. The root resolves from $ADSB_V2_DATA_ROOT, else
`paths.data_root` in the default YAML, else a `data/v2` fallback; see
set_data_root() for how the CLI redirects it (e.g. to a test sandbox).
"""

import os
from pathlib import Path

import yaml

_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parents[1]          # .../adsb_actions2
DEFAULT_CONFIG_PATH = _THIS_DIR / "pipeline_config.yaml"

# Region bounding boxes are integer-degree, inclusive min / exclusive max.
RegionBounds = tuple  # (lat_min, lat_max, lon_min, lon_max)


class Config:
    """Parsed pipeline_config.yaml with typed accessors.

    Paths are resolved relative to the project root so the CLI works from any
    cwd, matching how the legacy stage scripts assume cwd == project root.
    """

    def __init__(self, raw: dict, path: Path):
        self._raw = raw
        self.path = path
        self._validate()

    # -- raw sections -------------------------------------------------------
    @property
    def regions(self) -> dict:
        return self._raw["regions"]

    @property
    def profiles(self) -> dict:
        return self._raw["profiles"]

    @property
    def runtime(self) -> dict:
        return self._raw.get("runtime", {})

    @property
    def deploy(self) -> dict:
        return self._raw.get("deploy", {})

    @property
    def dates(self) -> dict:
        return self._raw.get("dates", {})

    # -- path accessors (absolute, project-root-relative) -------------------
    def _resolve(self, rel: str) -> Path:
        p = Path(rel)
        return p if p.is_absolute() else (_PROJECT_ROOT / p)

    @property
    def data_root(self) -> Path:
        return self._resolve(self._raw["paths"]["data_root"])

    @property
    def grid_dir(self) -> Path:
        return self.data_root / "grid"

    @property
    def events_dir(self) -> Path:
        return self.data_root / "events"

    @property
    def regional_dir(self) -> Path:
        return self.data_root / "regional"

    @property
    def maps_dir(self) -> Path:
        return self.data_root / "maps"

    @property
    def conus_dir(self) -> Path:
        return self._resolve(self._raw["paths"]["conus_dir"])

    @property
    def traffic_tiles_url(self) -> str | None:
        return self._raw["paths"].get("traffic_tiles_url")

    @property
    def traffic_tiles_local(self) -> str | None:
        return self._raw["paths"].get("traffic_tiles_local")

    # -- region / profile lookups -------------------------------------------
    def region_bounds(self, name: str) -> RegionBounds:
        """Return (lat_min, lat_max, lon_min, lon_max) for a named region."""
        if name not in self.regions:
            raise KeyError(
                f"unknown region '{name}'. Known: {sorted(self.regions)}")
        bb = self.regions[name]
        return (bb["lat_min"], bb["lat_max"], bb["lon_min"], bb["lon_max"])

    def regions_dict(self) -> dict:
        """Region table in stage4_aggregate's expected shape: name -> {lat_min,...}."""
        return dict(self.regions)

    def profile(self, name: str) -> dict:
        """Return a profile dict, e.g. {'stages': [5], 'pmtiles': True}."""
        if name not in self.profiles:
            raise KeyError(
                f"unknown profile '{name}'. Known: {sorted(self.profiles)}")
        return self.profiles[name]

    # -- runtime knobs (with sensible fallbacks) ----------------------------
    @property
    def workers(self) -> int:
        return int(self.runtime.get("workers", 1))

    @property
    def alt_ceiling_ft(self) -> int:
        return int(self.runtime.get("alt_ceiling_ft", 18000))

    @property
    def remount_cmd(self) -> str:
        return self.runtime.get("remount_cmd", "") or ""

    @property
    def retry_attempts(self) -> int:
        return int(self.runtime.get("retry_attempts", 1))

    @property
    def retry_pause_s(self) -> int:
        return int(self.runtime.get("retry_pause_s", 0))

    @property
    def default_start(self) -> str | None:
        return self.dates.get("default_start")

    @property
    def default_end(self) -> str | None:
        return self.dates.get("default_end")

    # -- validation ---------------------------------------------------------
    def _validate(self) -> None:
        for section in ("paths", "regions", "profiles"):
            if section not in self._raw:
                raise ValueError(f"{self.path}: missing required section '{section}'")
        if "data_root" not in self._raw["paths"]:
            raise ValueError(f"{self.path}: paths.data_root is required")

        for name, bb in self.regions.items():
            missing = {"lat_min", "lat_max", "lon_min", "lon_max"} - set(bb or {})
            if missing:
                raise ValueError(f"{self.path}: region '{name}' missing {sorted(missing)}")
            if bb["lat_min"] >= bb["lat_max"] or bb["lon_min"] >= bb["lon_max"]:
                raise ValueError(
                    f"{self.path}: region '{name}' has empty/inverted bounds {bb}")

        for name, prof in self.profiles.items():
            stages = (prof or {}).get("stages")
            if not stages:
                raise ValueError(f"{self.path}: profile '{name}' has no 'stages'")
            bad = set(stages) - {2, 3, 4, 5}
            if bad:
                raise ValueError(
                    f"{self.path}: profile '{name}' has invalid stages {sorted(bad)} "
                    f"(must be a subset of 2,3,4,5)")


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load and validate the pipeline config. Defaults to pipeline_config.yaml
    next to this module; override via the CLI's --config."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.exists():
        raise FileNotFoundError(f"pipeline config not found: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"{cfg_path}: top-level YAML must be a mapping")
    return Config(raw, cfg_path)


# ---------------------------------------------------------------------------
# Module-level path constants — the single source of truth the stage modules
# import (stage2_shard, stage3_analyze, stage4_aggregate, stage5_visualize,
# stage5b_trips) instead of each declaring its own Path("data/v2").
#
# The root resolves, in order of precedence:
#   1. $ADSB_V2_DATA_ROOT  (set by cli.py from --config; lets a test sandbox
#      redirect ALL stage reads/writes without editing any module)
#   2. paths.data_root in the default pipeline_config.yaml
#   3. "data/v2" literal fallback (if the config can't be read for any reason)
#
# Resolved at import time, so anything that sets the env var must do so BEFORE
# importing the stage modules (cli.py does this).
# ---------------------------------------------------------------------------

def _default_data_root() -> Path:
    env = os.environ.get("ADSB_V2_DATA_ROOT")
    if env:
        p = Path(env)
        return p if p.is_absolute() else (_PROJECT_ROOT / p)
    try:
        return load_config().data_root
    except Exception:
        return _PROJECT_ROOT / "data" / "v2"


DATA_ROOT = _default_data_root()
GRID_DIR = DATA_ROOT / "grid"
EVENTS_DIR = DATA_ROOT / "events"
REGIONAL_DIR = DATA_ROOT / "regional"
MAPS_DIR = DATA_ROOT / "maps"
ANIMATIONS_DIR = DATA_ROOT / "animations"


# Which of this module's path constants each stage module imported by name, so
# set_data_root() can refresh their (frozen) bindings too. `from hotspots.config
# import GRID_DIR` copies the value at import time; re-pointing the data root has
# to write the new value back into each module that already grabbed it.
_STAGE_MODULE_CONSTANTS = {
    "hotspots.stage2_shard": ["GRID_DIR"],
    "hotspots.stage3_analyze": ["GRID_DIR", "EVENTS_DIR", "ANIMATIONS_DIR"],
    "hotspots.stage4_aggregate": ["EVENTS_DIR", "REGIONAL_DIR"],
    "hotspots.stage5_visualize": ["MAPS_DIR"],
    "hotspots.stage5b_trips": ["V2_GRID"],  # imported as `GRID_DIR as V2_GRID`
    # pipeline.py re-imports several of these from the stage modules, so its
    # runners (run_stage4 etc.) hold their own frozen copies too.
    "hotspots.pipeline": ["GRID_DIR", "EVENTS_DIR", "REGIONAL_DIR", "MAPS_DIR"],
}
_STAGE_ALIASES = {"V2_GRID": "GRID_DIR"}


def set_data_root(root: str | os.PathLike) -> None:
    """Re-point the v2 data root: rebind this module's path constants, export
    $ADSB_V2_DATA_ROOT (so stage modules imported *later* pick it up), and refresh
    the bindings of any stage module already imported.

    cli.py calls this once after parsing --config, so a test sandbox (or any
    non-default data_root) redirects every stage's reads/writes — even across
    repeated in-process calls with different roots (as the tests do).
    """
    import sys as _sys

    global DATA_ROOT, GRID_DIR, EVENTS_DIR, REGIONAL_DIR, MAPS_DIR, ANIMATIONS_DIR
    p = Path(root)
    DATA_ROOT = p if p.is_absolute() else (_PROJECT_ROOT / p)
    os.environ["ADSB_V2_DATA_ROOT"] = str(DATA_ROOT)
    GRID_DIR = DATA_ROOT / "grid"
    EVENTS_DIR = DATA_ROOT / "events"
    REGIONAL_DIR = DATA_ROOT / "regional"
    MAPS_DIR = DATA_ROOT / "maps"
    ANIMATIONS_DIR = DATA_ROOT / "animations"

    here = _sys.modules[__name__]
    for mod_name, names in _STAGE_MODULE_CONSTANTS.items():
        mod = _sys.modules.get(mod_name)
        if mod is None:
            continue  # not imported yet — it'll read the fresh values on import
        for name in names:
            source = _STAGE_ALIASES.get(name, name)
            setattr(mod, name, getattr(here, source))

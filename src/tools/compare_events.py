#!/usr/bin/env python3
"""Compare v2 LOS events between two data roots — for stage-3 algorithm iteration.

Typical use: you re-ran stage 3 with an experimental detector into an isolated
root (see pipeline_config_exp.yaml) and want to know what changed vs. production,
without eyeballing two maps.

  python src/tools/compare_events.py --region wvi \\
      --start-date 20250601 --end-date 20250831 \\
      --exp data_local/v2_exp --prod data/v2

Events are matched on identity (flight1, flight2, rounded datetime) so the diff
is meaningful even when row order or minor float fields differ:
  - ADDED    : in exp, not in prod (the new algo now flags these)
  - REMOVED  : in prod, not in exp (the new algo no longer flags these)
  - CHANGED  : same event, but quality / lateral_nm / alt_sep_ft moved
  - SAME     : unchanged

Each ADDED/REMOVED/CHANGED entry prints a deep-link into the rendered map
(<base-url>/<exp>/maps/<region>_<start>_<end>.html?tail=<flight1>) so you can
click straight to that aircraft. Serve from the PROJECT ROOT first so the paths
resolve, e.g.:
    python src/hotspots/serve.py . 8080

Read-only; touches neither root.
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Region name -> (lat_min, lat_max, lon_min, lon_max). Falls back to the shared
# config if available; a tiny built-in map keeps this usable standalone.
_BUILTIN_REGIONS = {
    "wvi": (36, 37, -122, -121),
    "conus": (24, 50, -125, -65),
}


def _region_bounds(name: str):
    try:
        _root = Path(__file__).resolve().parents[1]
        if str(_root) not in sys.path:
            sys.path.insert(0, str(_root))
        from hotspots.config import load_config
        return load_config().region_bounds(name)
    except Exception:
        if name in _BUILTIN_REGIONS:
            return _BUILTIN_REGIONS[name]
        raise SystemExit(f"unknown region '{name}' (and config unavailable)")


def _date_tags(start: str, end: str):
    s = datetime.strptime(start, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()
    out, d = [], s
    while d <= e:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def _cells_in_box(bounds):
    lat_min, lat_max, lon_min, lon_max = bounds
    return [(lat, lon)
            for lat in range(lat_min, lat_max)
            for lon in range(lon_min, lon_max)]


def _load_events(root: Path, bounds, date_tags) -> pd.DataFrame:
    """Read every in-box cell parquet under <root>/events/<date>/ for the range."""
    frames = []
    for dt in date_tags:
        day_dir = root / "events" / dt
        if not day_dir.is_dir():
            continue
        for lat, lon in _cells_in_box(bounds):
            p = day_dir / f"{dt}_{lat}_{lon}.parquet"
            if p.exists():
                frames.append(pd.read_parquet(p))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _key(df: pd.DataFrame) -> pd.Series:
    """Stable per-event identity: the aircraft pair (order-independent) + the
    event time rounded to the minute. Tolerates row reordering and sub-minute
    jitter without collapsing genuinely distinct events."""
    pair = df.apply(
        lambda r: "|".join(sorted([str(r["flight1"]), str(r["flight2"])])), axis=1)
    minute = pd.to_datetime(df["datetime_utc"]).dt.strftime("%Y-%m-%dT%H:%M")
    return pair + "@" + minute


# Attributes we report drift on for matched events.
_COMPARE_COLS = ["quality", "lateral_nm", "alt_sep_ft", "duration_s"]


def compare(exp: pd.DataFrame, prod: pd.DataFrame) -> dict:
    if exp.empty and prod.empty:
        return {"added": [], "removed": [], "changed": [], "same": 0,
                "exp_n": 0, "prod_n": 0}
    exp = exp.copy()
    prod = prod.copy()
    exp["_k"] = _key(exp) if not exp.empty else pd.Series(dtype=str)
    prod["_k"] = _key(prod) if not prod.empty else pd.Series(dtype=str)
    # Drop duplicate keys (rare; keep first) so lookups are unambiguous.
    exp = exp.drop_duplicates("_k").set_index("_k")
    prod = prod.drop_duplicates("_k").set_index("_k")

    exp_keys, prod_keys = set(exp.index), set(prod.index)
    added = sorted(exp_keys - prod_keys)
    removed = sorted(prod_keys - exp_keys)

    changed, same = [], 0
    for k in sorted(exp_keys & prod_keys):
        deltas = {}
        for col in _COMPARE_COLS:
            a, b = prod.at[k, col], exp.at[k, col]
            if isinstance(a, float) or isinstance(b, float):
                differs = abs(float(a) - float(b)) > 1e-6
            else:
                differs = a != b
            if differs:
                deltas[col] = (a, b)
        if deltas:
            changed.append((k, deltas))
        else:
            same += 1

    return {"added": added, "removed": removed, "changed": changed,
            "same": same, "exp_n": len(exp), "prod_n": len(prod),
            # keep the key-indexed frames so the report can build per-event
            # flyto links (added/changed → exp row, removed → prod row).
            "_exp": exp, "_prod": prod}


def _map_link(row, map_path: str, base_url: str) -> str:
    """Deep-link into the rendered map for this event's aircraft. Uses ?tail=
    (which the map's JS runs as a tail search) on flight1 — event_id isn't stored
    per-cell (it's a render-time row index), so ?tail= is the reliable choice.

    `map_path` is the map's path relative to the serve root (project root when you
    run `serve.py . 8080`), e.g. data_local/v2_exp/maps/wvi_<start>_<end>.html."""
    try:
        tail = str(row["flight1"])
        return f"{base_url}/{map_path}?tail={tail}"
    except Exception:
        return ""


def _link_for(key: str, r: dict, prefer: str, map_path: str, base_url: str) -> str:
    """Look the event up in the preferred frame ('exp'/'prod'), fall back to the
    other, and build its map deep-link."""
    first = r["_exp"] if prefer == "exp" else r["_prod"]
    second = r["_prod"] if prefer == "exp" else r["_exp"]
    for frame in (first, second):
        if key in frame.index:
            return _map_link(frame.loc[key], map_path, base_url)
    return ""


def _print_report(r: dict, region: str, start: str, end: str,
                  base_url: str, exp_root: str) -> None:
    # Map path relative to the serve root (project root). The exp map lives at
    # <exp_root>/maps/<region>_<start>_<end>.html; serve with `serve.py . 8080`.
    map_path = f"{exp_root}/maps/{region}_{start}_{end}.html"
    print(f"LOS event diff — region {region}  {start}–{end}")
    print(f"  prod: {r['prod_n']} events   exp: {r['exp_n']} events")
    print(f"  same: {r['same']}   changed: {len(r['changed'])}   "
          f"added: {len(r['added'])}   removed: {len(r['removed'])}")
    if r["added"] or r["removed"] or r["changed"]:
        print(f"  links open the EXP map — serve from the PROJECT ROOT first: "
              f"python src/hotspots/serve.py . 8080")

    if r["added"]:
        print("\n  ADDED (exp flags, prod did not):")
        for k in r["added"]:
            print(f"    + {k}")
            print(f"        {_link_for(k, r, 'exp', map_path, base_url)}")
    if r["removed"]:
        print("\n  REMOVED (prod flagged, exp does not):")
        for k in r["removed"]:
            print(f"    - {k}")
            print(f"        {_link_for(k, r, 'prod', map_path, base_url)}")
    if r["changed"]:
        print("\n  CHANGED (same event, attributes moved  prod → exp):")
        for k, deltas in r["changed"]:
            parts = ", ".join(f"{c}: {a} → {b}" for c, (a, b) in deltas.items())
            print(f"    ~ {k}   {parts}")
            print(f"        {_link_for(k, r, 'exp', map_path, base_url)}")

    net = r["exp_n"] - r["prod_n"]
    print(f"\n  net event change: {net:+d}")


def main():
    ap = argparse.ArgumentParser(description="Diff v2 LOS events between two roots.")
    ap.add_argument("--region", default="wvi")
    ap.add_argument("--start-date", required=True, help="YYYYMMDD")
    ap.add_argument("--end-date", required=True, help="YYYYMMDD")
    ap.add_argument("--exp", default="data_local/v2_exp",
                    help="Experimental data root (default: data_local/v2_exp)")
    ap.add_argument("--prod", default="data/v2",
                    help="Baseline data root (default: data/v2)")
    ap.add_argument("--base-url", default="http://localhost:8080",
                    help="Base URL of the served map for the ?tail= deep-links "
                         "(default: http://localhost:8080)")
    args = ap.parse_args()

    bounds = _region_bounds(args.region)
    date_tags = _date_tags(args.start_date, args.end_date)
    exp = _load_events(Path(args.exp), bounds, date_tags)
    prod = _load_events(Path(args.prod), bounds, date_tags)
    _print_report(compare(exp, prod), args.region, args.start_date,
                  args.end_date, args.base_url.rstrip("/"),
                  args.exp.rstrip("/"))


if __name__ == "__main__":
    main()

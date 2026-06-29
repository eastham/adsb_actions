"""Read-only on-disk status reporter for the v2 pipeline.

Answers "what state is the pipeline in for this region/date range?" without
touching anything. Covers:
  - Stage 2/3: brief per-cell present/missing counts (via verify.verify_day)
  - Stage 4: regional parquet presence, size, event count, mtime
  - Stage 5: map HTML / pmtiles / tracks presence, sizes, mtime + staleness
  - Provenance: which git SHA(s) produced the on-disk events, with a loud
    version-mixing warning + the exact re-run command when a region's days were
    computed at different SHAs (the algorithm-version-mixing fear).

Pure reporting: callers print the returned lines.
"""

from datetime import datetime, timedelta
from pathlib import Path

from hotspots import provenance as prov
from hotspots.verify import verify_day
from hotspots.term import stage as _stage, warn as _warn, ok as _ok


def _date_tags(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()
    out, d = [], s
    while d <= e:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def _mb(path: Path) -> str:
    return f"{path.stat().st_size / 1024 / 1024:.1f} MB"


def _mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime)


def _cell_in_box(cell_tag: str, bounds) -> bool:
    lat_min, lat_max, lon_min, lon_max = bounds
    try:
        lat, lon = (int(x) for x in cell_tag.split("_"))
    except ValueError:
        return False
    return lat_min <= lat < lat_max and lon_min <= lon < lon_max


def collect_provenance_shas(config, bounds, date_tags: list[str]) -> dict:
    """Map git_sha -> sorted list of (date_tag, cell_tag) it produced, across all
    in-box event cells for the date range. Cells with no provenance manifest are
    grouped under the key None ('unknown / pre-tagging').

    Note: only days whose manifest file is entirely absent contribute to the None
    bucket. A day with a manifest that is merely *missing some cells* (e.g. a
    parquet written before tagging existed) under-reports those cells here — they
    are silently skipped rather than counted as untagged. Good enough for the
    version-mixing warning; not a precise audit of every parquet on disk."""
    by_sha: dict = {}
    for dt in date_tags:
        manifest = prov.read_provenance(config.events_dir / dt)
        if manifest is None:
            continue
        for cell_tag, rec in manifest.items():
            if not _cell_in_box(cell_tag, bounds):
                continue
            sha = rec.get("git_sha")
            by_sha.setdefault(sha, []).append((dt, cell_tag))
    return by_sha


def count_dirty_cells(config, bounds, date_tags: list[str]) -> int:
    """Number of in-box event cells built from a DIRTY working tree (git_dirty=
    true). A clean SHA match can still hide divergent code if it was built with
    uncommitted edits, so status surfaces this separately from SHA mixing."""
    dirty = 0
    for dt in date_tags:
        manifest = prov.read_provenance(config.events_dir / dt)
        if manifest is None:
            continue
        for cell_tag, rec in manifest.items():
            if _cell_in_box(cell_tag, bounds) and rec.get("git_dirty"):
                dirty += 1
    return dirty


def report(config, region_label: str, bounds, start: str, end: str) -> list[str]:
    """Build the human-readable status lines for a region + date range."""
    lat_min, lat_max, lon_min, lon_max = bounds
    n_cells = (lat_max - lat_min) * (lon_max - lon_min)
    date_tags = _date_tags(start, end)
    lines: list[str] = []

    lines.append(_stage(f"v2 status — region {region_label} "
             f"lat[{lat_min},{lat_max})×lon[{lon_min},{lon_max})  "
             f"({n_cells} cells/day)"))
    lines.append(f"  dates {start}–{end}  ({len(date_tags)} day(s))")

    # --- Stage 2 + 3: brief per-day cell counts -------------------------------
    s2_present = s2_expected = 0
    s3_ok = s3_empty = s3_missing = s3_expected = 0
    s3_missing_days = []
    for dt in date_tags:
        # Existence-only (sanity=False): a status overview shouldn't open every
        # parquet on the network mount — that's the gate's job, not status's.
        r2 = verify_day(2, dt, bounds, config.grid_dir, config.events_dir,
                        sanity=False)
        s2_present += r2.present_ok
        s2_expected += r2.expected
        r3 = verify_day(3, dt, bounds, config.grid_dir, config.events_dir,
                        sanity=False)
        s3_ok += r3.present_ok
        s3_empty += r3.present_empty
        s3_missing += len(r3.missing)
        s3_expected += r3.expected
        if r3.missing:
            s3_missing_days.append(dt)

    lines.append(f"  Stage 2 shards: {s2_present}/{s2_expected} cell-days present")
    lines.append(f"  Stage 3 events: {s3_ok + s3_empty}/{s3_expected} cell-days "
             f"({s3_ok} with events, {s3_empty} empty, {s3_missing} missing)")
    if s3_missing_days:
        head = ", ".join(s3_missing_days[:5])
        more = f" (+{len(s3_missing_days)-5} more)" if len(s3_missing_days) > 5 else ""
        lines.append("    " + _warn(f"days with missing event cells: {head}{more}"))

    # --- Stage 4: regional parquet --------------------------------------------
    # GOTCHA: the filename uses region_label verbatim. A *named* region ('conus')
    # and the equivalent explicit bounds ('24_50_-125_-65') produce DIFFERENT
    # filenames for the same geography, so `status --region conus` won't see a
    # regional file that was generated under the bounds label (and vice-versa).
    regional = config.regional_dir / f"{region_label}_{start}_{end}.parquet"
    if regional.exists():
        try:
            import pyarrow.parquet as pq
            n_events = pq.read_metadata(str(regional)).num_rows
        except Exception:
            n_events = "?"
        lines.append(f"  Stage 4 regional: {regional.name}  {_mb(regional)}  "
                 f"{n_events} events  ({_mtime(regional):%Y-%m-%d %H:%M})")
    else:
        lines.append(f"  Stage 4 regional: (none) — expected {regional.name}")

    # --- Stage 5: map artifacts + staleness -----------------------------------
    html = config.maps_dir / f"{region_label}_{start}_{end}.html"
    pmt = html.with_suffix(".pmtiles")
    tracks = config.maps_dir / f"{region_label}_{start}_{end}_tracks"
    if html.exists():
        parts = [f"html {_mb(html)}"]
        if pmt.exists():
            parts.append(f"pmtiles {_mb(pmt)}")
        if tracks.is_dir():
            parts.append("tracks ✓")
        lines.append(f"  Stage 5 map: {html.name}  ({', '.join(parts)})  "
                 f"({_mtime(html):%Y-%m-%d %H:%M})")
        if regional.exists() and _mtime(regional) > _mtime(html):
            lines.append("    " + _warn(
                "map is STALE — regional parquet is newer than the map "
                "(re-run stage 5 to refresh)"))
    else:
        lines.append(f"  Stage 5 map: (none) — expected {html.name}")

    # --- Provenance / version-mixing check ------------------------------------
    lines.extend(_provenance_lines(config, region_label, bounds, date_tags))

    # --- Traffic-tile build provenance (standalone tool, not a v2 stage) -------
    lines.extend(_traffic_tile_lines(config))
    return lines


def _traffic_tile_lines(config) -> list[str]:
    """Report the version/settings that produced the local traffic-tile heatmap,
    read from <traffic_tiles_local>/_provenance.json. The heatmap is hand-tuned
    often, so its SHA/dirty/tuning is worth surfacing alongside the v2 stages."""
    local = config.traffic_tiles_local
    if not local:
        return []
    # traffic_tiles_local is a browser-facing path prefix (e.g. ../../../tiles/
    # traffic) whose trailing portion is the project-root-relative tile dir.
    # Resolve to <project_root>/tiles/traffic by dropping leading '../' segments.
    project_root = config.data_root.parents[1]   # data_root is <root>/data/v2
    tiles_dir = Path(local)
    if tiles_dir.is_absolute():
        pass
    else:
        rel = Path(*[p for p in tiles_dir.parts if p != ".."])
        tiles_dir = project_root / rel
    rec = prov.read_provenance(tiles_dir)
    if rec is None:
        return [f"  Traffic tiles: no provenance at {tiles_dir} (untagged build)"]

    sha = rec.get("git_sha", "?")
    built = rec.get("built_utc", "?")[:16].replace("T", " ")
    line = f"  Traffic tiles: built at {sha} ({built})"
    if rec.get("git_dirty"):
        return [line, "    " + _warn(
            "tile build was DIRTY — heatmap tuning/code had uncommitted changes")]
    return ["  " + _ok(line.strip())]


def _dirty_lines(config, bounds, date_tags) -> list[str]:
    """A '⚠ N cells built dirty' note, or [] if all builds were clean. A clean
    SHA match can still hide divergent code built with uncommitted edits."""
    n_dirty = count_dirty_cells(config, bounds, date_tags)
    if not n_dirty:
        return []
    return ["  " + _warn(
        f"{n_dirty} cell(s) built with uncommitted changes (dirty) — "
        f"the SHA may not capture the exact code that ran")]


def _provenance_lines(config, region_label, bounds, date_tags) -> list[str]:
    by_sha = collect_provenance_shas(config, bounds, date_tags)
    if not by_sha:
        return ["  Provenance: unknown (no _provenance.json — pre-tagging outputs)"]

    known = {sha: cells for sha, cells in by_sha.items() if sha is not None}
    n_unknown = len(by_sha.get(None, []))
    current = prov.git_sha()

    lines = []
    if len(known) <= 1 and n_unknown == 0:
        only = next(iter(known), None)
        tag = " (matches HEAD)" if only == current else f" (HEAD is {current})"
        lines.append("  " + _ok(f"Provenance: all events at {only}{tag}"))
        return lines + _dirty_lines(config, bounds, date_tags)

    # Mixed: list each SHA with its day span, then offer the fix.
    lines.append("  " + _warn(
        f"Provenance MIXED — region {region_label} built from "
        f"{len(known)} detector version(s)"
        + (f" + {n_unknown} untagged cell(s)" if n_unknown else "") + ":"))
    stale_days: set = set()
    for sha in sorted(known, key=lambda s: min(d for d, _ in known[s])):
        cells = known[sha]
        days = sorted({d for d, _ in cells})
        span = f"{days[0]}–{days[-1]}" if len(days) > 1 else days[0]
        marker = "  ← current" if sha == current else ""
        lines.append(f"      {sha}: {len(cells)} cell(s), days {span}{marker}")
        if sha != current:
            stale_days.update(days)
    if n_unknown:
        lines.append(f"      (untagged): {n_unknown} cell(s)")

    if stale_days and current:
        lo, hi = min(stale_days), max(stale_days)
        lines.append(f"  Rebuild stale days at current SHA ({current}):")
        lines.append(f"    cli.py run analyze --region {region_label} "
                     f"--start-date {lo} --end-date {hi}")
    return lines + _dirty_lines(config, bounds, date_tags)

#!/usr/bin/env python3
"""
v2 LOS Pipeline CLI — declarative, config-driven entry point.

Replaces the old flag-soup invocation of pipeline.py with named regions and
workflow profiles from pipeline_config.yaml, plus a read-only `status` command
and a day-at-a-time orchestration loop that verifies each day's output and
retries (with an optional remount) when the network drive drops.

  # Most common: re-render the map for a region (stage 5 only)
  python src/hotspots/cli.py run visualize --region conus

  # Re-aggregate + render
  python src/hotspots/cli.py run aggregate-viz --region wvi

  # Explicit stage range (no profile)
  python src/hotspots/cli.py run --from 4 --to 5 --region conus

  # What's on disk?
  python src/hotspots/cli.py status --region conus

  # Override the default date window
  python src/hotspots/cli.py run analyze --region wvi \\
      --start-date 20250714 --end-date 20250714

Stage selection maps onto pipeline.py's existing skip flags; the runners
(run_stages_23 / run_stage4 / run_stage5) are reused unchanged.
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Where the raspi5 share is expected to be mounted. `data/` in the project root
# is a symlink to this; runs whose paths resolve under it are mount-checked
# before any writes (see _assert_network_mounted).
NETWORK_MOUNT = "~/raspi5-data"
MOUNT_HINT = "mount_smbfs //pi@raspi5/data ~/raspi5-data"

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT / "src"), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from hotspots import config as config_mod
from hotspots.config import load_config
from hotspots.verify import verify_day
from hotspots import provenance as prov
from hotspots import status as status_mod


from hotspots.term import (stage as _stage, ok as _ok, fail as _fail,
                           warn as _warn, rel as _rel, ARROW)


def _assert_network_mounted(config) -> None:
    """Abort before any writes if data_root/conus_dir point at the network share
    but it isn't actually mounted.

    When the SMB share drops, its mountpoint reverts to an ordinary empty
    directory that is still perfectly writable. A full run then "succeeds" —
    every cell written, verify_day() counting the expected total — with the
    output silently landing on local disk. The day-at-a-time gate can't catch
    this: it checks completeness (did we get N cells?), not destination, and
    output on the wrong disk is complete. So this has to be a precondition.

    Only paths under the network mount are checked, so the local-disk configs
    (test/exp sandboxes) run untouched.
    """
    for label, path in (("data_root", config.data_root),
                        ("conus_dir", config.conus_dir)):
        # Expand ~ before resolving: a config path like ~/raspi5-data/v2 would
        # otherwise resolve to a bogus cwd-relative path and escape the check.
        # Resolve symlinks too — `data/` is expected to be a symlink to the
        # mount, so the mountpoint test has to run against the real location.
        real = Path(path).expanduser().resolve()
        mount = _mount_root(real)
        if mount is None:
            continue  # local disk (test/exp sandbox) — nothing to verify
        if not os.path.ismount(mount):
            raise SystemExit(_fail(
                f"\nABORT: {label} ({_rel(path)}) lives on the network share at "
                f"{mount}, which is NOT mounted.\n"
                f"  Writing now would silently fill local disk instead of the "
                f"drive.\n"
                f"  Remount with:\n    {config.remount_cmd or MOUNT_HINT}\n"))


def _mount_root(real: Path):
    """Return the expected network mountpoint that `real` sits under, or None if
    it's on local disk. Keyed off the configured mount location so a relocated
    share only has to be updated in one place."""
    expected = Path(NETWORK_MOUNT).expanduser()
    if real == expected or expected in real.parents:
        return expected
    return None


def _parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y%m%d").date()


def _date_range(start: datetime.date, end: datetime.date):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def _resolve_region(config, args):
    """Return (region_label, bounds) from --region or explicit --lat/--lon-*."""
    if args.region:
        return args.region, config.region_bounds(args.region)
    if None in (args.lat_min, args.lat_max, args.lon_min, args.lon_max):
        raise SystemExit("provide --region NAME, or all of "
                         "--lat-min/--lat-max/--lon-min/--lon-max")
    label = f"{args.lat_min}_{args.lat_max}_{args.lon_min}_{args.lon_max}"
    return label, (args.lat_min, args.lat_max, args.lon_min, args.lon_max)


def _resolve_stages(config, args) -> list[int]:
    """Stages to run: from a named profile, or explicit --from/--to."""
    if args.profile:
        return sorted(config.profile(args.profile)["stages"])
    if args.from_stage is not None:
        to = args.to_stage if args.to_stage is not None else 5
        return [s for s in range(args.from_stage, to + 1) if s in (2, 3, 4, 5)]
    raise SystemExit("specify a profile (e.g. 'run visualize') or --from N")


def _resolve_dates(config, args):
    start = args.start_date or config.default_start
    end = args.end_date or config.default_end
    if not start or not end:
        raise SystemExit("no dates: pass --start-date/--end-date or set "
                         "dates.default_* in the config")
    return _parse_date(start), _parse_date(end)


# ---------------------------------------------------------------------------
# Day-at-a-time orchestration gate (stages 2 & 3)
# ---------------------------------------------------------------------------

def _attempt_remount(config) -> None:
    """Run the configured remount command (if any) and pause, to recover a
    dropped network mount before retrying a day."""
    cmd = config.remount_cmd
    if cmd:
        print(f"  [remount] running: {cmd}")
        try:
            subprocess.run(cmd, shell=True, timeout=120, check=False)
        except Exception as e:  # never let remount failure crash the run
            print(f"  [remount] command failed: {e}")
    if config.retry_pause_s:
        print(f"  [remount] pausing {config.retry_pause_s}s before retry...")
        time.sleep(config.retry_pause_s)


def _run_day_gated(config, runners, date, bounds, stages, workers,
                   skip_existing, region_label) -> dict:
    """Run stages 2/3 for one day, verifying after each attempt and retrying
    (with remount) on incomplete output. Returns the stats dict, or raises
    SystemExit if a day can't be completed."""
    lat_min, lat_max, lon_min, lon_max = bounds
    date_tag = date.strftime("%Y%m%d")
    do_shard = 2 in stages
    gate_stages = [s for s in (2, 3) if s in stages]

    last_stats = {}
    for attempt in range(1, config.retry_attempts + 1):
        last_stats = runners.run_stages_23(
            date=date,
            lat_min=lat_min, lat_max=lat_max, lon_min=lon_min, lon_max=lon_max,
            conus_dir=str(config.conus_dir),
            workers=workers,
            skip_shard=not do_shard,
            skip_existing=skip_existing,
        )

        # Verify each gated stage with deep sanity (catches truncation).
        bad = None
        for st in gate_stages:
            rpt = verify_day(st, date_tag, bounds, config.grid_dir,
                             config.events_dir, sanity=True)
            if not rpt.ok:
                bad = (st, rpt)
                break

        if bad is None:
            return last_stats

        st, rpt = bad
        print(_fail(f"day {date_tag} stage {st} incomplete "
                    f"(attempt {attempt}/{config.retry_attempts}): "
                    f"{rpt.accounted}/{rpt.expected} cells, "
                    f"{len(rpt.missing)} missing"))
        if attempt < config.retry_attempts:
            _attempt_remount(config)

    # Exhausted retries — stop with a precise, copy-pasteable re-run command.
    st, rpt = bad
    raise SystemExit(_fail(
        f"\nABORT: day {date_tag} stage {st} incomplete after "
        f"{config.retry_attempts} attempts: expected {rpt.expected}, "
        f"found {rpt.accounted} (drive likely disconnected).") +
        f"\n  Re-run just this day:\n"
        f"    python src/hotspots/cli.py run --from {min(st, 3)} "
        f"--region {region_label} "
        f"--start-date {date_tag} --end-date {date_tag}\n"
    )


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def cmd_run(config, args) -> None:
    import hotspots.pipeline as runners  # the runner library
    import pandas as pd

    run_start = time.time()  # provenance: only stamp cells (re)written this run
    region_label, bounds = _resolve_region(config, args)
    stages = _resolve_stages(config, args)
    start, end = _resolve_dates(config, args)
    if end < start:
        raise SystemExit("--end-date must be >= --start-date")

    workers = args.workers if args.workers is not None else config.workers
    pmtiles = _resolve_pmtiles(config, args)
    # Defaults to the PRODUCTION tile URL. For local preview pass --traffic-tiles
    # (e.g. the paths.traffic_tiles_local value) — it is not used automatically.
    traffic = args.traffic_tiles or config.traffic_tiles_url
    # ForeFlight packs a real filesystem tile tree (not the browser prefix). An
    # explicit local --traffic-tiles wins; otherwise use the config's local path.
    # A URL passed to --traffic-tiles is a browser value only, so ignore it here.
    ff_tiles = (args.traffic_tiles
                if (args.traffic_tiles and not args.traffic_tiles.startswith("http"))
                else config.traffic_tiles_local)
    lat_min, lat_max, lon_min, lon_max = bounds
    n_cells = (lat_max - lat_min) * (lon_max - lon_min)
    n_days = (end - start).days + 1

    start_tag, end_tag = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    regional = config.regional_dir / f"{region_label}_{start_tag}_{end_tag}.parquet"
    out_html = config.maps_dir / f"{region_label}_{start_tag}_{end_tag}.html"
    # --no-foreflight opts out of the (slow) Content Pack build: run_stage5
    # skips it when foreflight_output is None.
    ff_out = None if args.no_foreflight else str(
        config.data_root / "foreflight" /
        f"{region_label}_{start_tag}_{end_tag}.zip")
    # Append the run's date range to the pack's display name so pilots can see
    # which vintage is installed in ForeFlight's More > Custom Content list.
    # The config value is a BASE name; None -> export_pack's built-in default.
    # Slash-free format on purpose: pack_name also becomes the zip folder name.
    ff_name = config.foreflight_pack_name
    if ff_name:
        ff_name = f"{ff_name} {start:%b %-d}-{end:%b %-d, %Y}"

    print(_stage("v2 LOS Pipeline (cli)"))
    print(f"  Profile/stages: {args.profile or '(explicit)'} {ARROW} {stages}")
    print(f"  Region: {region_label}  lat[{lat_min},{lat_max})×lon[{lon_min},{lon_max})  "
          f"({n_cells} cells/day)")
    print(f"  Dates:  {start_tag}–{end_tag}  ({n_days} day(s))")
    print(f"  Workers: {workers} | PMTiles: {pmtiles}")

    if args.dry_run:
        _print_dry_run(config, args, stages, bounds, start, end,
                       regional, out_html,
                       Path(ff_out) if ff_out else None, pmtiles, ff_tiles)
        return

    # Fail fast if the network share isn't mounted — an unmounted share is still
    # writable and would silently absorb the whole run onto local disk.
    _assert_network_mounted(config)

    # Stages 2/3: day-at-a-time with verify + remount/retry gate. Provenance is
    # written per-day right after the gate passes, so if a later day aborts the
    # days already completed are still correctly tagged (not left untagged).
    if 2 in stages or 3 in stages:
        for i, d in enumerate(_date_range(start, end), 1):
            print(_stage(f"\n[day {i}/{n_days}] {d:%Y%m%d}  "
                         f"stages {sorted(set(stages) & {2,3})}"))
            s = _run_day_gated(config, runners, d, bounds, stages, workers,
                               args.skip_existing, region_label)
            print(_ok(f"shard: {s['shard_kb']:,} KB  analyze: {s['analyze_s']:.0f}s  "
                      f"events: {s['events']}"))
            # Tag exactly the cells stage 3 (re)wrote this run. Cells skipped via
            # --skip-existing keep their prior SHA — re-stamping them with the
            # current SHA would falsely claim they were built by this code.
            if 3 in stages:
                _write_stage3_provenance(config, d, d, bounds, run_start)

    # Stage 4: aggregate.
    df = pd.DataFrame()
    if 4 in stages:
        print(_stage(f"\nStage 4: aggregate {ARROW} {regional.name}"))
        date_tags = [d.strftime("%Y%m%d") for d in _date_range(start, end)]
        df = runners.run_stage4(date_tags, lat_min, lat_max, lon_min, lon_max,
                                region_label, str(regional))
        _write_regional_provenance(config, regional, bounds, date_tags)
        print(_ok(f"aggregated {len(df):,} events"))
    elif regional.exists():
        from hotspots.stage5_visualize import load_events
        df = load_events(str(regional))
        print(_stage(f"\nStage 4 skipped") +
              f" — loaded {len(df):,} events from {regional.name}")

    # Airport-quality / runway-usage overlay. OFF by default; enabled only by
    # --airport-quality. When on, it runs after stage 4 and before stage 5
    # (stage 5 renders the icons), so it's not a numbered stage of its own.
    airport_quality = _resolve_airport_quality(config, args, start, end) \
        if args.airport_quality else None

    # Stage 5: visualize.
    if 5 in stages:
        print(_stage(f"\nStage 5: map "
                     f"({'PMTiles' if pmtiles else 'self-contained HTML'})"))
        runners.run_stage5(df, str(out_html), pmtiles=pmtiles, zoom=args.zoom,
                           traffic_tile_dir=traffic, html_only=args.html_only,
                           foreflight_output=ff_out,
                           foreflight_name=ff_name,
                           foreflight_tiles=ff_tiles,
                           print_summary=False,
                           airport_quality=airport_quality,
                           asset_stem=args.asset_stem)
        print(_ok(f"map written: {out_html.name}"))

    # Always print actionable next-step commands.
    local_tiles = traffic if (traffic and not traffic.startswith("http")) else None
    print()
    if pmtiles:
        # Local test: serve over HTTP (search/track sidecars are fetched, so
        # file:// won't work — the Range-capable server is required). The URL
        # path must be relative to the served dir. Serve from cwd when the map
        # lives under it (production layout); otherwise serve the map's own
        # parent dir so the sidecars resolve (e.g. absolute test maps_dir).
        try:
            url_path = out_html.resolve().relative_to(Path.cwd().resolve())
            serve_dir = "."
        except ValueError:
            url_path = out_html.name
            serve_dir = str(out_html.resolve().parent)
        base_url = f"http://localhost:8080/{url_path}"
        print(f"  Serve:  python src/hotspots/serve.py {serve_dir} 8080")
        print(f"  Open:   {base_url}")
        print(f"  Search: {base_url}?tail=N12345"
              f"   (tail-number deep-link; replace with a real tail)")
        # deploy_v2's --traffic-tiles-dir is a LOCAL upload dir (relative to
        # cwd), not the HTML's tile URL (which is relative to the served map and
        # often "../../../tiles/traffic"). Only hint the flag when traffic tiles
        # were used; the conventional local dir is tiles/traffic.
        traffic_flag = " --traffic-tiles-dir tiles/traffic" if local_tiles else ""
        # Push the ForeFlight Content Pack alongside the map when one was built.
        # --foreflight-publish-as promotes it to the stable conus.zip the public
        # page links to, and stamps the page's date range from this same pack.
        ff_flag = (f" --foreflight-pack {ff_out} --foreflight-publish-as conus"
                   if ff_out and Path(ff_out).exists() else "")
        # Promoting also updates the site root; see deploy_v2 --no-publish-root.
        print(f"  Deploy: python src/tools/deploy_v2 --publish-as conus "
              f"--source-stem {out_html.stem}{traffic_flag}{ff_flag}")
    else:
        print(f"  Open:  file://{out_html.resolve()}")
    if ff_out and Path(ff_out).exists():
        preview_cmd = f"python src/tools/preview_mbtiles.py --zip '{ff_out}'"
        if local_tiles:
            preview_cmd += f" --traffic-tiles '{local_tiles}'"
        print(f"  ForeFlight: {ff_out}")
        print(f"  Preview:    {preview_cmd}")
    print()
    print(_ok("Done."))


def _print_dry_run(config, args, stages, bounds, start, end,
                   regional, out_html, ff_out, pmtiles, ff_tiles=None) -> None:
    """Describe what a real run with these exact args WOULD do, without touching
    anything. Stage 2/3 use verify_day (existence-only, per-day counts); stages
    4/5 are single-file outputs. WRITE = output absent; SKIP = present and
    --skip-existing; OVERWRITE = present and not skipping."""
    skip = args.skip_existing

    def _verb(exists: bool, skippable: bool) -> str:
        if not exists:
            return _ok("WRITE (new)")
        return (_stage("SKIP (exists)") if (skippable and skip)
                else _warn("OVERWRITE (exists)"))

    print(_stage("\nDRY RUN — nothing will be written"))
    if not skip:
        print("  (no --skip-existing: existing outputs would be OVERWRITTEN)")

    # Stages 2/3: per-day present/missing counts. Present cells are skippable
    # (the runners honor --skip-existing per cell); missing cells are new writes.
    for st in (2, 3):
        if st not in stages:
            continue
        would_write = would_touch = 0
        for d in _date_range(start, end):
            rpt = verify_day(st, d.strftime("%Y%m%d"), bounds,
                             config.grid_dir, config.events_dir, sanity=False)
            would_write += len(rpt.missing)
            would_touch += rpt.accounted
        touch_verb = "skip" if skip else "overwrite"
        print(f"  Stage {st}: {would_write} cell-day(s) to WRITE, "
              f"{would_touch} existing to {touch_verb.upper()}")

    # Stages 4/5: single-file outputs. These runners do NOT honor --skip-existing,
    # so an existing file is always an overwrite regardless of the flag. Show
    # cwd-relative paths so it's clear WHERE each artifact lands.
    if 4 in stages:
        print(f"  Stage 4: {_rel(regional)}  {_verb(regional.exists(), skippable=False)}")
    if 5 in stages:
        print(f"  Stage 5: {_rel(out_html)}  {_verb(out_html.exists(), skippable=False)}")
        if ff_out:
            print(f"  Stage 5 ForeFlight: {_rel(ff_out)}  "
                  f"{_verb(ff_out.exists(), skippable=False)}")
            # Will the traffic layer be packed? Resolve the same local tile tree
            # the export uses (config.traffic_tiles_local, browser-relative) and
            # report found/missing so a silent no-traffic pack is visible here.
            from hotspots.pipeline import _foreflight_tile_dir
            tiles = _foreflight_tile_dir(ff_tiles or config.traffic_tiles_local)
            if tiles is not None:
                print(f"    traffic layer: {_rel(tiles)}  {_ok('will pack')}")
            else:
                print("    traffic layer: " +
                      _warn("NOT packed (no local tile tree found — "
                            "LOS-events-only pack)"))

    # Airport-quality overlay (opt-in): report the aq/ cache state so a stale
    # cache (predating runway-usage) is visible before the real run.
    if getattr(args, "airport_quality", False) and 5 in stages:
        for line in _airport_quality_status_lines(config, start, end):
            print(line)

    print(_ok("\nDry run complete — re-run without --dry-run to execute."))


def _airport_quality_status_lines(config, start, end) -> list[str]:
    """Lines describing the aq/ per-day cache for a date range: how many
    day-files are present, where they live (cwd-relative), and whether any
    predate the runway-usage feature (so the overlay would omit runway stats).

    Shared by the dry-run and the real aggregate path so both surface a stale
    cache identically.
    """
    from tools.v2_airport_quality import aq_day_path
    from hotspots.status import _day_file_has_runway_data

    aq_dir = config.aq_dir
    tags = [d.strftime("%Y%m%d") for d in _date_range(start, end)]
    present = [t for t in tags if aq_day_path(aq_dir, t).exists()]
    lines = [f"  Airport-quality cache: {len(present)}/{len(tags)} day-file(s) "
             f"in {_rel(aq_dir)}"]
    if not present:
        lines.append("    " + _warn("overlay would be skipped — compute it with:"))
        lines.append(f"      python -m tools.v2_airport_quality --mode compute "
                     f"--start-date {tags[0]} --end-date {tags[-1]}")
        return lines
    stale = [t for t in present
             if not _day_file_has_runway_data(aq_day_path(aq_dir, t))]
    if stale:
        head = ", ".join(stale[:5])
        more = f" (+{len(stale)-5} more)" if len(stale) > 5 else ""
        lines.append("    " + _warn(
            f"{len(stale)} day-file(s) predate runway-usage ({head}{more}) — "
            f"airport overlay will omit runway stats. Recompute with:"))
        lines.append(f"      python -m tools.v2_airport_quality --mode compute "
                     f"--start-date {tags[0]} --end-date {tags[-1]} --force")
    return lines


def _resolve_airport_quality(config, args, start, end) -> dict | None:
    """Build the airport-quality / runway-usage dict that stage 5 renders as icons.

    This only READS pre-computed scores; it never runs the hours-long compute
    itself (that's the standalone `v2_airport_quality --mode compute` tool). It
    resolves in this order:
      1. --airport-quality-path FILE  → load that pre-built aggregate JSON as-is.
      2. else aggregate the per-day score files in <aq_dir>/ for the date range:
         - all days present  → return the full aggregate.
         - some days present → aggregate those and warn about the missing ones.
         - no days present   → return None (skip), printing the compute command.
    """
    import json
    from tools.v2_airport_quality import aggregate_days, aq_day_path

    if args.airport_quality_path:
        aq_path = Path(args.airport_quality_path)
        if not aq_path.exists():
            raise SystemExit(f"--airport-quality-path not found: {aq_path}")
        print(_stage("\nAirport-quality: loading ") + str(aq_path))
        with open(aq_path, "r", encoding="utf-8") as f:
            return json.load(f)

    aq_dir = config.aq_dir
    requested = list(_date_range(start, end))
    present = [d for d in requested
               if aq_day_path(aq_dir, d.strftime("%Y%m%d")).exists()]
    missing = [d for d in requested if d not in present]

    print(_stage("\nAirport-quality: aggregate cached per-day scores"))
    if not present:
        print(_warn(f"no per-day files in {aq_dir} for {start:%Y%m%d}–{end:%Y%m%d} "
                    f"— skipping airport quality"))
        print(f"  To compute them first, run:\n"
              f"    python -m tools.v2_airport_quality --mode compute "
              f"--start-date {start:%Y%m%d} --end-date {end:%Y%m%d} "
              f"--workers {args.workers if args.workers else config.workers}")
        return None
    if missing:
        print(_warn(f"have {len(present)} day(s), missing {len(missing)} "
                    f"(first: {missing[0]:%Y%m%d}) — quality reflects "
                    f"{len(present)} day(s) only"))

    # Warn if any present day-file predates the runway-usage feature: the
    # overlay will render (coverage score is unaffected) but silently omit
    # runway stats until the cache is recomputed. Same lines the dry-run shows.
    for line in _airport_quality_status_lines(config, start, end):
        print(line)

    quality = aggregate_days(date_range=present, aq_dir=aq_dir)
    print(_ok(f"airport-quality: {len(quality)} airports "
              f"from {len(present)} day(s)"))
    return quality


def _resolve_pmtiles(config, args) -> bool:
    """Precedence: explicit --no-pmtiles / --pmtiles win; else the profile's
    `pmtiles:` default; else False (self-contained HTML) for explicit --from/--to
    runs that have no profile."""
    if args.no_pmtiles:
        return False
    if args.pmtiles:
        return True
    if args.profile:
        return bool(config.profile(args.profile).get("pmtiles", False))
    return False


def _write_stage3_provenance(config, start, end, bounds, run_start) -> None:
    """After stage 3, stamp each day's manifest with the current code version —
    but only for cells whose output was (re)written during THIS run (mtime >=
    run_start). Cells skipped via --skip-existing retain their earlier SHA, so
    the manifest keeps telling the truth about which code produced each cell."""
    rec = prov.current_provenance(config)
    lat_min, lat_max, lon_min, lon_max = bounds
    for d in _date_range(start, end):
        dt = d.strftime("%Y%m%d")
        day_dir = config.events_dir / dt
        cells = {}
        for lat in range(lat_min, lat_max):
            for lon in range(lon_min, lon_max):
                stem = f"{dt}_{lat}_{lon}"
                for ext in (".parquet", ".empty"):
                    p = day_dir / f"{stem}{ext}"
                    if p.exists() and p.stat().st_mtime >= run_start:
                        cells[f"{lat}_{lon}"] = rec
                        break
        if cells:
            prov.merge_cell_provenance(day_dir, cells)


def _write_regional_provenance(config, regional, bounds, date_tags) -> None:
    """Roll up the SHAs of exactly the cells aggregated into this regional file,
    writing a sidecar so status can detect version-mixing later."""
    by_sha = status_mod.collect_provenance_shas(config, bounds, date_tags)
    summary = {
        "built_utc": prov.current_provenance(config)["written_utc"],
        "built_from_shas": sorted(s for s in by_sha if s is not None),
        "untagged_cells": len(by_sha.get(None, [])),
    }
    sidecar = regional.with_name(regional.stem + "_provenance.json")
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=0, sort_keys=True)


# ---------------------------------------------------------------------------
# catchup — sliding-window daily refresh (+ chunked initial backfill)
# ---------------------------------------------------------------------------

def _resolve_window(config, args):
    """Resolve the (start, end) dates for a catch-up run.

    Precedence:
      - explicit --start-date/--end-date are used verbatim (backfill chunks);
      - else end = yesterday UTC (today's source release usually isn't up yet),
        start = end − (window_days − 1).
    The true leading edge is discovered at fetch time (ensure_conus → None), so a
    too-recent end just stops early at the last published day.
    """
    days = args.window_days or config.window_days
    if args.end_date:
        end = _parse_date(args.end_date)
    else:
        end = datetime.date.today() - datetime.timedelta(days=1)
    if args.start_date:
        start = _parse_date(args.start_date)
    else:
        start = end - datetime.timedelta(days=days - 1)
    return start, end


def _prune_window(config, keep_start, bounds, dry_run) -> None:
    """Delete v2 data for days strictly before keep_start (out of the window):
    grid/<YYYYMMDD>/ and events/<YYYYMMDD>/ (v2-pipeline-owned), plus regional/
    and maps/ artifacts whose date-stem ends before the window.

    Deliberately does NOT touch source CONUS_/global_ gz files: they live in the
    shared data/ mount and may be used by v1 tooling or downloaded by hand, so
    catch-up never deletes them. Clean data/ by hand if the gz pile up.

    Every removal is printed; --dry-run lists without deleting."""
    import shutil

    verb = "would remove" if dry_run else "removing"
    for path in _prune_targets(config, keep_start):
        print(f"  [prune] {verb} {_rel(path)}")
        if dry_run:
            continue
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)


def _prune_targets(config, keep_start) -> list:
    """The exact paths _prune_window would delete for this keep window.

    Split out so the confirmation prompt and the deletion agree by construction
    rather than by two copies of the same selection rules."""
    import re

    keep_tag = keep_start.strftime("%Y%m%d")
    protected = config.window_keep

    def _kept(tag: str) -> bool:
        """True if tag is inside the rolling window or a protected range."""
        return tag >= keep_tag or any(a <= tag <= b for a, b in protected)

    targets = []

    # Per-day grid/events dirs named YYYYMMDD (owned by the v2 pipeline).
    for base in (config.grid_dir, config.events_dir):
        if not base.exists():
            continue
        for day_dir in sorted(base.iterdir()):
            if day_dir.is_dir() and day_dir.name.isdigit() \
                    and len(day_dir.name) == 8 and not _kept(day_dir.name):
                targets.append(day_dir)

    # Regional/map artifacts whose trailing _<end> date is before the window.
    stem_end = re.compile(r"_(\d{8})_(\d{8})(?:_.*)?$")
    for base in (config.regional_dir, config.maps_dir):
        if not base.exists():
            continue
        for p in sorted(base.iterdir()):
            m = stem_end.search(p.stem if p.is_file() else p.name)
            if not m or m.group(2) >= keep_tag:
                continue
            # An artifact covers a span, so protect it if it OVERLAPS a kept
            # range — an end-date test alone would delete the 2025 summer map.
            span_start, span_end = m.group(1), m.group(2)
            if any(a <= span_end and span_start <= b for a, b in protected):
                continue
            targets.append(p)
    return targets


def _confirm_prune(config, keep_start, bounds, assume_yes=False) -> bool:
    """Ask before deleting out-of-window data. Returns True to proceed.

    Prune is irreversible and (on a mis-scoped window) can wipe days that took
    hours to build, so it gets an explicit y/N gate. Non-interactive runs (cron,
    piped stdin) ABORT the prune rather than assume yes — skipping a prune is
    cheap and self-correcting on the next run; a wrong delete is not."""
    targets = _prune_targets(config, keep_start)
    if not targets:
        print("  [prune] nothing out of window")
        return False

    for path in targets:
        print(f"  [prune] would remove {_rel(path)}")
    day_dirs = sum(1 for p in targets if p.is_dir() and p.name.isdigit())
    print(_warn(f"\n  {len(targets)} path(s) to delete, including {day_dirs} "
                f"day(s) of grid/events data, keeping {keep_start:%Y%m%d} onward."))
    for a, b in config.window_keep:
        print(f"  [prune] protected (window.keep): {a}-{b}")

    if assume_yes:
        print("  --yes given — proceeding.")
        return True
    if not sys.stdin.isatty():
        print(_warn("  Non-interactive session — skipping prune. "
                    "Re-run in a terminal, or pass --no-prune to silence this."))
        return False
    try:
        reply = input("  Delete these? [y/N] ").strip().lower()
    except EOFError:
        return False
    return reply in ("y", "yes")


def cmd_catchup(config, args) -> None:
    import hotspots.pipeline as runners
    import pandas as pd
    from hotspots.fetch_source import ensure_conus

    run_start = time.time()
    region_label = args.region or config.window_region
    bounds = config.region_bounds(region_label)
    lat_min, lat_max, lon_min, lon_max = bounds
    alias = config.window_deploy_alias
    workers = args.workers if args.workers is not None else config.workers
    # stages-only stops after stage 3 → aggregate/visualize/deploy/prune all off.
    do_prune = config.window_prune and not args.no_prune and not args.stages_only
    do_deploy = not args.no_deploy and not args.stages_only

    start, end = _resolve_window(config, args)
    if end < start:
        raise SystemExit("--end-date must be >= --start-date")

    print(_stage("v2 catch-up"))
    print(f"  Region: {region_label}  window {start:%Y%m%d}–{end:%Y%m%d}")
    print(f"  stages-only: {args.stages_only} | deploy: {do_deploy} "
          f"(alias '{alias}') | prune: {do_prune}")

    # --- Fetch + stages 2/3 per day ---
    # Missing (unpublished) or corrupt days are skipped, not fatal — a mid-window
    # gap must not block later days. Trailing missing days are the true leading
    # edge and get trimmed from the window below; all skips are reported at the end.
    missing_days: list[datetime.date] = []   # unpublished (gap or leading edge)
    corrupt_days: list[datetime.date] = []   # release exists but tar unrecoverable
    usable_days: list[datetime.date] = []    # processed or already-present, in window
    for d in _date_range(start, end):
        # Skip days already fully processed unless a rebuild is requested.
        if not args.rebuild:
            r2 = verify_day(2, d.strftime("%Y%m%d"), bounds, config.grid_dir,
                            config.events_dir, sanity=False)
            r3 = verify_day(3, d.strftime("%Y%m%d"), bounds, config.grid_dir,
                            config.events_dir, sanity=False)
            if r2.ok and r3.ok:
                print(_ok(f"{d:%Y%m%d} already processed — skipping"))
                usable_days.append(d)
                continue

        if args.dry_run:
            print(f"  [dry-run] would fetch + run stages 2/3 for {d:%Y%m%d}")
            usable_days.append(d)
            continue

        res = ensure_conus(d, config.conus_dir)
        if res.status == "missing":
            missing_days.append(d)
            continue          # gap — keep going; may be trimmed as leading edge
        if res.status == "corrupt":
            print(_warn(f"{d:%Y%m%d} source corrupt ({res.detail}) — skipping"))
            corrupt_days.append(d)
            continue

        print(_stage(f"\n[{d:%Y%m%d}] stages 2/3"))
        s = _run_day_gated(config, runners, d, bounds, [2, 3], workers,
                           args.skip_existing, region_label)
        print(_ok(f"shard: {s['shard_kb']:,} KB  analyze: {s['analyze_s']:.0f}s  "
                  f"events: {s['events']}"))
        _write_stage3_provenance(config, d, d, bounds, run_start)
        usable_days.append(d)

    # Distinguish the trailing leading-edge run (missing days AFTER the last usable
    # day) from genuine mid-window gaps (missing days that have a usable day after).
    last_usable = max(usable_days) if usable_days else None
    leading_edge = [d for d in missing_days
                    if last_usable is None or d > last_usable]
    window_gaps = [d for d in missing_days if last_usable is not None
                   and d < last_usable]

    def _report_skips() -> None:
        if window_gaps:
            print(_warn(f"\n{len(window_gaps)} missing day(s) skipped WITHIN the "
                        f"window (unpublished on GitHub): "
                        f"{', '.join(f'{d:%Y%m%d}' for d in sorted(window_gaps))}"))
        if corrupt_days:
            print(_warn(f"{len(corrupt_days)} corrupt day(s) skipped: "
                        f"{', '.join(f'{d:%Y%m%d}' for d in sorted(corrupt_days))}"))
        if leading_edge:
            print(_warn(f"{len(leading_edge)} unpublished day(s) at the leading "
                        f"edge (not yet released): "
                        f"{', '.join(f'{d:%Y%m%d}' for d in sorted(leading_edge))}"))

    if args.stages_only:
        _report_skips()
        if args.dry_run:
            print(_ok("\nDry run complete (stages-only)."))
            return
        print(_ok("\nStages 2/3 done (stages-only — no aggregate/deploy/prune)."))
        return

    # The window actually covered ends at the last usable day (trailing unpublished
    # days at the leading edge are dropped from the built/deployed window).
    window_end = last_usable if last_usable is not None else end
    # Prune keeps the FULL retention window back from the window end — never the
    # chunk's --start-date. A backfill chunk (--start-date/--end-date over 10 days)
    # means "process these days", not "keep only these days"; using `start` here
    # would delete every day already backfilled before the chunk.
    prune_start = min(start,
                      window_end - datetime.timedelta(days=config.window_days - 1))

    start_tag, end_tag = start.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
    regional = config.regional_dir / f"{alias}_{start_tag}_{end_tag}.parquet"
    out_html = config.maps_dir / f"{alias}_{start_tag}_{end_tag}.html"

    if args.dry_run:
        print(f"  [dry-run] would aggregate → {_rel(regional)}")
        print(f"  [dry-run] would visualize → {_rel(out_html)}")
        if do_deploy:
            print(f"  [dry-run] would deploy: deploy_v2 --publish-as {alias} "
                  f"--source-stem {out_html.stem}")
        if do_prune:
            _prune_window(config, prune_start, bounds, dry_run=True)
        _report_skips()
        print(_ok("\nDry run complete."))
        return

    # --- Stage 4: aggregate the whole window ---
    date_tags = [d.strftime("%Y%m%d") for d in _date_range(start, window_end)]
    print(_stage(f"\nStage 4: aggregate {ARROW} {regional.name}"))
    df = runners.run_stage4(date_tags, lat_min, lat_max, lon_min, lon_max,
                            alias, str(regional))
    _write_regional_provenance(config, regional, bounds, date_tags)
    print(_ok(f"aggregated {len(df):,} events"))

    # --- Stage 5: visualize (PMTiles, stable asset stem for --publish-as) ---
    traffic = args.traffic_tiles or config.traffic_tiles_url
    ff_tiles = (args.traffic_tiles
                if (args.traffic_tiles and not args.traffic_tiles.startswith("http"))
                else config.traffic_tiles_local)
    ff_out = str(config.data_root / "foreflight" /
                 f"{alias}_{start_tag}_{end_tag}.zip")
    print(_stage("\nStage 5: map (PMTiles)"))
    runners.run_stage5(df, str(out_html), pmtiles=True, zoom=args.zoom,
                       traffic_tile_dir=traffic, html_only=False,
                       foreflight_output=ff_out,
                       foreflight_name=config.foreflight_pack_name,
                       foreflight_tiles=ff_tiles,
                       print_summary=False, airport_quality=None,
                       asset_stem=alias)
    print(_ok(f"map written: {out_html.name}"))

    # A window far shorter than the retention target means the fetch found few
    # usable days (mid-backfill, or the source releases lag). Deploying it would
    # replace the live alias with a near-empty map, so make that loud.
    covered = (window_end - start).days + 1
    if do_deploy and covered < config.window_days / 2:
        print(_warn(f"\n⚠ Window covers only {covered} day(s), well short of the "
                    f"{config.window_days}-day target — deploying this would "
                    f"publish a sparse map over '{alias}'."))
        if not args.yes and sys.stdin.isatty():
            if input("  Deploy it anyway? [y/N] ").strip().lower() not in ("y", "yes"):
                print(_warn("Deploy skipped."))
                do_deploy = False

    # --- Deploy under the stable alias ---
    if do_deploy:
        local_tiles = (traffic if (traffic and not traffic.startswith("http"))
                       else None)
        cmd = ["python", "src/tools/deploy_v2",
               "--publish-as", alias, "--source-stem", out_html.stem]
        if local_tiles:
            cmd += ["--traffic-tiles-dir", local_tiles]
        print(_stage(f"\nDeploy: {' '.join(cmd)}"))
        rc = subprocess.run(cmd, cwd=str(_ROOT)).returncode
        if rc != 0:
            raise SystemExit(_fail(f"deploy_v2 failed (rc={rc})"))
        print(_ok("deployed"))

    # --- Prune out-of-window data ---
    if do_prune:
        print(_stage(f"\nPrune: dropping data before {prune_start:%Y%m%d}"))
        if _confirm_prune(config, prune_start, bounds, assume_yes=args.yes):
            _prune_window(config, prune_start, bounds, dry_run=False)
        else:
            print(_warn("Prune skipped."))

    _report_skips()
    print(_ok("\nDone."))


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def cmd_status(config, args) -> None:
    region_label, bounds = _resolve_region(config, args)
    start, end = _resolve_dates(config, args)
    for line in status_mod.report(config, region_label, bounds,
                                  start.strftime("%Y%m%d"), end.strftime("%Y%m%d")):
        print(line)


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------

def _add_common(p, config) -> None:
    p.add_argument("--region", choices=sorted(config.regions),
                   help="Named region from the config")
    p.add_argument("--lat-min", type=int)
    p.add_argument("--lat-max", type=int)
    p.add_argument("--lon-min", type=int)
    p.add_argument("--lon-max", type=int)
    p.add_argument("--start-date", help="YYYYMMDD (default: config dates.default_start)")
    p.add_argument("--end-date", help="YYYYMMDD (default: config dates.default_end)")


def build_parser(config) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli.py", description="v2 LOS pipeline (config-driven)")
    parser.add_argument("--config", help="Path to pipeline_config.yaml override")
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    pr = sub.add_parser("run", help="Run pipeline stages for a region/date range")
    pr.add_argument("profile", nargs="?", choices=sorted(config.profiles),
                    help="Named workflow profile (omit to use --from/--to)")
    pr.add_argument("--from", dest="from_stage", type=int,
                    help="First stage (explicit, no profile)")
    pr.add_argument("--to", dest="to_stage", type=int,
                    help="Last stage (default 5)")
    _add_common(pr, config)
    pr.add_argument("--workers", type=int, help="Override config runtime.workers")
    pr.add_argument("--pmtiles", action="store_true", help="Force PMTiles output")
    pr.add_argument("--no-pmtiles", action="store_true",
                    help="Force self-contained HTML output")
    pr.add_argument("--zoom", type=float, default=None)
    pr.add_argument("--traffic-tiles", help="Traffic tile URL or local path prefix")
    pr.add_argument("--skip-existing", action="store_true",
                    help="Skip cells/dates whose outputs already exist")
    pr.add_argument("--dry-run", action="store_true",
                    help="Report what each stage WOULD write/overwrite/skip "
                         "(honoring --skip-existing) without running anything")
    pr.add_argument("--html-only", action="store_true",
                    help="Stage 5: reuse existing .pmtiles/_tracks (PMTiles only)")
    pr.add_argument("--no-foreflight", action="store_true",
                    help="Stage 5: skip building the ForeFlight Content Pack "
                         "(the tile-packing step is slow)")
    pr.add_argument("--airport-quality", action="store_true",
                    help="Render per-airport ADS-B coverage / runway-usage icons "
                         "on the map (aggregates cached per-day scores from the "
                         "aq/ dir; never computes fresh — use "
                         "`python -m tools.v2_airport_quality --mode compute`)")
    pr.add_argument("--airport-quality-path", default=None,
                    help="Use this pre-built airport_quality JSON verbatim "
                         "instead of aggregating the aq/ cache")
    pr.add_argument("--asset-stem", default=None,
                    help="Bake a stable filename stem (e.g. 'conus') into the "
                         "map's inlined .pmtiles/_tracks refs, for deploy_v2 "
                         "--publish-as. PMTiles mode only")
    pr.set_defaults(func=cmd_run)

    # catchup
    pc = sub.add_parser(
        "catchup",
        help="Fetch missing day(s), rebuild the sliding window, deploy, prune")
    _add_common(pc, config)
    pc.add_argument("--window-days", type=int,
                    help="Sliding window length (override config window.days)")
    pc.add_argument("--rebuild", action="store_true",
                    help="Reprocess every day in the window, not just missing ones")
    pc.add_argument("--stages-only", action="store_true",
                    help="Fetch + stages 2/3 only (skip aggregate/visualize/deploy/"
                         "prune). For chunked initial backfill.")
    pc.add_argument("--skip-existing", action="store_true",
                    help="Skip cells whose stage 2/3 outputs already exist")
    pc.add_argument("--no-deploy", action="store_true",
                    help="Build the window but don't deploy_v2")
    pc.add_argument("--no-prune", action="store_true",
                    help="Don't delete out-of-window data")
    pc.add_argument("--yes", "-y", action="store_true",
                    help="Skip the prune confirmation prompt (for cron/automation)")
    pc.add_argument("--workers", type=int, help="Override config runtime.workers")
    pc.add_argument("--traffic-tiles", help="Traffic tile URL or local path prefix")
    pc.add_argument("--zoom", type=float, default=None)
    pc.add_argument("--dry-run", action="store_true",
                    help="Report the days/artifacts/prune-set without doing anything")
    pc.set_defaults(func=cmd_catchup)

    # status
    ps = sub.add_parser("status", help="Report what's on disk (read-only)")
    _add_common(ps, config)
    ps.set_defaults(func=cmd_status)

    return parser


def main(argv=None) -> None:
    # Two-pass parse: build_parser() bakes the region/profile names into argparse
    # `choices=` from the config, so we must load the config (honoring --config)
    # BEFORE building the real parser. The throwaway pre-parser extracts just
    # --config; parse_known_args ignores everything else so it can't error here.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config")
    known, _ = pre.parse_known_args(argv)
    config = load_config(known.config)

    # Redirect ALL stage reads/writes to this config's data_root before any
    # runner/stage module is imported (cmd_run imports them lazily). Critical
    # when --config points at a test sandbox so real data/v2 is never touched.
    config_mod.set_data_root(config.data_root)

    parser = build_parser(config)
    args = parser.parse_args(argv)
    if getattr(args, "pmtiles", False) and getattr(args, "no_pmtiles", False):
        parser.error("--pmtiles and --no-pmtiles are mutually exclusive")
    args.func(config, args)


if __name__ == "__main__":
    main()

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
import subprocess
import sys
import time
from pathlib import Path

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
    lat_min, lat_max, lon_min, lon_max = bounds
    n_cells = (lat_max - lat_min) * (lon_max - lon_min)
    n_days = (end - start).days + 1

    start_tag, end_tag = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    regional = config.regional_dir / f"{region_label}_{start_tag}_{end_tag}.parquet"
    out_html = config.maps_dir / f"{region_label}_{start_tag}_{end_tag}.html"
    ff_out = str(config.data_root / "foreflight" /
                 f"{region_label}_{start_tag}_{end_tag}.zip")

    print(_stage("v2 LOS Pipeline (cli)"))
    print(f"  Profile/stages: {args.profile or '(explicit)'} {ARROW} {stages}")
    print(f"  Region: {region_label}  lat[{lat_min},{lat_max})×lon[{lon_min},{lon_max})  "
          f"({n_cells} cells/day)")
    print(f"  Dates:  {start_tag}–{end_tag}  ({n_days} day(s))")
    print(f"  Workers: {workers} | PMTiles: {pmtiles}")

    if args.dry_run:
        _print_dry_run(config, args, stages, bounds, start, end,
                       regional, out_html, Path(ff_out), pmtiles)
        return

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
                           foreflight_name=config.foreflight_pack_name,
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
        print(f"  Deploy: python src/tools/deploy_v2 --publish-as conus "
              f"--source-stem {out_html.stem}")
    else:
        print(f"  Open:  file://{out_html.resolve()}")
    if Path(ff_out).exists():
        preview_cmd = f"python src/tools/preview_mbtiles.py --zip '{ff_out}'"
        if local_tiles:
            preview_cmd += f" --traffic-tiles '{local_tiles}'"
        print(f"  ForeFlight: {ff_out}")
        print(f"  Preview:    {preview_cmd}")
    print()
    print(_ok("Done."))


def _print_dry_run(config, args, stages, bounds, start, end,
                   regional, out_html, ff_out, pmtiles) -> None:
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

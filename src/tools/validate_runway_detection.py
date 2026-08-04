"""Validate runway-usage detection against a single airport's ADS-B shards.

Runs the real approach-box / sustained-run algorithm from runway_usage.py over
every low, in-ring track for an airport, prints the resulting per-runway vote
breakdown, and (optionally) plots each track colored by the runway it voted for
with start (o) and end (x) markers, plus the approach boxes and airport.

This is a validation aid: the algorithm's parameters (box length/width,
alignment tolerance, minimum sustained run, approach-split thresholds) were
tuned against several airports with it, and any change to them should be
re-checked here across fields with different runway layouts and traffic
patterns.

Usage:
    python src/tools/validate_runway_detection.py KWVI \\
        --shard-dir tests/fixtures/KWVI --plot out.png

    # Inspect only the tracks that voted for one runway (full tracks drawn):
    python src/tools/validate_runway_detection.py KWVI \\
        --shard-dir tests/fixtures/KWVI --only 27 --plot rwy27.png

Shard files are the same gzipped time-sorted JSONL used elsewhere (named
"<date>_<ICAO>.gz"). Airport coordinates/elevation come from OurAirports via
generate_airport_config.
"""

import argparse
import gzip
import json
import logging
import math
from collections import defaultdict
from pathlib import Path

try:
    from src.tools.generate_airport_config import load_airport
    from src.tools import data_quality as dq
    from src.tools import runway_usage as ru
except ImportError:
    from generate_airport_config import load_airport
    import data_quality as dq
    import runway_usage as ru

logger = logging.getLogger(__name__)

# Distinct colors per runway ident (falls back to a cycle for extra runways).
_COLOR_CYCLE = ["tab:blue", "tab:red", "tab:green", "tab:orange", "tab:purple",
                "tab:brown", "tab:pink", "tab:olive", "tab:cyan", "magenta"]


def load_airport_coords(icao: str) -> tuple[float, float, int]:
    """Return (lat, lon, field_elev_ft) for an ICAO, or raise if unknown."""
    airport = load_airport(icao)
    if not airport:
        raise SystemExit(f"Airport {icao} not found in OurAirports data")
    lat = float(airport.get("latitude_deg") or 0)
    lon = float(airport.get("longitude_deg") or 0)
    elev = int(float(airport.get("elevation_ft") or 0))
    return lat, lon, elev


def read_tracks(shard_dir: Path, icao: str) -> dict[str, list[tuple]]:
    """Read all shards for an airport into per-aircraft point lists.

    Each point is (now, alt_int_or_None, lat, lon), time-sorted per aircraft.
    Mirrors the parsing data_quality does, but keeps every point (the algorithm
    does its own low/in-ring filtering) so we can also draw full tracks.

    Only files matching the "<date>_<ICAO>.gz" convention are read. We do NOT
    fall back to every *.gz in the directory: a missing airport shard used to
    silently pull in unrelated (and possibly multi-GB CONUS) files and hang.
    """
    if not shard_dir.is_dir():
        raise SystemExit(f"Shard directory not found: {shard_dir}")
    shards = sorted(shard_dir.glob(f"*_{icao}.gz"))
    if not shards:
        raise SystemExit(
            f"No shards for {icao} in {shard_dir} "
            f"(expected files named '<date>_{icao}.gz')")

    by_hex: dict[str, list[tuple]] = defaultdict(list)
    for shard in shards:
        try:
            with gzip.open(shard, "rt") as f:
                for line in f:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    lat, lon = r.get("lat"), r.get("lon")
                    if lat is None or lon is None:
                        continue
                    alt = r.get("alt_baro")
                    alt = 0 if alt == "ground" else alt
                    try:
                        alt = int(alt)
                    except (ValueError, TypeError):
                        alt = None
                    hex_id = r.get("hex")
                    ts = r.get("now")
                    if hex_id is None or ts is None:
                        continue
                    by_hex[hex_id].append((ts, alt, lat, lon))
        except (EOFError, OSError) as e:
            logger.warning("Error reading %s: %s (partial)", shard, e)

    for pts in by_hex.values():
        pts.sort()
    return by_hex


def low_in_ring_points(pts: list[tuple], lat: float, lon: float,
                       field_elev: int) -> list[tuple]:
    """The (now, lat, lon) points fed to the runway algorithm for one track.

    Matches analyze_shard_quality: inside the ring and at/below the corridor
    top (≤ field_elev + LOW_ALT_MAX_AGL). Timestamps are kept so the caller can
    split repeated pattern work into separate approaches.
    """
    low_max = field_elev + dq.LOW_ALT_MAX_AGL
    out = []
    for ts, alt, plat, plon in pts:
        if alt is None or alt > low_max:
            continue
        if dq._fast_distance_nm(lat, lon, plat, plon) <= dq.LOW_ALT_RADIUS_NM:
            out.append((ts, plat, plon))
    return out


def classify(by_hex: dict[str, list[tuple]], lat: float, lon: float,
             field_elev: int, boxes: list[dict]) -> dict[str, list[str]]:
    """Return {hex: [runway_votes]} using the real per-approach split algorithm.

    Each hex may yield zero, one, or several votes (pattern work); an empty
    list means the aircraft was tracked low near the field but never
    established on a final.
    """
    result = {}
    for hex_id, pts in by_hex.items():
        app = low_in_ring_points(pts, lat, lon, field_elev)
        if len(app) < 2:
            continue
        result[hex_id] = ru.runway_votes_for_track(app, boxes)
    return result


def print_summary(icao: str, votes: dict[str, list[str]], boxes: list[dict]):
    counts: dict[str, int] = defaultdict(int)
    no_vote = 0
    for rwys in votes.values():
        if not rwys:
            no_vote += 1
        for rwy in rwys:
            counts[rwy] += 1
    total = sum(counts.values())

    print(f"\n=== Runway usage for {icao} ===")
    print(f"Parameters: box_len={ru.APPROACH_BOX_LEN_NM}nm "
          f"width={ru.APPROACH_BOX_WIDTH_FT}ft "
          f"align={ru.RUNWAY_MATCH_TOLERANCE_DEG}deg "
          f"min_run={ru.MIN_FINAL_RUN_NM}nm "
          f"split_gap={ru.APPROACH_SPLIT_GAP_S}s "
          f"cross={ru.CROSS_NEAR_NM}/{ru.CROSS_FAR_NM}nm")
    print(f"Runways: {sorted(b['ident'] for b in boxes)}")
    print(f"Aircraft considered: {len(votes)}  approaches voted: {total}  "
          f"aircraft with no vote: {no_vote}")
    if total == 0:
        print("No runway votes.")
        return
    for rwy, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {rwy:>4}: {n:4d}  ({100 * n / total:5.1f}%)")


def _box_polygon(box: dict) -> tuple[list[float], list[float]]:
    """Lon/lat corner lists tracing a runway's approach box (for plotting)."""
    L = ru.APPROACH_BOX_LEN_NM
    half_w = (ru.APPROACH_BOX_WIDTH_FT / 2.0) / ru.FT_PER_NM
    approach = math.radians((box["heading"] + 180.0) % 360.0)
    axis_n, axis_e = math.cos(approach), math.sin(approach)
    perp_n, perp_e = math.cos(approach + math.pi / 2), math.sin(approach + math.pi / 2)
    coslat = math.cos(math.radians(box["lat"]))
    xs, ys = [], []
    for along, cross in [(0, -half_w), (L, -half_w), (L, half_w),
                         (0, half_w), (0, -half_w)]:
        dn = along * axis_n + cross * perp_n
        de = along * axis_e + cross * perp_e
        ys.append(box["lat"] + dn / ru.NM_PER_DEG_LAT)
        xs.append(box["lon"] + de / ru.NM_PER_DEG_LAT / coslat)
    return xs, ys


def plot(out_path: Path, icao: str, lat: float, lon: float, field_elev: int,
         by_hex: dict[str, list[tuple]], votes: dict[str, list[str]],
         boxes: list[dict], only: str | None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    colors = {b["ident"]: _COLOR_CYCLE[i % len(_COLOR_CYCLE)]
              for i, b in enumerate(sorted(boxes, key=lambda b: b["ident"]))}

    fig, ax = plt.subplots(figsize=(11, 11))
    n_drawn = 0
    for hex_id, rwys in votes.items():
        if only is not None and only not in rwys:
            continue
        # A hex may vote several times (pattern work); color by its first vote.
        rwy = rwys[0] if rwys else None
        pts = by_hex[hex_id]
        if only is not None:
            # Focused view: draw the full track (gray) + low/in-ring (bold).
            fx = [p[3] for p in pts]
            fy = [p[2] for p in pts]
            ax.plot(fx, fy, "-", color="0.8", lw=0.9, zorder=1)
            app = low_in_ring_points(pts, lat, lon, field_elev)
            ax.plot([p[2] for p in app], [p[1] for p in app], "-",
                    color=colors.get(only, "k"), lw=1.8, zorder=2)
            start, end = (fy[0], fx[0]), (fy[-1], fx[-1])
        else:
            # Overview: draw just the low/in-ring points, colored by vote.
            app = low_in_ring_points(pts, lat, lon, field_elev)
            xs = [p[2] for p in app]
            ys = [p[1] for p in app]
            color = colors.get(rwy, "lightgray") if rwy else "lightgray"
            alpha = 0.6 if rwy else 0.3
            ax.plot(xs, ys, "-", color=color, lw=1.0 if rwy else 0.6,
                    alpha=alpha, zorder=2 if rwy else 1)
            start, end = (ys[0], xs[0]), (ys[-1], xs[-1])
        ax.plot(start[1], start[0], "o", color="green", ms=5, zorder=3)
        ax.plot(end[1], end[0], "x", color="black", ms=6, zorder=3)
        n_drawn += 1

    for box in boxes:
        xs, ys = _box_polygon(box)
        ax.plot(xs, ys, color=colors.get(box["ident"], "k"), lw=2, zorder=4)
        ax.annotate(box["ident"], (box["lon"], box["lat"]),
                    color=colors.get(box["ident"], "k"),
                    fontweight="bold", fontsize=12, zorder=5)
    ax.plot(lon, lat, "k*", ms=16, zorder=5)

    title = f"{icao} runway detection — {n_drawn} tracks"
    if only:
        title += f" (only rwy {only}; gray=full track, bold=low/in-ring)"
    ax.set_title(title)
    ax.set_aspect(1.0 / math.cos(math.radians(lat)))

    legend = [Line2D([0], [0], color=c, label=k) for k, c in sorted(colors.items())]
    if only is None:
        legend.append(Line2D([0], [0], color="lightgray", label="no vote"))
    legend += [Line2D([0], [0], marker="o", color="green", ls="", label="start"),
               Line2D([0], [0], marker="x", color="black", ls="", label="end")]
    ax.legend(handles=legend, fontsize=9)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=110, bbox_inches="tight")
    print(f"Saved plot: {out_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("icao", help="Airport ICAO/local code (e.g. KWVI)")
    ap.add_argument("--shard-dir", type=Path, required=True,
                    help="Directory of <date>_<ICAO>.gz shard files")
    ap.add_argument("--plot", type=Path, default=None,
                    help="Write a track plot to this PNG path")
    ap.add_argument("--only", default=None,
                    help="Plot only tracks voting for this runway ident "
                         "(e.g. 27); draws their full tracks for inspection")
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING)

    lat, lon, field_elev = load_airport_coords(args.icao)
    boxes = ru.build_runway_boxes(args.icao)
    if not boxes:
        raise SystemExit(f"No runway data for {args.icao} in OurAirports")

    # Catch the common "--only 7" (vs "07") mistake before drawing an empty
    # plot: the ident must match a real runway at this airport.
    if args.only is not None and args.only not in {b["ident"] for b in boxes}:
        valid = ", ".join(sorted(b["ident"] for b in boxes))
        raise SystemExit(f"--only {args.only!r} is not a runway at {args.icao}"
                         f" (runways: {valid})")

    by_hex = read_tracks(args.shard_dir, args.icao)
    votes = classify(by_hex, lat, lon, field_elev, boxes)
    print_summary(args.icao, votes, boxes)

    if args.plot:
        plot(args.plot, args.icao, lat, lon, field_elev, by_hex, votes,
             boxes, args.only)


if __name__ == "__main__":
    main()

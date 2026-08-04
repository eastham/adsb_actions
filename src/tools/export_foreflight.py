#!/usr/bin/env python3
"""Export traffic density tiles and LOS event heatmap as a ForeFlight Content Pack.

Produces a .zip file containing two MBTiles raster layers that can be imported
directly into ForeFlight via AirDrop, email, or download link.

Usage:
    python src/tools/export_foreflight.py \
        --traffic-tiles tiles/traffic \
        --events data/v2/regional/CONUS_20260101_20260131.parquet \
        --output data/v2/foreflight/CONUS_Jan2026.zip \
        --name "CONUS Safety Layers" \
        --org "ADS-B Safety Research" \
        --version 1

Either --traffic-tiles or --events may be omitted to produce a single-layer pack.

MBTiles georeferencing notes:
  - Tile coordinates (z/x/y) encode geography implicitly in Web Mercator (EPSG:3857).
  - MBTiles uses TMS row convention: tile_row = (2^zoom - 1) - slippy_y.
  - The 'bounds' metadata field (minlon,minlat,maxlon,maxlat) tells ForeFlight
    the coverage extent; tiles outside it are not requested.
"""

import argparse
import io
import json
import math
import os
import re
import sqlite3
import sys
import zipfile
from pathlib import Path

from PIL import Image

# CONUS bounding box — matches traffic_tiles.py constants
CONUS_BOUNDS = (-125.0, 24.5, -66.0, 49.5)  # minlon, minlat, maxlon, maxlat

# LOS heatmap zoom range (traffic tiles use whatever zoom levels exist on disk)
LOS_MIN_ZOOM = 7
LOS_MAX_ZOOM = 11

# Circle radius in pixels at each zoom level. Originally sized to ~1/8 mile on
# the ground (z7=1, z8=1, z9=2, z10=3, z11=6); reduced by ~1/3 for readability
# so dense clusters don't merge into blobs. Sub-pixel radii are fine — the
# SUPERSAMPLE pass renders them at 4x before downsampling.
LOS_RADIUS_PX = {7: 0.67, 8: 0.67, 9: 1.33, 10: 2.0, 11: 4.0}

# Supersampling factor: render at this multiple then downsample for smoother circles.
SUPERSAMPLE = 4

# Quality → color: vhigh=red, high=orange, medium=yellow. 'low' excluded.
QUALITY_COLOR = {
    "vhigh": (200,   0,   0, 255),  # deep red
    "high":  (255, 100,   0, 255),  # orange
    "medium":(255, 220,   0, 255),  # yellow
}

# Tile size (must match traffic_tiles.py)
TILE_SIZE = 256


# ---------------------------------------------------------------------------
# Web Mercator tile math (mirrors traffic_tiles.py to avoid importing it)
# ---------------------------------------------------------------------------

def latlon_to_tile_pixel(lat, lon, zoom):
    """Return (tile_x, tile_y, pixel_x, pixel_y) for a lat/lon at zoom."""
    n = 2 ** zoom
    tx_float = (lon + 180.0) / 360.0 * n
    lat_rad = math.radians(lat)
    ty_float = (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad))
                / math.pi) / 2.0 * n
    tx = int(tx_float)
    ty = int(ty_float)
    px = max(0, min(TILE_SIZE - 1, int((tx_float - tx) * TILE_SIZE)))
    py = max(0, min(TILE_SIZE - 1, int((ty_float - ty) * TILE_SIZE)))
    return tx, ty, px, py


def tile_latlon_bounds(tx, ty, zoom):
    """Return (sw_lat, sw_lon, ne_lat, ne_lon) for a slippy-map tile."""
    n = 2 ** zoom
    sw_lon = tx / n * 360.0 - 180.0
    ne_lon = (tx + 1) / n * 360.0 - 180.0
    ne_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty / n))))
    sw_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1) / n))))
    return sw_lat, sw_lon, ne_lat, ne_lon


def slippy_to_tms(ty, zoom):
    """Convert slippy-map y to TMS y (MBTiles row convention)."""
    return (2 ** zoom - 1) - ty


def conus_tile_range(zoom):
    """Return (tx_min, tx_max, ty_min, ty_max) of slippy tiles covering CONUS."""
    minlon, minlat, maxlon, maxlat = CONUS_BOUNDS
    tx_min, ty_max, _, _ = latlon_to_tile_pixel(minlat, minlon, zoom)
    tx_max, ty_min, _, _ = latlon_to_tile_pixel(maxlat, maxlon, zoom)
    return tx_min, tx_max, ty_min, ty_max


# ---------------------------------------------------------------------------
# MBTiles helpers
# ---------------------------------------------------------------------------

def open_mbtiles(path, name, fmt="png", layer_type="overlay",
                 bounds=CONUS_BOUNDS, minzoom=5, maxzoom=11):
    """Create a new MBTiles SQLite file and return the connection."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE metadata (name TEXT, value TEXT);"
    )
    conn.execute(
        "CREATE TABLE tiles "
        "(zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB, "
        "PRIMARY KEY (zoom_level, tile_column, tile_row));"
    )
    minlon, minlat, maxlon, maxlat = bounds
    meta = [
        ("name",    name),
        ("format",  fmt),
        ("type",    layer_type),
        ("bounds",  f"{minlon},{minlat},{maxlon},{maxlat}"),
        ("minzoom", str(minzoom)),
        ("maxzoom", str(maxzoom)),
    ]
    conn.executemany("INSERT INTO metadata VALUES (?,?)", meta)
    conn.commit()
    return conn


def insert_tile(conn, zoom, tx, ty_slippy, png_bytes):
    """Insert one PNG tile; converts slippy y → TMS row."""
    conn.execute(
        "INSERT OR REPLACE INTO tiles VALUES (?,?,?,?)",
        (zoom, tx, slippy_to_tms(ty_slippy, zoom), png_bytes),
    )


def png_bytes(img: Image.Image, optimize: bool = False) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=optimize)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Layer 1: Traffic tiles (pack existing PNG tree)
# ---------------------------------------------------------------------------

TRAFFIC_MAX_ZOOM = 10  # zoom 11 adds ~25k tiles for minimal visual benefit in ForeFlight
# Alpha threshold: pixels below this are zeroed out, keeping only the busiest regions.
# p75 of the alpha distribution across tiles; removes sparse light-blue noise.
TRAFFIC_ALPHA_THRESHOLD = 55

# Single flat color for all surviving traffic pixels (transparent-background
# overlay for ForeFlight). Purple matches the high-density end of the source
# density ramp in traffic_tiles.band_to_color().
TRAFFIC_COLOR = (128, 0, 255)

# Round alpha to this many distinct levels. With one flat RGB color, PNG size is
# driven almost entirely by the number of distinct RGBA values; collapsing the
# 256-level alpha gradient to 8 buckets shrinks tiles ~2.5x while the
# busier-is-more-opaque gradient stays visually intact.
TRAFFIC_ALPHA_LEVELS = 8

def threshold_traffic_tile(img: Image.Image) -> Image.Image | None:
    """Keep only high-density pixels, recolor to a single flat color, keep the
    source alpha as a transparent-background overlay.

    The source tile encodes density in both color and alpha; we discard the
    color ramp and paint every surviving pixel TRAFFIC_COLOR, preserving the
    source alpha so busier pixels stay more opaque. Returns an RGBA image, or
    None if no pixels survive the threshold.
    """
    import numpy as np
    arr = np.array(img.convert("RGBA"))
    alpha = arr[:, :, 3].copy()
    alpha[alpha < TRAFFIC_ALPHA_THRESHOLD] = 0
    if alpha.max() == 0:
        return None
    # Bucket alpha to a few levels so the PNG has few distinct RGBA values and
    # compresses hard; surviving pixels keep a nonzero alpha (never rounded to 0).
    step = 256 // TRAFFIC_ALPHA_LEVELS
    quantized = ((alpha.astype(np.uint16) // step) * step + step - 1).clip(0, 255)
    alpha = np.where(alpha > 0, quantized, 0).astype(np.uint8)
    out = np.zeros_like(arr)
    keep = alpha > 0
    out[keep, 0] = TRAFFIC_COLOR[0]
    out[keep, 1] = TRAFFIC_COLOR[1]
    out[keep, 2] = TRAFFIC_COLOR[2]
    out[:, :, 3] = alpha
    return Image.fromarray(out, "RGBA")


def pack_traffic_tiles(tile_dir: Path, conn: sqlite3.Connection) -> int:
    """Walk the slippy-map tile tree and insert all PNGs into MBTiles."""
    total = 0
    zoom_dirs = sorted(
        (d for d in tile_dir.iterdir() if d.is_dir() and d.name.isdigit()
         and int(d.name) <= TRAFFIC_MAX_ZOOM),
        key=lambda d: int(d.name),
    )
    for zoom_dir in zoom_dirs:
        zoom = int(zoom_dir.name)
        for x_dir in sorted(zoom_dir.iterdir(), key=lambda d: int(d.name)):
            if not x_dir.is_dir():
                continue
            tx = int(x_dir.name)
            for tile_file in sorted(x_dir.glob("*.png"), key=lambda f: int(f.stem)):
                ty = int(tile_file.stem)
                img = threshold_traffic_tile(Image.open(tile_file))
                if img is not None:
                    insert_tile(conn, zoom, tx, ty, png_bytes(img, optimize=True))
                total += 1
    conn.commit()
    return total


# ---------------------------------------------------------------------------
# Layer 2: LOS event heatmap (rasterize event points to PNG tiles)
# ---------------------------------------------------------------------------


def render_los_tile(events_in_tile, radius_px):
    """Render LOS events as opaque filled circles, color-coded by quality.

    events_in_tile: list of (px, py, rgba) where px/py may be outside [0, TILE_SIZE).
    Renders at SUPERSAMPLE× resolution then downsamples for smooth antialiased edges.
    Returns RGBA PIL Image or None if no events touch this tile.
    """
    if not events_in_tile:
        return None

    from PIL import ImageDraw

    s = SUPERSAMPLE
    r = radius_px * s  # may be fractional; the ellipse honors the sub-pixel radius
    # Padded canvas so circles near tile edges aren't clipped differently on
    # adjacent tiles — paint at shifted coords, then crop back to TILE_SIZE.
    # pad must be an int (image dims / crop offsets); round up to fully contain r.
    pad = math.ceil(r)
    canvas = TILE_SIZE * s + 2 * pad
    img = Image.new("RGBA", (canvas, canvas), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    for px, py, color in events_in_tile:
        cx, cy = px * s + pad, py * s + pad
        draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=color)

    tile_hires = img.crop((pad, pad, pad + TILE_SIZE * s, pad + TILE_SIZE * s))
    return tile_hires.resize((TILE_SIZE, TILE_SIZE), Image.LANCZOS) if tile_hires.getbbox() else None


def pack_los_heatmap(df, conn: sqlite3.Connection) -> int:
    """Rasterize LOS events from a DataFrame into MBTiles tiles."""
    import numpy as np

    # Keep only quality levels with a defined color
    df = df[df["quality"].isin(QUALITY_COLOR)].copy()
    lats = df["lat"].to_numpy(dtype=np.float64)
    lons = df["lon"].to_numpy(dtype=np.float64)
    colors = [QUALITY_COLOR[q] for q in df["quality"]]

    total = 0
    for zoom in range(LOS_MIN_ZOOM, LOS_MAX_ZOOM + 1):
        radius_px = LOS_RADIUS_PX[zoom]
        tx_min, tx_max, ty_min, ty_max = conus_tile_range(zoom)

        # Map every event to its home tile, then spread to neighbors whose
        # boundary is within radius_px so circles aren't clipped at tile edges.
        padded: dict[tuple, list] = {}
        for i in range(len(lats)):
            etx, ety, epx, epy = latlon_to_tile_pixel(lats[i], lons[i], zoom)
            global_px = etx * TILE_SIZE + epx
            global_py = ety * TILE_SIZE + epy
            color = colors[i]
            for dtx in range(-1, 2):
                for dty in range(-1, 2):
                    ntx, nty = etx + dtx, ety + dty
                    if tx_min <= ntx <= tx_max and ty_min <= nty <= ty_max:
                        tile_px = global_px - ntx * TILE_SIZE
                        tile_py = global_py - nty * TILE_SIZE
                        if (-radius_px <= tile_px <= TILE_SIZE + radius_px and
                                -radius_px <= tile_py <= TILE_SIZE + radius_px):
                            padded.setdefault((ntx, nty), []).append(
                                (tile_px, tile_py, color))

        n_tiles = len(padded)
        print(f"  zoom {zoom}: {n_tiles} tiles, {len(lats)} events", flush=True)

        for (tx, ty), pts in padded.items():
            img = render_los_tile(pts, radius_px)
            if img is not None:
                insert_tile(conn, zoom, tx, ty, png_bytes(img))
                total += 1

    conn.commit()
    return total


# ---------------------------------------------------------------------------
# Content pack zip assembly
# ---------------------------------------------------------------------------

def build_content_pack(mbtiles_files: list[tuple[str, Path]],
                       output_zip: Path, pack_name: str,
                       org: str = "", version: int = 1):
    """Zip mbtiles_files into a ForeFlight Content Pack.

    mbtiles_files: list of (layer_display_name, path_to_mbtiles)
    The mbtiles files are NOT deleted — callers are responsible for cleanup.
    """
    output_zip.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "name": pack_name,
        "abbreviation": "".join(w[0].upper() for w in pack_name.split()[:4]),
        "version": version,
        "organizationName": org,
    }
    # Top-level folder name in zip = pack display name. Sanitize first: a name
    # carrying a date range ("... 6/25-8/25") would otherwise have its slashes
    # read as path separators, nesting manifest.json several dirs deep instead
    # of at the top of one flat pack folder. The manifest's own "name" keeps the
    # unsanitized text — it's JSON, not a path.
    folder = re.sub(r"[/\\]", "-", pack_name).strip() or "Content Pack"
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{folder}/manifest.json",
                    json.dumps(manifest, indent=2))
        for display_name, mbtiles_path in mbtiles_files:
            arcname = f"{folder}/layers/{display_name}.mbtiles"
            zf.write(mbtiles_path, arcname)
    print(f"  ForeFlight pack: {output_zip}  ({output_zip.stat().st_size / 1e6:.1f} MB)")


def export_pack(
    output_zip: Path,
    pack_name: str = "CONUS Safety Layers",
    org: str = "ADS-B Safety Research",
    version: int = 1,
    traffic_tile_dir: Path | None = None,
    events_df=None,
) -> list[Path]:
    """Build a ForeFlight Content Pack and return the kept .mbtiles paths.

    Called from pipeline.py after Stage 5. Returns a list of absolute .mbtiles
    paths that are kept on disk alongside the zip for preview.
    Caller is responsible for cleanup if desired.

    traffic_tile_dir: local slippy-map tile tree root (None → skip traffic layer)
    events_df: pandas DataFrame with lat/lon/quality columns (None → skip LOS layer)
    """
    tmp_dir = output_zip.parent / ".foreflight_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    mbtiles_files = []

    if traffic_tile_dir is not None:
        tile_dir = Path(traffic_tile_dir)
        zoom_levels = sorted(
            int(d.name) for d in tile_dir.iterdir()
            if d.is_dir() and d.name.isdigit() and int(d.name) <= TRAFFIC_MAX_ZOOM
        )
        if zoom_levels:
            out_path = tmp_dir / "Traffic Density.mbtiles"
            print(f"  ForeFlight: packing traffic tiles (zoom {zoom_levels[0]}–{zoom_levels[-1]})…")
            conn = open_mbtiles(out_path, "Traffic Density", fmt="png",
                                minzoom=zoom_levels[0], maxzoom=zoom_levels[-1])
            n = pack_traffic_tiles(tile_dir, conn)
            conn.close()
            print(f"    {n} traffic tiles written")
            mbtiles_files.append(("Traffic Density", out_path))

    if events_df is not None and not events_df.empty:
        out_path = tmp_dir / "LOS Events.mbtiles"
        conn = open_mbtiles(out_path, "LOS Events",
                            minzoom=LOS_MIN_ZOOM, maxzoom=LOS_MAX_ZOOM)
        print(f"  ForeFlight: rasterizing LOS heatmap (zoom {LOS_MIN_ZOOM}–{LOS_MAX_ZOOM})…")
        n = pack_los_heatmap(events_df, conn)
        conn.close()
        print(f"    {n} LOS tiles written")
        mbtiles_files.append(("LOS Events", out_path))

    if mbtiles_files:
        print(f"  ForeFlight: assembling '{pack_name}'…")
        build_content_pack(mbtiles_files, output_zip, pack_name, org=org, version=version)

    return [p.resolve() for _, p in mbtiles_files]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--traffic-tiles", metavar="DIR",
                    help="Slippy-map tile tree root (tiles/traffic/)")
    ap.add_argument("--events", metavar="PARQUET",
                    help="Stage-4 regional parquet with lat/lon/quality columns")
    ap.add_argument("--output", required=True, metavar="ZIP",
                    help="Output .zip path for the ForeFlight Content Pack")
    ap.add_argument("--name", default="CONUS Safety Layers",
                    help="Content pack display name (default: 'CONUS Safety Layers')")
    ap.add_argument("--org", default="ADS-B Safety Research",
                    help="Organization name shown in ForeFlight pack details")
    ap.add_argument("--version", type=int, default=1,
                    help="Pack version integer (default: 1)")
    args = ap.parse_args()

    if not args.traffic_tiles and not args.events:
        ap.error("At least one of --traffic-tiles or --events is required.")

    output_zip = Path(args.output)

    df = None
    if args.events:
        import pandas as pd
        events_path = Path(args.events)
        if not events_path.exists():
            print(f"ERROR: --events file not found: {events_path}", file=sys.stderr)
            sys.exit(1)
        print(f"Loading events from {events_path.name}…")
        df = pd.read_parquet(events_path, columns=["lat", "lon", "quality"])
        df = df.dropna(subset=["lat", "lon", "quality"])
        print(f"  {len(df)} events loaded")

    tile_dir = Path(args.traffic_tiles) if args.traffic_tiles else None
    if tile_dir and not tile_dir.is_dir():
        print(f"ERROR: --traffic-tiles dir not found: {tile_dir}", file=sys.stderr)
        sys.exit(1)

    kept = export_pack(output_zip, pack_name=args.name, org=args.org,
                       version=args.version, traffic_tile_dir=tile_dir,
                       events_df=df)

    # Preview from the zip, not the temp mbtiles — those are deleted just below.
    # preview_mbtiles.py uses MapLibre + OSM, so unlike mbview it needs no
    # Mapbox token, and it shows both layers on one map.
    if kept:
        print(f"\nPreview:\n  python src/tools/preview_mbtiles.py --zip '{output_zip}'")

    # Clean up temp mbtiles
    tmp_dir = output_zip.parent / ".foreflight_tmp"
    for p in kept:
        p.unlink(missing_ok=True)
    try:
        tmp_dir.rmdir()
    except OSError:
        pass


if __name__ == "__main__":
    main()

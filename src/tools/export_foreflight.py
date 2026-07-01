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
import sqlite3
import sys
import zipfile
from pathlib import Path

from PIL import Image, ImageFilter

# CONUS bounding box — matches traffic_tiles.py constants
CONUS_BOUNDS = (-125.0, 24.5, -66.0, 49.5)  # minlon, minlat, maxlon, maxlat

# LOS heatmap zoom range (traffic tiles use whatever zoom levels exist on disk)
LOS_MIN_ZOOM = 7
LOS_MAX_ZOOM = 11

# Heatmap blob radius in pixels at each zoom level (doubles per zoom, matching
# the MapLibre heatmap-radius in stage5_visualize.py: z8→20, z9→40, z10→80…)
# We start at z7 with 2px and double upward.
LOS_RADIUS_PX = {7: 2, 8: 4, 9: 8, 10: 16, 11: 32}

# Quality weights — higher = more opaque blob. 'low' excluded (matches browser).
QUALITY_WEIGHT = {"vhigh": 1.0, "high": 0.7, "medium": 0.4}

# LOS heatmap color: yellow → orange → deep red (warm, distinct from traffic blue)
# Interpolated by per-pixel accumulated weight [0..1].
LOS_COLOR_LOW  = (255, 220,   0)   # yellow  (sparse)
LOS_COLOR_MID  = (255, 100,   0)   # orange
LOS_COLOR_HIGH = (200,   0,   0)   # deep red (dense)

# Max alpha for LOS tiles (keeps aeronautical chart readable underneath)
LOS_MAX_ALPHA = 180

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


def png_bytes(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Layer 1: Traffic tiles (pack existing PNG tree)
# ---------------------------------------------------------------------------

def pack_traffic_tiles(tile_dir: Path, conn: sqlite3.Connection) -> int:
    """Walk the slippy-map tile tree and insert all PNGs into MBTiles."""
    total = 0
    zoom_dirs = sorted(
        (d for d in tile_dir.iterdir() if d.is_dir() and d.name.isdigit()),
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
                insert_tile(conn, zoom, tx, ty, tile_file.read_bytes())
                total += 1
    conn.commit()
    return total


# ---------------------------------------------------------------------------
# Layer 2: LOS event heatmap (rasterize event points to PNG tiles)
# ---------------------------------------------------------------------------

def _lerp_color(c1, c2, t):
    return tuple(int(a + (b - a) * t) for a, b in zip(c1, c2))


def _weight_to_color_alpha(w_norm):
    """Map normalized weight [0..1] to (R,G,B,A)."""
    if w_norm <= 0.5:
        rgb = _lerp_color(LOS_COLOR_LOW, LOS_COLOR_MID, w_norm * 2)
    else:
        rgb = _lerp_color(LOS_COLOR_MID, LOS_COLOR_HIGH, (w_norm - 0.5) * 2)
    alpha = int(LOS_MAX_ALPHA * w_norm)
    return rgb + (alpha,)


def render_los_tile(events_in_tile, zoom, tx, ty, radius_px):
    """Render one LOS heatmap tile from a list of (px, py, weight) tuples.

    Uses a Gaussian blur over weight-painted pixels to produce smooth blobs.
    Returns RGBA PIL Image or None if no events.
    """
    if not events_in_tile:
        return None

    import numpy as np

    # Accumulate weights on a float32 canvas (larger to accommodate blob radius)
    pad = radius_px
    canvas_size = TILE_SIZE + 2 * pad
    weight_canvas = np.zeros((canvas_size, canvas_size), dtype=np.float32)

    for px, py, weight in events_in_tile:
        # Place weight at padded pixel
        cpx, cpy = px + pad, py + pad
        # Paint a circular weight blob using a distance-falloff mask
        y_min = max(0, cpy - radius_px)
        y_max = min(canvas_size, cpy + radius_px + 1)
        x_min = max(0, cpx - radius_px)
        x_max = min(canvas_size, cpx + radius_px + 1)
        ys = np.arange(y_min, y_max)[:, None]
        xs = np.arange(x_min, x_max)[None, :]
        dist2 = (ys - cpy) ** 2 + (xs - cpx) ** 2
        r2 = radius_px ** 2
        mask = dist2 <= r2
        # Gaussian falloff within circle
        falloff = np.exp(-dist2 / (2 * (radius_px / 2.5) ** 2)) * mask
        weight_canvas[y_min:y_max, x_min:x_max] += falloff * weight

    # Crop to tile area (strip padding)
    tile_weights = weight_canvas[pad:pad + TILE_SIZE, pad:pad + TILE_SIZE]

    max_w = tile_weights.max()
    if max_w == 0:
        return None

    # Normalize and map to RGBA
    norm = np.clip(tile_weights / max_w, 0, 1)
    rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    for row in range(TILE_SIZE):
        for col in range(TILE_SIZE):
            w = norm[row, col]
            if w > 0.01:
                rgba[row, col] = _weight_to_color_alpha(float(w))

    return Image.fromarray(rgba, mode="RGBA")


def render_los_tile_fast(events_in_tile, zoom, tx, ty, radius_px):
    """Vectorized version using PIL GaussianBlur — faster for dense tiles."""
    if not events_in_tile:
        return None

    import numpy as np

    # Paint weight points onto a float canvas, then blur
    weight_img = Image.new("F", (TILE_SIZE, TILE_SIZE), 0.0)
    from PIL import ImageDraw
    draw = ImageDraw.Draw(weight_img)
    for px, py, weight in events_in_tile:
        # Small dot; blur will expand it
        r = max(1, radius_px // 8)
        draw.ellipse([px - r, py - r, px + r, py + r], fill=weight)

    # Blur to create smooth blob (GaussianBlur requires "L" mode, not "F")
    sigma = radius_px / 2.5
    raw = np.array(weight_img, dtype=np.float32)
    raw_norm = np.clip(raw / max(raw.max(), 1e-6), 0, 1)
    weight_l = Image.fromarray((raw_norm * 255).astype(np.uint8), mode="L")
    blurred_l = weight_l.filter(ImageFilter.GaussianBlur(radius=sigma))
    w_arr = np.array(blurred_l, dtype=np.float32) / 255.0

    max_w = w_arr.max()
    if max_w < 1e-6:
        return None

    norm = np.clip(w_arr / max_w, 0, 1)
    rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)

    # Vectorized color mapping (piecewise linear between 3 anchor colors)
    lo = np.array(LOS_COLOR_LOW,  dtype=np.float32)
    mi = np.array(LOS_COLOR_MID,  dtype=np.float32)
    hi = np.array(LOS_COLOR_HIGH, dtype=np.float32)

    lower_half = norm <= 0.5
    t_lo = np.clip(norm * 2, 0, 1)
    t_hi = np.clip((norm - 0.5) * 2, 0, 1)

    rgb = np.where(
        lower_half[:, :, None],
        lo + (mi - lo) * t_lo[:, :, None],
        mi + (hi - mi) * t_hi[:, :, None],
    ).astype(np.uint8)

    alpha = (norm * LOS_MAX_ALPHA).astype(np.uint8)
    mask = norm > 0.01
    rgba[:, :, :3] = rgb
    rgba[:, :, 3] = np.where(mask, alpha, 0)

    return Image.fromarray(rgba, mode="RGBA")


def pack_los_heatmap(df, conn: sqlite3.Connection) -> int:
    """Rasterize LOS events from a DataFrame into MBTiles tiles."""
    import numpy as np

    # Keep only quality levels that appear in the browser heatmap
    df = df[df["quality"].isin(QUALITY_WEIGHT)].copy()
    df["weight"] = df["quality"].map(QUALITY_WEIGHT)
    lats = df["lat"].to_numpy(dtype=np.float64)
    lons = df["lon"].to_numpy(dtype=np.float64)
    weights = df["weight"].to_numpy(dtype=np.float64)

    total = 0
    for zoom in range(LOS_MIN_ZOOM, LOS_MAX_ZOOM + 1):
        radius_px = LOS_RADIUS_PX[zoom]
        tx_min, tx_max, ty_min, ty_max = conus_tile_range(zoom)

        # Map every event to its tile + pixel at this zoom
        event_tiles: dict[tuple, list] = {}
        for i in range(len(lats)):
            etx, ety, epx, epy = latlon_to_tile_pixel(lats[i], lons[i], zoom)
            if tx_min <= etx <= tx_max and ty_min <= ety <= ty_max:
                key = (etx, ety)
                event_tiles.setdefault(key, []).append((epx, epy, weights[i]))

        # For each occupied tile, also include events from neighboring tiles whose
        # blobs may bleed in (within radius_px of the tile edge)
        # Build a quick lookup: tile → events that touch it (including neighbors)
        padded: dict[tuple, list] = {}
        # Degrees-per-pixel at this zoom (approximate at mid-CONUS lat ~37°N)
        deg_per_px = 360.0 / (TILE_SIZE * 2 ** zoom)
        pad_deg = radius_px * deg_per_px * 1.5  # generous padding

        for i in range(len(lats)):
            etx, ety, epx, epy = latlon_to_tile_pixel(lats[i], lons[i], zoom)
            # Find which tiles this event's blob could touch
            lat, lon, w = lats[i], lons[i], weights[i]
            for dtx in range(-1, 2):
                for dty in range(-1, 2):
                    ntx, nty = etx + dtx, ety + dty
                    if tx_min <= ntx <= tx_max and ty_min <= nty <= ty_max:
                        # Compute pixel position relative to neighbor tile
                        ntx2, nty2, npx, npy = latlon_to_tile_pixel(lat, lon, zoom)
                        # Pixel offset: event pixel in global pixel space
                        global_px = etx * TILE_SIZE + epx
                        global_py = ety * TILE_SIZE + epy
                        tile_px = global_px - ntx * TILE_SIZE
                        tile_py = global_py - nty * TILE_SIZE
                        # Only include if blob could reach this tile
                        if (-radius_px <= tile_px <= TILE_SIZE + radius_px and
                                -radius_px <= tile_py <= TILE_SIZE + radius_px):
                            padded.setdefault((ntx, nty), []).append(
                                (tile_px, tile_py, w))

        n_tiles = len(padded)
        print(f"  zoom {zoom}: {n_tiles} tiles, {len(lats)} events", flush=True)

        for (tx, ty), pts in padded.items():
            img = render_los_tile_fast(pts, zoom, tx, ty, radius_px)
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
    folder = pack_name  # top-level folder name in zip = pack display name
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
    paths that are kept on disk alongside the zip for mbview preview.
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
            if d.is_dir() and d.name.isdigit()
        )
        if zoom_levels:
            out_path = tmp_dir / "Traffic Density.mbtiles"
            print(f"  ForeFlight: packing traffic tiles (zoom {zoom_levels[0]}–{zoom_levels[-1]})…")
            conn = open_mbtiles(out_path, "Traffic Density",
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

    if kept:
        print("\nPreview with mbview (install: npm install -g mbview):")
        for p in kept:
            print(f"  mbview '{p}'")

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

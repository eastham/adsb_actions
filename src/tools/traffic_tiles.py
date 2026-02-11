#!/usr/bin/env python3
"""Generate traffic track tiles from global ADS-B data.

Reads global gzipped JSONL files, reconstructs aircraft tracks, and renders
altitude-colored density tiles as 256x256 PNGs in slippy-map layout
({z}/{x}/{y}.png) for use as a folium ImageOverlay layer.

Usage:
    python src/tools/traffic_tiles.py --data-dir data/ --output-dir tiles/traffic
"""

import argparse
import csv
import gzip
import json
import logging
import math
import random
import shutil
import sys
import time
from collections import defaultdict, OrderedDict
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

INPUT_DATA_PREFIX = "KMOD_100nm_"  # Prefix for global sorted JSONL input files from convert_traces.py  


try:
    from src.tools.batch_helpers import FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT
    from src.tools.generate_airport_config import download_with_cache, AIRPORTS_URL
except ImportError:
    from batch_helpers import FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT
    from generate_airport_config import download_with_cache, AIRPORTS_URL

logger = logging.getLogger(__name__)

# Track continuity: gap > this starts a new track
MAX_GAP_SECONDS = 120

# Altitude color bands
NUM_BANDS = 10

# Default zoom level (~160ft/pixel at 40°N)
DEFAULT_ZOOM = 11

DENSITY_FOR_FULL_BRIGHTNESS = 20  # adjust this: lower = brighter single tracks
COLOR_VIBRANCY = 0.9  # adjust this: 1.0 = full brightness, <1.0 = dimmer, >1.0 = brighter
TRACK_WIDTH = 3  # pixels - adjust this for thicker or thinner track lines

# --- Tile coordinate math (standard Web Mercator) ---

def latlon_to_tile_pixel(lat, lon, zoom):
    """Convert lat/lon to tile index and pixel within that tile.

    Returns (tile_x, tile_y, pixel_x, pixel_y).
    """
    n = 2 ** zoom
    tx_float = (lon + 180.0) / 360.0 * n
    lat_rad = math.radians(lat)
    ty_float = (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad))
                / math.pi) / 2.0 * n

    tile_x = int(tx_float)
    tile_y = int(ty_float)
    pixel_x = int((tx_float - tile_x) * 256)
    pixel_y = int((ty_float - tile_y) * 256)

    # Clamp to valid range
    pixel_x = max(0, min(255, pixel_x))
    pixel_y = max(0, min(255, pixel_y))
    return tile_x, tile_y, pixel_x, pixel_y


def tile_to_latlon_bounds(tx, ty, zoom):
    """Convert tile indices to geographic bounds.

    Returns [[sw_lat, sw_lon], [ne_lat, ne_lon]] suitable for folium
    ImageOverlay bounds parameter.
    """
    n = 2 ** zoom
    # West and east longitudes
    sw_lon = tx / n * 360.0 - 180.0
    ne_lon = (tx + 1) / n * 360.0 - 180.0
    # North and south latitudes (tile y=0 is top/north)
    ne_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty / n))))
    sw_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1) / n))))
    return [[sw_lat, sw_lon], [ne_lat, ne_lon]]


def tile_center(tx, ty, zoom):
    """Return (lat, lon) of tile center."""
    bounds = tile_to_latlon_bounds(tx, ty, zoom)
    lat = (bounds[0][0] + bounds[1][0]) / 2
    lon = (bounds[0][1] + bounds[1][1]) / 2
    return lat, lon


# --- Airport elevation lookup ---

def load_all_airports():
    """Load all airports with elevation from OurAirports CSV.

    Returns numpy array of shape (N, 3) with columns [lat, lon, elev_ft].
    Only includes small/medium/large airports (not helipads, closed, etc.).
    """
    airports_path = download_with_cache(AIRPORTS_URL, "airports.csv")
    rows = []
    with open(airports_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            atype = row.get('type', '').strip('"')
            if atype not in ('small_airport', 'medium_airport', 'large_airport'):
                continue
            try:
                lat = float(row['latitude_deg'])
                lon = float(row['longitude_deg'])
                elev = float(row.get('elevation_ft') or 0)
            except (ValueError, TypeError):
                continue
            rows.append((lat, lon, elev))
    return np.array(rows, dtype=np.float64)


def get_tile_field_elev(tx, ty, zoom, airports, cache):
    """Get field elevation for a tile from nearest airport.

    Uses simple Euclidean distance on lat/lon (adequate for nearest-neighbor
    over ~70K airports). Results are cached in the provided dict.
    """
    key = (tx, ty)
    if key in cache:
        return cache[key]

    lat, lon = tile_center(tx, ty, zoom)
    # Euclidean distance in degrees (good enough for nearest-neighbor)
    dlat = airports[:, 0] - lat
    dlon = airports[:, 1] - lon
    dist_sq = dlat * dlat + dlon * dlon
    nearest_idx = np.argmin(dist_sq)
    elev = int(airports[nearest_idx, 2])
    cache[key] = elev
    return elev


# --- Altitude to color ---

def altitude_to_band(alt, alt_floor, alt_ceil, num_bands=NUM_BANDS):
    """Map altitude to a color band index (0 = floor/red, num_bands-1 = ceil/green)."""
    if alt_ceil <= alt_floor:
        return 0
    frac = (alt - alt_floor) / (alt_ceil - alt_floor)
    frac = max(0.0, min(1.0, frac))
    band = int(frac * (num_bands - 1) + 0.5)
    return min(band, num_bands - 1)


def band_to_color(band, num_bands=NUM_BANDS):
    """Map band index to RGB: red -> orange -> yellow -> green.

    Uses a piecewise ramp that keeps colors bright throughout:
      0.0 = red (255,0,0)
      0.33 = orange (255,165,0)
      0.66 = yellow (255,255,0)
      1.0 = green (0,200,0)
    """
    if num_bands <= 1:
        return (255.0, 0.0, 0.0)
    frac = band / (num_bands - 1)
    if frac < 0.33:
        # Red to orange
        t = frac / 0.33
        return (255.0, 165.0 * t, 0.0)
    elif frac < 0.66:
        # Orange to yellow
        t = (frac - 0.33) / 0.33
        return (255.0, 165.0 + 90.0 * t, 0.0)
    else:
        # Yellow to green
        t = (frac - 0.66) / 0.34
        return (255.0 * (1.0 - t), 255.0 - 55.0 * t, 0.0)


# --- Data streaming ---

def stream_global_file(path):
    """Yield record dicts from a gzipped JSONL file.

    Skips records missing hex, now, lat, or lon.
    """
    try:
        with gzip.open(path, 'rt') as f:
            for line in f:
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                if (record.get('hex') is not None
                        and record.get('now') is not None
                        and record.get('lat') is not None
                        and record.get('lon') is not None):
                    yield record
    except (EOFError, OSError) as e:
        logger.warning(f"Error reading {path}: {e} (using partial data)")


# --- Disk-backed tile storage ---

FLUSH_INTERVAL = 1_000_000  # Flush segments to disk every N records


class TileStore:
    """Disk-backed tile accumulator storage.

    Keeps tile data as .npy files on disk, with a small LRU cache of
    recently-used tiles in memory. This bounds RAM to ~cache_size tiles
    regardless of how many total tiles exist.
    """

    def __init__(self, work_dir, cache_size=500):
        self.work_dir = Path(work_dir)
        # Clean any stale data from previous (possibly crashed) runs
        if self.work_dir.exists():
            shutil.rmtree(self.work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.cache_size = cache_size
        self._cache = OrderedDict()  # (tx,ty) -> (acc, count)
        self.tile_keys = set()  # all tiles ever created

    def _tile_path(self, tx, ty, suffix):
        return self.work_dir / f"{tx}_{ty}_{suffix}.npy"

    def _evict_oldest(self):
        """Write the oldest cached tile to disk and remove from cache."""
        key, (acc, count) = self._cache.popitem(last=False)
        np.save(self._tile_path(key[0], key[1], "acc"), acc)
        np.save(self._tile_path(key[0], key[1], "count"), count)

    def get(self, tx, ty):
        """Get tile accumulators, loading from disk or creating new."""
        key = (tx, ty)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]

        # Evict if cache full
        if len(self._cache) >= self.cache_size:
            self._evict_oldest()

        # Load from disk or create new
        acc_path = self._tile_path(tx, ty, "acc")
        if acc_path.exists():
            acc = np.load(acc_path)
            count = np.load(self._tile_path(tx, ty, "count"))
        else:
            acc = np.zeros((256, 256, 3), dtype=np.uint16)
            count = np.zeros((256, 256), dtype=np.uint16)

        self.tile_keys.add(key)
        self._cache[key] = (acc, count)
        return acc, count

    def flush_all(self):
        """Write all cached tiles to disk."""
        while self._cache:
            self._evict_oldest()

    def iter_tiles(self):
        """Iterate over all tiles, yielding (tx, ty, acc, count).

        Loads each tile from disk one at a time for memory efficiency.
        """
        for tx, ty in sorted(self.tile_keys):
            # Check cache first
            if (tx, ty) in self._cache:
                acc, count = self._cache[(tx, ty)]
            else:
                acc_path = self._tile_path(tx, ty, "acc")
                if not acc_path.exists():
                    continue
                acc = np.load(acc_path)
                count = np.load(self._tile_path(tx, ty, "count"))
            yield tx, ty, acc, count


def flush_segments(segments_by_tile_band, tile_store):
    """Render buffered segments into disk-backed tile store and clear buffer.

    Returns the number of tile-band groups rendered.
    """
    render_count = 0
    for (tx, ty, band), segs in segments_by_tile_band.items():
        if not segs:
            continue

        acc, count = tile_store.get(tx, ty)

        color_rgb = band_to_color(band)
        # Don't scale colors here - we'll handle opacity at save time
        cr = int(color_rgb[0] + 0.5)
        cg = int(color_rgb[1] + 0.5)
        cb = int(color_rgb[2] + 0.5)

        # Draw all segments for this band on a scratch image
        scratch = Image.new('L', (256, 256), 0)
        draw = ImageDraw.Draw(scratch)
        for seg in segs:
            draw.line([(seg[0], seg[1]), (seg[2], seg[3])],
                      fill=255, width=TRACK_WIDTH)

        # Use threshold > 128 to ignore anti-aliased edges (only keep solid pixels)
        mask = np.array(scratch) > 128
        # Use MAX not ADD - avoids dark colors from same track hitting pixels multiple times
        acc[:, :, 0][mask] = np.maximum(acc[:, :, 0][mask], cr)
        acc[:, :, 1][mask] = np.maximum(acc[:, :, 1][mask], cg)
        acc[:, :, 2][mask] = np.maximum(acc[:, :, 2][mask], cb)
        # Count tracks contributing to each pixel (for density/opacity later)
        count[mask] = np.minimum(count[mask].astype(np.uint32) + 1, 65535).astype(np.uint16)
        render_count += 1

    segments_by_tile_band.clear()
    return render_count


# --- Main tile generation ---

def generate_tiles(data_dir, output_dir, zoom=DEFAULT_ZOOM, max_files=10,
                   seed=None, max_records=None, input_file=None):
    """Generate traffic density tiles from global ADS-B files.

    Args:
        data_dir: Directory containing global_MMDDYY.gz files
        output_dir: Where to write tiles/{z}/{x}/{y}.png
        zoom: Tile zoom level (default 11)
        max_files: Maximum number of global files to process
        seed: Random seed for file selection (None = random)
        max_records: Stop processing each file after this many records (None = all)
        input_file: Specific .gz file to process (overrides data_dir/max_files/seed)
    """
    output_dir = Path(output_dir)

    if input_file:
        # Use the specific file provided
        input_path = Path(input_file)
        if not input_path.exists():
            print(f"Input file not found: {input_file}")
            return
        global_files = [input_path]
    else:
        data_dir = Path(data_dir)
        # Find global files
        global_files = sorted(data_dir.glob(f"{INPUT_DATA_PREFIX}*.gz"))
        if not global_files:
            print(f"No {INPUT_DATA_PREFIX}*.gz files found in {data_dir}")
            return

        # Sample files
        if seed is not None:
            random.seed(seed)
        if len(global_files) > max_files:
            global_files = random.sample(global_files, max_files)
        else:
            global_files = list(global_files)

    print(f"Processing {len(global_files)} global files at zoom {zoom}")
    for f in global_files:
        print(f"  {f.name}")

    # Load airport data for per-tile elevation lookup
    print("Loading airport database...")
    airports = load_all_airports()
    print(f"  {len(airports)} airports loaded")
    tile_elev_cache = {}

    # Disk-backed tile storage — only ~500 tiles in RAM at a time (~256MB)
    # Use /tmp for work dir to avoid slow network shares
    work_dir = Path("/tmp/traffic_tiles_work")
    tile_store = TileStore(work_dir, cache_size=1000)

    total_records = 0
    total_segments = 0
    total_filtered = 0
    total_renders = 0

    for file_idx, global_gz in enumerate(global_files):
        print(f"\n[{file_idx+1}/{len(global_files)}] Processing {global_gz.name}...")
        t0 = time.time()

        last_seen = {}  # hex -> (lat, lon, alt_int, ts)
        # (tx, ty, band) -> list of (px1, py1, px2, py2)
        segments_by_tile_band = defaultdict(list)
        file_records = 0
        file_segments = 0
        file_filtered = 0
        file_renders = 0
        since_flush = 0

        for record in stream_global_file(global_gz):
            file_records += 1
            since_flush += 1
            if max_records and file_records > max_records:
                print(f"  Reached --max-records limit ({max_records:,})")
                break

            # Periodically flush segments to keep memory bounded
            if since_flush >= FLUSH_INTERVAL:
                file_renders += flush_segments(segments_by_tile_band,
                                               tile_store)
                since_flush = 0

            if file_records % 1_000_000 == 0:
                elapsed = time.time() - t0
                rate = file_records / elapsed if elapsed > 0 else 0
                cache_mb = len(tile_store._cache) * 512 / 1024
                print(f"  {file_records:,} records ({rate:,.0f}/sec), "
                      f"{file_segments:,} segments, "
                      f"{file_filtered:,} altitude-filtered, "
                      f"{len(tile_store.tile_keys)} tiles "
                      f"({len(tile_store._cache)} cached, ~{cache_mb:.0f}MB)")

            hex_id = record['hex']
            lat = record['lat']
            lon = record['lon']
            ts = record['now']
            alt = record.get('alt_baro')

            # Parse altitude
            alt_int = None
            if alt is not None and alt != "ground":
                try:
                    alt_int = int(alt)
                except (ValueError, TypeError):
                    pass

            if hex_id in last_seen:
                plat, plon, palt, pts = last_seen[hex_id]
                gap = ts - pts
                if 0 < gap < MAX_GAP_SECONDS and alt_int is not None and palt is not None:
                    # Compute tile for both endpoints
                    tx1, ty1, px1, py1 = latlon_to_tile_pixel(plat, plon, zoom)
                    tx2, ty2, px2, py2 = latlon_to_tile_pixel(lat, lon, zoom)

                    if (tx1, ty1) == (tx2, ty2):
                        tile_key = (tx1, ty1)

                        # Per-tile altitude filter
                        fe = get_tile_field_elev(tx1, ty1, zoom, airports,
                                                 tile_elev_cache)
                        alt_floor = fe + FT_MIN_BELOW_AIRPORT
                        alt_ceil = fe + FT_MAX_ABOVE_AIRPORT

                        avg_alt = (palt + alt_int) // 2
                        if alt_floor <= avg_alt <= alt_ceil:
                            band = altitude_to_band(avg_alt, alt_floor,
                                                    alt_ceil)
                            segments_by_tile_band[(*tile_key, band)].append(
                                (px1, py1, px2, py2))
                            file_segments += 1
                        else:
                            file_filtered += 1

            last_seen[hex_id] = (lat, lon, alt_int, ts)

        # Final flush for remaining segments
        file_renders += flush_segments(segments_by_tile_band, tile_store)

        total_records += file_records
        total_segments += file_segments
        total_filtered += file_filtered
        total_renders += file_renders

        elapsed = time.time() - t0
        print(f"  Done: {file_records:,} records, {file_segments:,} segments, "
              f"{file_filtered:,} altitude-filtered, "
              f"{file_renders} tile-band renders in {elapsed:.1f}s")
        print(f"  Total tiles: {len(tile_store.tile_keys)}")

    # Flush remaining cached tiles to disk before saving
    tile_store.flush_all()

    # Save tiles with per-tile normalization and gamma boost
    print(f"\nSaving tiles...")
    if not tile_store.tile_keys:
        print("No tiles generated (no data)")
        return

    print(f"  {len(tile_store.tile_keys)} tiles at zoom {zoom}")
    saved = 0
    # Track which tiles were saved so downsampling can use the set directly
    # instead of walking the filesystem
    saved_tiles = set()
    for tx, ty, acc, count in tile_store.iter_tiles():
        tile_max = acc.max()
        if tile_max == 0:
            continue

        # Scale colors by track density to control opacity
        # count tracks how many bands/tracks touched each pixel
        # A single track = partial opacity, multiple overlapping tracks = brighter

        # Scale alpha by density (more tracks = more opaque)
        alpha_scale = np.minimum(count.astype(np.float32) / DENSITY_FOR_FULL_BRIGHTNESS, 1.0)
        alpha = (alpha_scale * 255.0).clip(0, 255).astype(np.uint8)

        # Scale color brightness (vibrancy control)
        rgb = (acc.astype(np.float32) * COLOR_VIBRANCY).clip(0, 255).astype(np.uint8)
        rgba = np.dstack([rgb, alpha])

        img = Image.fromarray(rgba, 'RGBA')
        tile_path = output_dir / str(zoom) / str(tx) / f"{ty}.png"
        tile_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(tile_path)
        saved_tiles.add((tx, ty))
        saved += 1

    print(f"  Saved {saved} tiles to {output_dir}/{zoom}/")
    print("Now generating lower zoom levels by downsampling...")
    # Generate lower zoom levels by downsampling (4 child tiles -> 1 parent)
    # Use saved_tiles set to compute parents directly — no directory walking
    child_tiles = saved_tiles
    for parent_zoom in range(zoom - 1, max(zoom - 4, 5), -1):
        child_zoom = parent_zoom + 1
        parent_tiles = {(tx // 2, ty // 2) for tx, ty in child_tiles}

        parent_saved = 0
        parent_saved_tiles = set()
        for ptx, pty in sorted(parent_tiles):
            # Paste 4 children at full res into 512x512, then resize once
            canvas = Image.new('RGBA', (512, 512), (0, 0, 0, 0))
            has_content = False
            for dx, dy in [(0, 0), (1, 0), (0, 1), (1, 1)]:
                if (ptx * 2 + dx, pty * 2 + dy) in child_tiles:
                    child_path = (output_dir / str(child_zoom)
                                  / str(ptx * 2 + dx) / f"{pty * 2 + dy}.png")
                    canvas.paste(Image.open(child_path), (dx * 256, dy * 256))
                    has_content = True

            if has_content:
                parent_img = canvas.resize((256, 256), Image.BILINEAR)
                tile_path = output_dir / str(parent_zoom) / str(ptx) / f"{pty}.png"
                tile_path.parent.mkdir(parents=True, exist_ok=True)
                parent_img.save(tile_path)
                parent_saved_tiles.add((ptx, pty))
                parent_saved += 1

        print(f"  Generated {parent_saved} tiles at zoom {parent_zoom}")
        child_tiles = parent_saved_tiles

    # Clean up work directory
    shutil.rmtree(work_dir, ignore_errors=True)

    print(f"\nTotals: {total_records:,} records, {total_segments:,} segments, "
          f"{total_filtered:,} altitude-filtered, {total_renders} renders")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate traffic track tiles from global ADS-B data")
    parser.add_argument("--data-dir", type=str, default="data",
                        help="Directory containing global_MMDDYY.gz files")
    parser.add_argument("--output-dir", type=str, default="tiles/traffic",
                        help="Output directory for tiles")
    parser.add_argument("--zoom", type=int, default=DEFAULT_ZOOM,
                        help=f"Tile zoom level (default: {DEFAULT_ZOOM})")
    parser.add_argument("--max-files", type=int, default=10,
                        help="Maximum number of global files to process")
    parser.add_argument("--seed", type=int, default=123,
                        help="Random seed for file selection")
    parser.add_argument("--max-records", type=int, default=None,
                        help="Stop processing each file after this many records")
    parser.add_argument("--input-file", type=str, default=None,
                        help="Specific .gz file to process (overrides --data-dir)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    generate_tiles(args.data_dir, args.output_dir, args.zoom,
                   args.max_files, args.seed, args.max_records,
                   args.input_file)

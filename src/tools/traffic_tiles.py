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
import logging
import math
import shutil
import sys
import time
from collections import defaultdict, OrderedDict
from pathlib import Path

import orjson
import numpy as np
from PIL import Image, ImageDraw

# Continental US bounding box (upper-left and lower-right corners)
# These bounds roughly cover the contiguous United States
CONUS_UPPER_LEFT = (49.5, -125.0)   # lat, lon (northwest corner near WA/Canada border)
CONUS_LOWER_RIGHT = (24.5, -66.0)   # lat, lon (southeast corner near FL/Atlantic)

try:
    from src.tools.batch_helpers import (FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT,
                                         validate_date, generate_date_range,
                                         global_files_for_dates)
    from src.tools.generate_airport_config import download_with_cache, AIRPORTS_URL
except ImportError:
    from batch_helpers import (FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT,
                               validate_date, generate_date_range,
                               global_files_for_dates)
    from generate_airport_config import download_with_cache, AIRPORTS_URL

logger = logging.getLogger(__name__)

# Track continuity: gap > this starts a new track
MAX_GAP_SECONDS = 120

# Altitude color bands
NUM_BANDS = 10

# Default zoom level (~190ft/pixel at 40°N; zoom 12 would be ~95ft/pixel)
DEFAULT_ZOOM = 11

DENSITY_FOR_FULL_BRIGHTNESS = 40  # adjust this: lower = brighter single tracks
COLOR_VIBRANCY = 0.9  # adjust this: 1.0 = full brightness, <1.0 = dimmer, >1.0 = brighter
TRACK_WIDTH = 2  # pixels - adjust this for thicker or thinner track lines

# --- Geographic bounds checking ---

def is_within_conus(lat, lon):
    """Check if a lat/lon point is within the continental US bounding box.

    Args:
        lat: Latitude in degrees
        lon: Longitude in degrees

    Returns:
        True if point is within CONUS bounds, False otherwise
    """
    ul_lat, ul_lon = CONUS_UPPER_LEFT
    lr_lat, lr_lon = CONUS_LOWER_RIGHT

    # Check latitude (north to south)
    if not (lr_lat <= lat <= ul_lat):
        return False

    # Check longitude (west to east)
    if not (ul_lon <= lon <= lr_lon):
        return False

    return True

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


def precompute_tile_elevations(zoom, airports):
    """Precompute nearest-airport elevation for all CONUS tiles at given zoom.

    Returns dict mapping (tx, ty) -> field_elev_ft.
    Uses scipy KD-tree for fast bulk nearest-neighbor lookup.
    """
    from scipy.spatial import cKDTree

    n = 2 ** zoom
    # Compute tile index range covering CONUS
    ul_lat, ul_lon = CONUS_UPPER_LEFT
    lr_lat, lr_lon = CONUS_LOWER_RIGHT
    _, ty_min, _, _ = latlon_to_tile_pixel(ul_lat, ul_lon, zoom)
    tx_min, _, _, _ = latlon_to_tile_pixel(ul_lat, ul_lon, zoom)
    _, ty_max, _, _ = latlon_to_tile_pixel(lr_lat, lr_lon, zoom)
    tx_max, _, _, _ = latlon_to_tile_pixel(lr_lat, lr_lon, zoom)

    # Build list of tile centers
    tile_keys = []
    tile_centers = []
    for tx in range(tx_min, tx_max + 1):
        for ty in range(ty_min, ty_max + 1):
            lat, lon = tile_center(tx, ty, zoom)
            tile_keys.append((tx, ty))
            tile_centers.append((lat, lon))

    tile_centers = np.array(tile_centers, dtype=np.float64)
    print(f"  Precomputing elevations for {len(tile_keys)} CONUS tiles...")

    # KD-tree on airport lat/lon for fast nearest-neighbor
    tree = cKDTree(airports[:, :2])
    _, indices = tree.query(tile_centers)

    cache = {}
    for i, key in enumerate(tile_keys):
        cache[key] = int(airports[indices[i], 2])

    return cache


# --- Line clipping ---

def clip_segment_to_tile(x1, y1, x2, y2, tx1, ty1, tx2, ty2, target_tx, target_ty):
    """Clip a line segment to a specific tile's pixel bounds [0, 256).

    Args:
        x1, y1: Start point in tile (tx1, ty1) pixel coords
        x2, y2: End point in tile (tx2, ty2) pixel coords
        tx1, ty1: Tile coordinates of start point
        tx2, ty2: Tile coordinates of end point
        target_tx, target_ty: Tile to clip to

    Returns:
        (clipped_x1, clipped_y1, clipped_x2, clipped_y2) in target tile coords,
        or None if segment doesn't intersect this tile.
    """
    # Convert both endpoints to target tile's coordinate system
    # Each tile is 256 pixels, so offset = (tile_diff) * 256
    offset_x1 = (tx1 - target_tx) * 256
    offset_y1 = (ty1 - target_ty) * 256
    offset_x2 = (tx2 - target_tx) * 256
    offset_y2 = (ty2 - target_ty) * 256

    # Convert to target tile coords
    seg_x1 = x1 + offset_x1
    seg_y1 = y1 + offset_y1
    seg_x2 = x2 + offset_x2
    seg_y2 = y2 + offset_y2

    # Clip to [0, 256) using Cohen-Sutherland
    xmin, ymin, xmax, ymax = 0, 0, 256, 256

    def outcode(x, y):
        code = 0
        if x < xmin: code |= 1  # LEFT
        if x >= xmax: code |= 2  # RIGHT
        if y < ymin: code |= 4  # TOP
        if y >= ymax: code |= 8  # BOTTOM
        return code

    out1 = outcode(seg_x1, seg_y1)
    out2 = outcode(seg_x2, seg_y2)

    while True:
        if not (out1 | out2):  # Both inside
            return (int(seg_x1), int(seg_y1), int(seg_x2), int(seg_y2))
        if out1 & out2:  # Both outside same edge
            return None

        # Pick point outside and clip
        out = out1 if out1 else out2

        # Find intersection with boundary
        if seg_x2 != seg_x1:
            slope = (seg_y2 - seg_y1) / (seg_x2 - seg_x1)
        else:
            slope = float('inf')

        if out & 1:  # LEFT
            x = xmin
            y = seg_y1 + slope * (x - seg_x1) if slope != float('inf') else seg_y1
        elif out & 2:  # RIGHT
            x = xmax - 0.001  # Just inside
            y = seg_y1 + slope * (x - seg_x1) if slope != float('inf') else seg_y1
        elif out & 4:  # TOP
            y = ymin
            x = seg_x1 + (y - seg_y1) / slope if slope != 0 and slope != float('inf') else seg_x1
        else:  # BOTTOM
            y = ymax - 0.001  # Just inside
            x = seg_x1 + (y - seg_y1) / slope if slope != 0 and slope != float('inf') else seg_x1

        if out == out1:
            seg_x1, seg_y1 = x, y
            out1 = outcode(seg_x1, seg_y1)
        else:
            seg_x2, seg_y2 = x, y
            out2 = outcode(seg_x2, seg_y2)


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
    """Map band index to RGB: purple -> blue -> light blue.

    Heavy traffic (band 0) = purple, light traffic (band max) = light blue.
    Sparse end is light blue (not white) so it's visible on light basemaps.
      0.0  = purple     (128, 0, 255)
      0.5  = blue       (0, 64, 255)
      1.0  = light blue (150, 220, 255)
    """
    if num_bands <= 1:
        return (128.0, 0.0, 255.0)
    frac = band / (num_bands - 1)
    if frac < 0.5:
        # Purple to blue
        t = frac / 0.5
        return (128.0 * (1.0 - t), 64.0 * t, 255.0)
    else:
        # Blue to light blue
        t = (frac - 0.5) / 0.5
        return (150.0 * t, 64.0 + 156.0 * t, 255.0)


# --- Data streaming ---

def stream_global_file(path):
    """Yield record dicts from a gzipped JSONL file.

    Skips records missing hex, now, lat, or lon.
    """
    try:
        with gzip.open(path, 'rb') as f:
            for line in f:
                try:
                    record = orjson.loads(line)
                except (orjson.JSONDecodeError, ValueError):
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
    """Disk-backed tile count storage.

    Keeps tile count data as .npy files on disk, with a small LRU cache of
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
        self._cache = OrderedDict()  # (tx,ty) -> count
        self.tile_keys = set()  # all tiles ever created

    def _tile_path(self, tx, ty):
        return self.work_dir / f"{tx}_{ty}_count.npy"

    def _evict_oldest(self):
        """Write the oldest cached tile to disk and remove from cache."""
        key, count = self._cache.popitem(last=False)
        np.save(self._tile_path(key[0], key[1]), count)

    def get(self, tx, ty):
        """Get tile count array, loading from disk or creating new."""
        key = (tx, ty)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]

        # Evict if cache full
        if len(self._cache) >= self.cache_size:
            self._evict_oldest()

        # Load from disk or create new
        count_path = self._tile_path(tx, ty)
        if count_path.exists():
            count = np.load(count_path)
        else:
            count = np.zeros((256, 256), dtype=np.uint16)

        self.tile_keys.add(key)
        self._cache[key] = count
        return count

    def flush_all(self):
        """Write all cached tiles to disk."""
        while self._cache:
            self._evict_oldest()

    def iter_tiles(self):
        """Iterate over all tiles, yielding (tx, ty, count).

        Loads each tile from disk one at a time for memory efficiency.
        """
        for tx, ty in sorted(self.tile_keys):
            # Check cache first
            if (tx, ty) in self._cache:
                count = self._cache[(tx, ty)]
            else:
                count_path = self._tile_path(tx, ty)
                if not count_path.exists():
                    continue
                count = np.load(count_path)
            yield tx, ty, count


def flush_segments(segments_by_tile_band, tile_store):
    """Render buffered segments into disk-backed tile store and clear buffer.

    Increments track count for each pixel. Color is applied at save time based
    on final density (not altitude).

    Returns the number of tile-band groups rendered.
    """
    render_count = 0
    for (tx, ty, band), segs in segments_by_tile_band.items():
        if not segs:
            continue

        count = tile_store.get(tx, ty)

        # Draw all segments for this band on a scratch image
        scratch = Image.new('L', (256, 256), 0)
        draw = ImageDraw.Draw(scratch)
        for seg in segs:
            draw.line([(seg[0], seg[1]), (seg[2], seg[3])],
                      fill=255, width=TRACK_WIDTH)

        # Use threshold > 128 to ignore anti-aliased edges (only keep solid pixels)
        mask = np.array(scratch) > 128

        # Increment count, color will be applied at save time based on density
        count[mask] = np.minimum(count[mask].astype(np.uint32) + 1, 65535).astype(np.uint16)
        render_count += 1

    segments_by_tile_band.clear()
    return render_count


# --- Main tile generation ---

def generate_tiles(output_dir, global_files, zoom=DEFAULT_ZOOM,
                   max_records=None):
    """Generate traffic density tiles from global ADS-B files.

    Colors are based on traffic density: red = high density, green = low density.

    Args:
        output_dir: Where to write tiles/{z}/{x}/{y}.png
        global_files: List of Path objects to process
        zoom: Tile zoom level (default 11)
        max_records: Stop processing each file after this many records (None = all)
    """
    output_dir = Path(output_dir)

    if not global_files:
        print("No global files to process")
        return

    print(f"Processing {len(global_files)} global files at zoom {zoom}")
    for f in global_files:
        print(f"  {f.name}")

    # Load airport data and precompute per-tile elevations
    print("Loading airport database...")
    airports = load_all_airports()
    print(f"  {len(airports)} airports loaded")
    tile_elev_cache = precompute_tile_elevations(zoom, airports)

    # Disk-backed tile storage with large cache to avoid I/O thrashing.
    # ~6000 tiles × 128KB = ~750MB RAM for a typical single-day CONUS run.
    work_dir = Path("/tmp/traffic_tiles_work")
    tile_store = TileStore(work_dir, cache_size=8000)

    total_records = 0
    total_segments = 0
    total_filtered = 0
    total_renders = 0

    for file_idx, global_gz in enumerate(global_files):
        print(f"\n[{file_idx+1}/{len(global_files)}] Processing {global_gz.name}...")
        t0 = time.time()

        last_seen = {}  # hex -> (alt_int, ts, tx, ty, px, py)
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

            # Skip records outside continental US
            if not is_within_conus(lat, lon):
                continue

            # Parse altitude
            alt_int = None
            if alt is not None and alt != "ground":
                try:
                    alt_int = int(alt)
                except (ValueError, TypeError):
                    pass

            # Compute tile/pixel for current point once; reuse as prev next time
            tx2, ty2, px2, py2 = latlon_to_tile_pixel(lat, lon, zoom)

            if hex_id in last_seen:
                palt, pts, tx1, ty1, px1, py1 = last_seen[hex_id]
                gap = ts - pts
                if 0 < gap < MAX_GAP_SECONDS and alt_int is not None and palt is not None:
                    # Get all tiles this segment potentially touches
                    min_tx, max_tx = min(tx1, tx2), max(tx1, tx2)
                    min_ty, max_ty = min(ty1, ty2), max(ty1, ty2)

                    avg_alt = (palt + alt_int) // 2

                    # Try to clip segment into each tile it might intersect
                    for ttx in range(min_tx, max_tx + 1):
                        for tty in range(min_ty, max_ty + 1):
                            # Altitude check first (cheap cached lookup) to
                            # skip expensive clip for filtered segments
                            fe = tile_elev_cache[ttx, tty]
                            alt_floor = fe + FT_MIN_BELOW_AIRPORT
                            alt_ceil = fe + FT_MAX_ABOVE_AIRPORT

                            if not (alt_floor <= avg_alt <= alt_ceil):
                                file_filtered += 1
                                continue

                            # Clip segment to this tile
                            clipped = clip_segment_to_tile(px1, py1, px2, py2,
                                                          tx1, ty1, tx2, ty2,
                                                          ttx, tty)
                            if clipped is None:
                                continue

                            clip_px1, clip_py1, clip_px2, clip_py2 = clipped
                            band = altitude_to_band(avg_alt, alt_floor, alt_ceil)
                            segments_by_tile_band[(ttx, tty, band)].append(
                                (clip_px1, clip_py1, clip_px2, clip_py2))
                            file_segments += 1

            last_seen[hex_id] = (alt_int, ts, tx2, ty2, px2, py2)

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

    print(f"\nSaving tiles...")
    if not tile_store.tile_keys:
        print("No tiles generated (no data)")
        return

    print(f"  {len(tile_store.tile_keys)} tiles at zoom {zoom}")
    print(f"  Using density-based coloring: purple = high density, white = low density")
    saved = 0
    # Track which tiles were saved so downsampling can use the set directly
    # instead of walking the filesystem
    saved_tiles = set()
    for tx, ty, count in tile_store.iter_tiles():
        # Color pixels based purely on density
        max_count = count.max()
        if max_count == 0:
            continue

        # Log-scale normalization so count 1-5 spreads across the color ramp
        # instead of clustering at the sparse end.
        # log1p(count)/log1p(DENSITY_FOR_FULL_BRIGHTNESS) gives 0..1
        log_max = math.log1p(DENSITY_FOR_FULL_BRIGHTNESS)
        count_f = count.astype(np.float32)

        # Apply color gradient: high density -> band 0 (purple), low -> band max (light blue)
        rgb = np.zeros((256, 256, 3), dtype=np.uint8)
        for i in range(256):
            for j in range(256):
                if count[i, j] > 0:
                    norm = min(math.log1p(count[i, j]) / log_max, 1.0)
                    band_frac = 1.0 - norm  # high density -> band 0
                    band_idx = int(band_frac * (NUM_BANDS - 1) + 0.5)
                    color = band_to_color(band_idx)
                    rgb[i, j, 0] = int(color[0] * COLOR_VIBRANCY + 0.5)
                    rgb[i, j, 1] = int(color[1] * COLOR_VIBRANCY + 0.5)
                    rgb[i, j, 2] = int(color[2] * COLOR_VIBRANCY + 0.5)

        # Alpha: quadratic ramp so single tracks (count=1) are nearly invisible
        # and density must build up before becoming prominent.
        # count=1 -> norm~0.23 -> alpha~14, count=3 -> ~40, count=10 -> ~127
        alpha_norm = np.minimum(np.log1p(count_f) / log_max, 1.0)
        alpha = np.where(count > 0,
                         (alpha_norm * alpha_norm * 255.0).clip(0, 255),
                         0).astype(np.uint8)
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
    parser.add_argument("--start-date", type=validate_date, required=True,
                        help="Start date in mm/dd/yy format")
    parser.add_argument("--end-date", type=validate_date, required=True,
                        help="End date in mm/dd/yy format")
    parser.add_argument("--day-filter", type=str, default="all",
                        choices=["all", "weekday", "weekend"],
                        help="Filter dates by day type (default: all)")
    parser.add_argument("--data-dir", type=str, default="data",
                        help="Directory containing global_MMDDYY.gz files")
    parser.add_argument("--output-dir", type=str, default="tiles/traffic",
                        help="Output directory for tiles")
    parser.add_argument("--zoom", type=int, default=DEFAULT_ZOOM,
                        help=f"Tile zoom level (default: {DEFAULT_ZOOM})")
    parser.add_argument("--max-records", type=int, default=None,
                        help="Stop processing each file after this many records")
    args = parser.parse_args()

    dates = generate_date_range(args.start_date, args.end_date, args.day_filter)
    global_files = global_files_for_dates(dates, Path(args.data_dir))

    logging.basicConfig(level=logging.INFO)
    generate_tiles(args.output_dir, global_files, args.zoom,
                   args.max_records)

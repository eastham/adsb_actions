#!/usr/bin/env python3
"""Convert trace files to time-sorted JSONL for efficient streaming analysis.

This tool performs a one-time preprocessing of readsb trace files (gzipped JSON,
one file per aircraft) into a single time-sorted JSONL file that can be streamed
with minimal memory usage.

Performance optimizations:
- Uses orjson for 2-5x faster JSON parsing (auto-detected)
- Uses pigz for parallel gzip compression if available (2-3x faster)
  Install: brew install pigz (macOS) or apt-get install pigz (Linux)

Algorithm: External Merge Sort
------------------------------
A typical day's ADS-B data globally (~85M points, ~15GB uncompressed) is too 
large to sort in memory.  We use external merge sort, which works in two phases:

Phase 1 - Chunking:
  Read input files and accumulate points in memory. When the buffer reaches
  CHUNK_SIZE points (~500K, about 100MB), sort the buffer by timestamp and
  write it to a temporary file. A "chunk" is one of these sorted temp files.

  Note: Chunks are NOT per-aircraft. Each chunk contains points from many
  aircraft, sorted by timestamp. This is intentional - we're sorting globally
  by time, not grouping by aircraft.

Phase 2 - K-way Merge:
  Open all chunk files simultaneously and use a min-heap to merge them into
  a single sorted output. The heap always contains one point from each chunk,
  and we repeatedly extract the minimum (earliest timestamp) and write it out.

Memory usage: O(CHUNK_SIZE) during Phase 1, O(num_chunks) during Phase 2.

Usage:
    # Convert all data
    python convert_traces.py ~/Downloads/adsb_lol_data/traces -o traces_sorted.jsonl.gz

    # Convert only data within 50nm of a location (much smaller output)
    python convert_traces.py ~/Downloads/adsb_lol_data/traces -o local.jsonl.gz \
        --lat 37.5 --lon -122.0 --radius 50

The output file can then be used with:
    python prox_analyze_from_files.py --sorted-file traces_sorted.jsonl.gz ...
"""

import argparse
import gzip
import heapq
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.replay import locate_files, json_loads, json_dumps
from lib.readsb_parse import parse_readsb_json

# Tuning parameters
DEFAULT_CHUNK_SIZE = 2000000  # points per temp chunk before flushing to disk

# Constants for fast distance calculation
NM_PER_DEG_LAT = 60.0  # nautical miles per degree of latitude


def fast_distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Fast approximate distance in nautical miles using equirectangular projection.

    This is much faster than geopy.geodesic and accurate enough for filtering
    (within ~0.5% for distances under 100nm at mid-latitudes).

    Args:
        lat1, lon1: First point
        lat2, lon2: Second point

    Returns:
        Approximate distance in nautical miles
    """
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Adjust longitude difference for latitude (approximate)
    avg_lat = (lat1 + lat2) / 2.0
    dlon_adjusted = dlon * math.cos(math.radians(avg_lat))

    # Distance in degrees, then convert to nm
    dist_deg = math.sqrt(dlat * dlat + dlon_adjusted * dlon_adjusted)
    return dist_deg * NM_PER_DEG_LAT


def parse_file_to_points(filepath: str, lat_filter: float = None,
                         lon_filter: float = None, radius_nm: float = None) -> list:
    """Parse a single trace file and return list of (timestamp, point_dict) tuples.

    Args:
        filepath: Path to gzipped JSON trace file
        lat_filter: Optional center latitude for spatial filter
        lon_filter: Optional center longitude for spatial filter
        radius_nm: Optional radius in nautical miles for spatial filter

    Returns:
        List of (timestamp, point_dict) tuples
    """
    points = []

    try:
        with gzip.open(filepath, 'rb') as f:
            data = json_loads(f.read())
    except gzip.BadGzipFile:
        print(f"Warning: Failed to decompress {filepath}, skipping")
        return points
    except Exception as e:
        print(f"Warning: Error reading {filepath}: {e}, skipping")
        return points

    # Use a collector dict to gather points
    collector = {}
    parse_readsb_json(data, collector)

    # Check if spatial filtering is enabled
    do_filter = lat_filter is not None and lon_filter is not None and radius_nm is not None

    # Flatten to list of (timestamp, point) tuples
    for ts, point_list in collector.items():
        for point in point_list:
            if do_filter:
                # Check if point is within radius
                plat = point.get('lat')
                plon = point.get('lon')
                if plat is None or plon is None:
                    continue
                # Use fast approximate distance (accurate enough for filtering)
                dist_nm = fast_distance_nm(plat, plon, lat_filter, lon_filter)
                if dist_nm > radius_nm:
                    continue
            points.append((ts, point))

    return points


def write_sorted_chunk(buffer: list, temp_dir: str) -> str:
    """Sort buffer by timestamp and write to a temp file.

    Args:
        buffer: List of (timestamp, point_dict) tuples
        temp_dir: Directory for temp files

    Returns:
        Path to temp file
    """
    buffer.sort(key=lambda x: x[0])

    fd, path = tempfile.mkstemp(suffix='.jsonl.gz', dir=temp_dir)
    os.close(fd)

    # Use fast compression for temp chunks (they get deleted anyway)
    with gzip.open(path, 'wt', compresslevel=1) as f:
        for ts, point in buffer:
            f.write(json_dumps(point) + '\n')

    return path


def read_chunk(filepath: str):
    """Generator that yields (timestamp, point_dict) from a chunk file.

    Args:
        filepath: Path to gzipped JSONL chunk file

    Yields:
        (timestamp, point_dict) tuples
    """
    with gzip.open(filepath, 'rt') as f:
        for line in f:
            point = json_loads(line)
            ts = point.get('now', 0)
            yield (ts, point)


def convert_to_sorted(input_dir: str, output_path: str, progress_interval: int = 1000,
                      lat_filter: float = None, lon_filter: float = None,
                      radius_nm: float = None, chunk_size: int = DEFAULT_CHUNK_SIZE):
    """Convert directory of trace files to single time-sorted JSONL file.

    Uses external merge sort:
    1. Read files in batches, sort each batch, write to temp files
    2. K-way merge all temp files into final sorted output

    Args:
        input_dir: Directory containing trace files
        output_path: Output path for sorted JSONL file (will be gzipped)
        progress_interval: How often to print progress (files)
        lat_filter: Optional center latitude for spatial filter
        lon_filter: Optional center longitude for spatial filter
        radius_nm: Optional radius in nautical miles for spatial filter
    """
    start_time = time.time()

    # Find all trace files
    files = locate_files(input_dir, "*.json")
    if not files:
        print(f"No .json files found in {input_dir}")
        return

    print(f"Found {len(files):,} trace files to process")

    if lat_filter is not None and lon_filter is not None and radius_nm is not None:
        print(f"Spatial filter: {radius_nm} nm radius around ({lat_filter}, {lon_filter})")

    # Create temp directory for intermediate sorted chunks
    temp_dir = tempfile.mkdtemp(prefix='trace_sort_')
    print(f"Using temp directory: {temp_dir}")

    try:
        # Phase 1: Read files and create sorted chunks
        print("\n=== Phase 1: Creating sorted chunks ===")
        temp_files = []
        buffer = []
        total_points = 0
        files_processed = 0

        for filepath in files:
            points = parse_file_to_points(filepath, lat_filter, lon_filter, radius_nm)
            buffer.extend(points)
            total_points += len(points)
            files_processed += 1

            if files_processed % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = files_processed / elapsed
                eta = (len(files) - files_processed) / rate
                print(f"  {files_processed:,}/{len(files):,} files, "
                      f"{total_points:,} points, "
                      f"{rate:.0f} files/sec, ETA {eta/60:.1f} min")

            # Flush buffer to disk when it gets large
            if len(buffer) >= chunk_size:
                chunk_path = write_sorted_chunk(buffer, temp_dir)
                temp_files.append(chunk_path)
                print(f"  Wrote chunk {len(temp_files)} ({len(buffer):,} points)")
                buffer = []

        # Don't forget remaining buffer
        if buffer:
            chunk_path = write_sorted_chunk(buffer, temp_dir)
            temp_files.append(chunk_path)
            print(f"  Wrote final chunk ({len(buffer):,} points)")

        phase1_time = time.time() - start_time
        print(f"\nPhase 1 complete: {len(temp_files)} chunks, "
              f"{total_points:,} total points, {phase1_time:.1f}s")

        # Phase 2: K-way merge to final output
        print(f"\n=== Phase 2: Merging to {output_path} ===")
        merge_start = time.time()

        # Open all chunk readers
        chunk_readers = [read_chunk(f) for f in temp_files]

        # Merge and write (using pigz for parallel compression if available)
        written = 0

        # Check if pigz is available for parallel gzip compression
        use_pigz = shutil.which('pigz') is not None

        if use_pigz:
            print(f"  Using pigz for parallel compression")
            # Use pigz subprocess for parallel compression
            pigz_process = subprocess.Popen(
                ['pigz', '-c'],
                stdin=subprocess.PIPE,
                stdout=open(output_path, 'wb'),
                text=True
            )
            out = pigz_process.stdin
        else:
            print(f"  Using standard gzip (pigz not available)")
            out = gzip.open(output_path, 'wt')

        try:
            merged = heapq.merge(*chunk_readers, key=lambda x: x[0])

            for ts, point in merged:
                out.write(json_dumps(point) + '\n')
                written += 1

                if written % 1000000 == 0:
                    elapsed = time.time() - merge_start
                    rate = written / elapsed
                    eta = (total_points - written) / rate
                    print(f"  {written/1e6:.1f}M/{total_points/1e6:.1f}M points, "
                          f"{rate/1e6:.2f}M/sec, ETA {eta:.0f}s")
        finally:
            out.close()
            if use_pigz:
                pigz_process.wait()

        merge_time = time.time() - merge_start
        total_time = time.time() - start_time

        # Get output file size
        output_size = os.path.getsize(output_path) / (1024 * 1024 * 1024)

        print(f"\n=== Complete ===")
        print(f"Output: {output_path}")
        print(f"Size: {output_size:.2f} GB")
        print(f"Points: {written:,}")
        print(f"Phase 1 (read/chunk): {phase1_time:.1f}s")
        print(f"Phase 2 (merge): {merge_time:.1f}s")
        print(f"Total time: {total_time/60:.1f} min")

    finally:
        # Cleanup temp files
        print(f"\nCleaning up {len(temp_files)} temp files...")
        for f in temp_files:
            try:
                os.unlink(f)
            except Exception:
                pass
        try:
            os.rmdir(temp_dir)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description='Convert trace files to time-sorted JSONL for streaming analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('input_dir', help='Directory containing trace files')
    parser.add_argument('-o', '--output', required=True,
                        help='Output path for sorted JSONL file (will be gzipped)')
    parser.add_argument('--progress', type=int, default=1000,
                        help='Progress update interval (files)')
    parser.add_argument('--lat', type=float,
                        help='Center latitude for spatial filter')
    parser.add_argument('--lon', type=float,
                        help='Center longitude for spatial filter')
    parser.add_argument('--radius', type=float,
                        help='Radius in nautical miles for spatial filter')
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f'Points per sort chunk (default: {DEFAULT_CHUNK_SIZE})')

    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f"Error: {args.input_dir} is not a directory")
        sys.exit(1)

    # Validate spatial filter args - all three must be provided together
    filter_args = [args.lat, args.lon, args.radius]
    if any(a is not None for a in filter_args) and not all(a is not None for a in filter_args):
        print("Error: --lat, --lon, and --radius must all be specified together")
        sys.exit(1)

    convert_to_sorted(args.input_dir, args.output, args.progress,
                      args.lat, args.lon, args.radius, args.chunk_size)


if __name__ == '__main__':
    main()

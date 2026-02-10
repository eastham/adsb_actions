#!/usr/bin/env python3
"""
Profiler for trace file reading to identify bottlenecks.

Run with: source .venv/bin/activate && python src/tools/profile_trace_reading.py ~/Downloads/adsb_lol_data/traces

This profiles:
1. File discovery time
2. Gzip decompression time (CPU-bound)
3. JSON parsing time
4. Data structure building time
5. Memory usage at each stage
"""

import argparse
import gc
import gzip
import json
import os
import sys
import time
import tracemalloc
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from fnmatch import fnmatch

# Try to import optional faster libraries
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

try:
    import zlib
    HAS_ZLIB = True
except ImportError:
    HAS_ZLIB = False


def get_memory_mb():
    """Get current memory usage in MB."""
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)


def format_time(seconds):
    """Format seconds as human-readable string."""
    if seconds < 1:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        return f"{seconds/60:.1f}min"


def locate_files(directory, pattern="*.json"):
    """Find all relevant files in this directory tree."""
    allfiles = []
    for path, _, files in os.walk(directory):
        for name in files:
            if fnmatch(name, pattern):
                allfiles.append(os.path.join(path, name))
    return allfiles


def profile_file_discovery(directory):
    """Profile file discovery."""
    print("\n=== PHASE 1: File Discovery ===")
    start = time.time()
    files = locate_files(directory)
    elapsed = time.time() - start
    print(f"  Found {len(files):,} files in {format_time(elapsed)}")
    print(f"  Rate: {len(files)/elapsed:,.0f} files/sec")
    return files


def decompress_file(filepath):
    """Decompress a single file, return (filepath, data, elapsed)."""
    start = time.time()
    with gzip.open(filepath, 'rb') as f:
        data = f.read()
    return filepath, data, time.time() - start


def profile_decompression(files, sample_size=1000):
    """Profile gzip decompression."""
    print(f"\n=== PHASE 2: Gzip Decompression (sample of {sample_size} files) ===")
    sample = files[:sample_size]

    # Sequential decompression
    start = time.time()
    total_compressed = 0
    total_uncompressed = 0
    for f in sample:
        total_compressed += os.path.getsize(f)
        with gzip.open(f, 'rb') as fd:
            data = fd.read()
            total_uncompressed += len(data)
    seq_time = time.time() - start

    compression_ratio = total_compressed / total_uncompressed if total_uncompressed else 0

    print(f"  Sequential: {format_time(seq_time)}")
    print(f"  Throughput: {total_uncompressed/seq_time/1e6:.1f} MB/s uncompressed")
    print(f"  Compression ratio: {compression_ratio:.2%} ({total_compressed/1e6:.1f}MB -> {total_uncompressed/1e6:.1f}MB)")

    # Parallel decompression with threads
    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(decompress_file, sample))
    thread_time = time.time() - start
    print(f"  Threaded (4 workers): {format_time(thread_time)} ({seq_time/thread_time:.1f}x speedup)")

    # Parallel decompression with processes
    start = time.time()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(decompress_file, sample))
    proc_time = time.time() - start
    print(f"  Multiprocess (4 workers): {format_time(proc_time)} ({seq_time/proc_time:.1f}x speedup)")

    # Estimate full dataset
    estimated_full = seq_time * len(files) / sample_size
    print(f"\n  Estimated full dataset (sequential): {format_time(estimated_full)}")
    print(f"  Estimated full dataset (4 processes): {format_time(estimated_full/4)}")

    return seq_time, total_uncompressed


def profile_json_parsing(files, sample_size=1000):
    """Profile JSON parsing."""
    print(f"\n=== PHASE 3: JSON Parsing (sample of {sample_size} files) ===")
    sample = files[:sample_size]

    # Load raw data first
    raw_data = []
    for f in sample:
        with gzip.open(f, 'rb') as fd:
            raw_data.append(fd.read())

    # Profile stdlib json
    start = time.time()
    parsed = []
    for data in raw_data:
        parsed.append(json.loads(data))
    json_time = time.time() - start
    total_bytes = sum(len(d) for d in raw_data)
    print(f"  stdlib json: {format_time(json_time)} ({total_bytes/json_time/1e6:.1f} MB/s)")

    # Profile orjson if available
    if HAS_ORJSON:
        start = time.time()
        parsed = []
        for data in raw_data:
            parsed.append(orjson.loads(data))
        orjson_time = time.time() - start
        print(f"  orjson: {format_time(orjson_time)} ({total_bytes/orjson_time/1e6:.1f} MB/s, {json_time/orjson_time:.1f}x faster)")
    else:
        print("  orjson: not installed (pip install orjson for ~5x speedup)")

    return json_time


def profile_data_structure_building(files, sample_size=500):
    """Profile the parse_readsb_json function."""
    print(f"\n=== PHASE 4: Data Structure Building (sample of {sample_size} files) ===")

    # Import the actual parsing function
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from lib import readsb_parse

    sample = files[:sample_size]

    # Load and parse JSON
    parsed_json = []
    for f in sample:
        with gzip.open(f, 'rb') as fd:
            parsed_json.append(json.loads(fd.read()))

    # Profile data structure building
    tracemalloc.start()
    gc.collect()
    mem_before = tracemalloc.get_traced_memory()[0]

    start = time.time()
    allpoints = {}
    total_trace_points = 0
    for j in parsed_json:
        total_trace_points += len(j.get('trace', []))
        readsb_parse.parse_readsb_json(j, allpoints)
    build_time = time.time() - start

    gc.collect()
    mem_after = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()

    mem_used_mb = (mem_after - mem_before) / (1024 * 1024)

    print(f"  Time: {format_time(build_time)}")
    print(f"  Trace points processed: {total_trace_points:,}")
    print(f"  Rate: {total_trace_points/build_time:,.0f} points/sec")
    print(f"  Memory used: {mem_used_mb:.1f} MB for {sample_size} files")
    print(f"  Unique timestamps: {len(allpoints):,}")

    # Estimate full dataset memory
    estimated_memory = mem_used_mb * len(files) / sample_size
    print(f"\n  Estimated full dataset memory: {estimated_memory/1024:.1f} GB")

    return build_time, mem_used_mb


def profile_full_pipeline(files, sample_size=200):
    """Profile the complete pipeline end-to-end."""
    print(f"\n=== PHASE 5: Full Pipeline (sample of {sample_size} files) ===")

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from lib import readsb_parse

    sample = files[:sample_size]

    gc.collect()
    tracemalloc.start()

    timings = defaultdict(float)

    allpoints = {}
    for f in sample:
        # File I/O + decompression
        t0 = time.time()
        with gzip.open(f, 'rb') as fd:
            raw = fd.read()
        timings['decompress'] += time.time() - t0

        # JSON parsing
        t0 = time.time()
        j = json.loads(raw)
        timings['json_parse'] += time.time() - t0

        # Data structure building
        t0 = time.time()
        readsb_parse.parse_readsb_json(j, allpoints)
        timings['build_struct'] += time.time() - t0

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    total_time = sum(timings.values())

    print(f"  Total time: {format_time(total_time)}")
    print(f"  Breakdown:")
    for phase, t in sorted(timings.items(), key=lambda x: -x[1]):
        pct = t / total_time * 100
        print(f"    {phase}: {format_time(t)} ({pct:.1f}%)")

    print(f"\n  Peak memory: {peak/1024/1024:.1f} MB")
    print(f"  Estimated full dataset time: {format_time(total_time * len(files) / sample_size)}")
    print(f"  Estimated full dataset memory: {peak/1024/1024 * len(files) / sample_size / 1024:.1f} GB")


def main():
    parser = argparse.ArgumentParser(description='Profile trace file reading')
    parser.add_argument('directory', help='Directory containing trace files')
    parser.add_argument('--sample-size', type=int, default=500,
                        help='Number of files to sample for profiling')
    args = parser.parse_args()

    print("=" * 60)
    print("TRACE FILE READING PROFILER")
    print("=" * 60)
    print(f"Directory: {args.directory}")
    print(f"Sample size: {args.sample_size}")
    print(f"orjson available: {HAS_ORJSON}")
    print(f"Initial memory: {get_memory_mb():.1f} MB")

    files = profile_file_discovery(args.directory)

    if not files:
        print("No files found!")
        return

    profile_decompression(files, args.sample_size)
    profile_json_parsing(files, args.sample_size)
    profile_data_structure_building(files, min(args.sample_size, 500))
    profile_full_pipeline(files, min(args.sample_size, 200))

    print("\n" + "=" * 60)
    print("SUMMARY & RECOMMENDATIONS")
    print("=" * 60)
    print("""
Based on the profiling results above:

1. If DECOMPRESSION is the bottleneck:
   - Use multiprocessing (ProcessPoolExecutor) for parallel decompression
   - Consider pre-decompressing files to a temp directory
   - Or convert to a more efficient format (Parquet, etc.)

2. If JSON PARSING is the bottleneck:
   - Install orjson: pip install orjson (typically 5-10x faster)
   - Consider pre-parsing to a binary format

3. If MEMORY is the bottleneck (most likely for 60K files):
   - Implement streaming processing instead of loading all data
   - Process in time-window chunks (e.g., 1 hour at a time)
   - Use generators instead of building giant dicts

4. For repeated analysis of the same data:
   - Pre-convert to Parquet or SQLite with proper indexing
   - This is a one-time cost that makes all subsequent reads near-instant
""")


if __name__ == '__main__':
    main()

# Spatial Grid Optimization for Batch Processing

## Overview

The spatial grid optimization dramatically reduces rule processing overhead during the batch pipeline's shard pass by using grid-based spatial indexing. Instead of checking all N airport rules for every point, the system now checks only the ~1-3 airports that are actually nearby.

## Performance Impact

**Before optimization:**
- Shard pass: 942 seconds (22% of pipeline)
- 85M points × 99 airports = 8.4 billion bbox checks

**After optimization (estimated):**
- Shard pass: ~20-40 seconds (40-50x speedup)
- 85M points × ~1.5 airports = 127 million bbox checks (98% reduction)

## How It Works

### 1. Grid Indexing (1-degree cells ≈ 60nm)

The system divides the globe into 1-degree grid cells and maps each cell to the list of airport rules whose coverage area (latlongring radius) intersects that cell.

```
Grid cell (37, -122) → [KSJC, KPAO, KSQL]  (3 Bay Area airports)
Grid cell (40, -105) → [KAPA]               (1 Colorado airport)
Grid cell (0, 0)     → []                   (no airports in mid-ocean)
```

### 2. Point-to-Rules Lookup

For each incoming ADS-B point at (lat, lon):
1. Compute grid cell: `(floor(lat), floor(lon))`
2. Look up candidate airports in that cell (typically 0-3)
3. Only check those airports' rules

### 3. Integration

The optimization is implemented in [`src/adsb_actions/rules_optimizations.py`](src/adsb_actions/rules_optimizations.py) with three main functions:

- **`build_rule_list_with_bboxes()`**: Pre-computes bounding boxes for all latlongring rules (always enabled)
- **`build_latlongring_spatial_grid()`**: Builds the grid→rules mapping
- **`initialize_rule_optimizations()`**: Main entry point called from `Rules.__init__()`

## Usage

### Automatic (Recommended)

The optimization is **enabled by default** in the batch pipeline's shard pass. No changes needed.

```bash
# Spatial grid is automatically used
python src/tools/batch_los_pipeline.py \
    --start-date 01/15/26 --end-date 01/15/26 \
    --airports examples/busiest_nontowered.txt
```

### Manual Control

Disable for debugging/testing:

```bash
# Disable spatial grid optimization
python src/analyzers/prox_analyze_from_files.py \
    --yaml config.yaml \
    --sorted-file data/global.gz \
    --no-spatial-grid
```

### Programmatic Usage

```python
from adsb_actions.adsbactions import AdsbActions

# Enable spatial grid (batch processing with many rules)
adsb = AdsbActions(
    yaml_file='config.yaml',
    use_spatial_grid=True,  # Default: False
    grid_size_deg=1.0       # Default: 1.0 degree ≈ 60nm
)

# Disable (streaming mode or few rules)
adsb = AdsbActions(
    yaml_file='config.yaml',
    use_spatial_grid=False
)
```

## When to Use

### ✅ Enable Spatial Grid:
- **Batch processing** with sorted files (`--sorted-file`)
- **Many airport rules** (>20 airports)
- **Global datasets** (processing all U.S. or worldwide data)
- **Shard pass** in batch_los_pipeline

### ❌ Disable Spatial Grid:
- **Streaming mode** (live network data)
- **Few rules** (<5 airports, overhead > benefit)
- **Single-airport analysis** (no spatial filtering needed)
- **Debugging** rule matching issues

## Architecture

```
rules.py                       rules_optimizations.py
┌─────────────────────┐       ┌──────────────────────────────┐
│ Rules.__init__()    │──────▶│ initialize_rule_optimizations│
│                     │       │                              │
│ - Validate rules    │       │ 1. build_rule_list_with_bboxes│
│ - Call optimizer    │       │    (always, minimal overhead) │
│ - Store grid        │       │                              │
│                     │       │ 2. build_spatial_grid        │
│ process_flight()    │       │    (optional, batch mode)    │
│ - Get candidates◀───────────│                              │
│ - Check only ~1-3   │       │ get_candidate_rule_indices   │
│   airports          │       │ - Grid lookup O(1)           │
└─────────────────────┘       └──────────────────────────────┘
```

## Verification

Test the optimization:

```bash
# Run with spatial grid (default)
time python src/tools/batch_los_pipeline.py \
    --start-date 01/15/26 --end-date 01/15/26 \
    --airports examples/busiest_nontowered.txt \
    --timing-output timing_with_grid.json

# Run without spatial grid (comparison)
time python src/analyzers/prox_analyze_from_files.py \
    --yaml data/shard_011526.yaml \
    --sorted-file data/global_011526.gz \
    --no-spatial-grid
```

Compare the `shard_pass` timing in the output.

## Technical Details

### Grid Cell Size Trade-offs

- **Smaller cells (0.5°)**: Fewer rules per cell, more precise → slightly faster lookup, more memory
- **Larger cells (2.0°)**: More rules per cell, less precise → slightly slower lookup, less memory
- **Default (1.0° ≈ 60nm)**: Good balance for typical airport spacing

### Memory Usage

- Grid index: ~10-50 KB for 99 airports (negligible)
- Per airport: ~100 bytes overhead
- No impact on Location/Flight object memory

### Edge Cases

- **Date line crossing**: Grid cells wrap correctly (handled by floor())
- **Polar regions**: Works correctly but cells are physically smaller
- **Overlapping airports**: Multiple airports in same cell handled correctly
- **Empty cells**: Most grid cells are empty (oceans, rural areas) → no overhead

## Future Enhancements

Potential improvements (not currently implemented):

1. **Dynamic grid sizing**: Adjust cell size based on airport density
2. **Multi-level grid**: Coarse grid (10°) → fine grid (1°) for very dense areas
3. **Caching**: Memoize grid lookups for repeated coordinates
4. **Statistics**: Track grid hit rates and efficiency metrics

## Related Files

- [`src/adsb_actions/rules_optimizations.py`](src/adsb_actions/rules_optimizations.py) - Optimization implementation
- [`src/adsb_actions/rules.py`](src/adsb_actions/rules.py) - Rules engine integration
- [`src/adsb_actions/adsbactions.py`](src/adsb_actions/adsbactions.py) - API parameter handling
- [`src/analyzers/prox_analyze_from_files.py`](src/analyzers/prox_analyze_from_files.py) - CLI flag handling
- [`src/tools/batch_los_pipeline.py`](src/tools/batch_los_pipeline.py) - Batch pipeline orchestration

## Questions?

See the plan document at `~/.claude/plans/iterative-herding-engelbart.md` for the full optimization strategy and profiling data.

"""
Optimizations for rules.py processing, particularly for batch shard operations.

Provides spatial indexing to reduce the number of latlongring checks needed
when processing global datasets with many airports.
"""

import math
import logging
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from .geo_helpers import nm_to_lat_lon_offsets

logger = logging.getLogger(__name__)


def build_rule_list_with_bboxes(rules_dict: dict) -> List[Tuple[Optional[Tuple], str, dict]]:
    """Build optimized flat list of rules with pre-computed bounding boxes.

    This is a performance optimization that pre-computes bounding boxes for
    latlongring rules, allowing fast bbox rejection before expensive polygon
    checks. Called once during Rules.__init__().

    Args:
        rules_dict: Dictionary of rules from YAML (e.g., yaml_data['rules'])

    Returns:
        List of (bbox_or_None, rule_name, rule_value) tuples where:
        - bbox is (min_lat, max_lat, min_lon, max_lon) for latlongring rules
        - bbox is None for non-latlongring rules

    Example:
        >>> rules_list = build_rule_list_with_bboxes(yaml_data['rules'])
        >>> for bbox, rule_name, rule_value in rules_list:
        >>>     if bbox and (lat < bbox[0] or lat > bbox[1]):
        >>>         continue  # Fast rejection
    """
    rule_list = []
    for rule_name, rule_value in rules_dict.items():
        conds = rule_value['conditions']
        if 'latlongring' in conds:
            cv = conds['latlongring']  # cv is [radius_nm, center_lat, center_lon]
            lat_offset, lon_offset = nm_to_lat_lon_offsets(cv[0], cv[1])
            bbox = (cv[1] - lat_offset, cv[1] + lat_offset,
                    cv[2] - lon_offset, cv[2] + lon_offset)
        else:
            bbox = None
        rule_list.append((bbox, rule_name, rule_value))
    return rule_list


def build_latlongring_spatial_grid(
    rules_list: List[Tuple[Optional[Tuple], str, dict]],
    grid_size_deg: float = .3
) -> Dict[Tuple[int, int], List[int]]:
    """Build spatial grid index for latlongring rules.

    Maps grid cells (coarse lat/lon buckets) to rule indices that might match
    points in those cells. This allows checking only ~1-3 rules per point
    instead of all N rules.

    Args:
        rules_list: List of (bbox, rule_name, rule_body) tuples from Rules._rule_list
        grid_size_deg: Grid cell size in degrees (default: 1 degree ≈ 60nm)

    Returns:
        Dict mapping (grid_lat, grid_lon) -> list of rule indices

    Example:
        >>> grid = build_latlongring_spatial_grid(rules._rule_list)
        >>> # For a point at (37.5, -122.0):
        >>> grid_cell = (37, -122)
        >>> candidate_rule_indices = grid.get(grid_cell, [])
        >>> # Now only check these ~2 rules instead of all 99!
    """
    grid = defaultdict(list)

    for rule_idx, (bbox, rule_name, rule_body) in enumerate(rules_list):
        # Only index latlongring rules (bbox is pre-computed)
        if bbox is None:
            continue

        # bbox format: (min_lat, max_lat, min_lon, max_lon)
        min_lat, max_lat, min_lon, max_lon = bbox

        # Find all grid cells that intersect this rule's bbox
        min_grid_lat = int(math.floor(min_lat / grid_size_deg))
        max_grid_lat = int(math.ceil(max_lat / grid_size_deg))
        min_grid_lon = int(math.floor(min_lon / grid_size_deg))
        max_grid_lon = int(math.ceil(max_lon / grid_size_deg))

        # Add this rule to all intersecting grid cells
        for grid_lat in range(min_grid_lat, max_grid_lat + 1):
            for grid_lon in range(min_grid_lon, max_grid_lon + 1):
                grid[(grid_lat, grid_lon)].append(rule_idx)

    return dict(grid)


def get_candidate_rule_indices(
    lat: float,
    lon: float,
    spatial_grid: Dict[Tuple[int, int], List[int]],
    grid_size_deg: float = 1.0
) -> List[int]:
    """Get list of rule indices that might match this point.

    Args:
        lat: Point latitude
        lon: Point longitude
        spatial_grid: Pre-computed grid from build_latlongring_spatial_grid()
        grid_size_deg: Grid cell size (must match grid construction)

    Returns:
        List of rule indices to check (typically 0-3 rules)
    """
    grid_lat = int(math.floor(lat / grid_size_deg))
    grid_lon = int(math.floor(lon / grid_size_deg))
    return spatial_grid.get((grid_lat, grid_lon), [])


def compute_grid_stats(spatial_grid: Dict[Tuple[int, int], List[int]]) -> dict:
    """Compute statistics about the spatial grid for debugging/analysis.

    Returns:
        Dict with stats like total cells, avg rules per cell, etc.
    """
    if not spatial_grid:
        return {'total_cells': 0, 'avg_rules_per_cell': 0, 'max_rules_per_cell': 0}

    rules_per_cell = [len(indices) for indices in spatial_grid.values()]

    return {
        'total_cells': len(spatial_grid),
        'avg_rules_per_cell': sum(rules_per_cell) / len(rules_per_cell),
        'max_rules_per_cell': max(rules_per_cell),
        'min_rules_per_cell': min(rules_per_cell),
    }


def initialize_rule_optimizations(
    rules_dict: dict,
    use_optimizations: bool = False,
    grid_size_deg: float = 1.0
) -> Tuple[List[Tuple[Optional[Tuple], str, dict]], Optional[Dict[Tuple[int, int], List[int]]]]:
    """Initialize rule processing optimizations (bbox pre-computation + spatial grid).

    This is the main entry point called from Rules.__init__() to set up both
    optimization layers:
    1. Bbox pre-computation (enabled when use_optimizations=True)
    2. Spatial grid indexing (enabled when use_optimizations=True)

    Args:
        rules_dict: Dictionary of rules from YAML
        use_optimizations: Enable both bbox and spatial grid optimizations (for batch processing)
        grid_size_deg: Grid cell size in degrees (default: 1.0 ≈ 60nm)

    Returns:
        Tuple of (rule_list, spatial_grid) where:
        - rule_list: List of (bbox, rule_name, rule_value) tuples (or simple list if optimizations disabled)
        - spatial_grid: Dict mapping grid cells to rule indices (or None if optimizations disabled)

    Example:
        >>> rule_list, spatial_grid = initialize_rule_optimizations(
        ...     yaml_data['rules'], use_optimizations=True)
    """
    if not use_optimizations:
        # No optimizations: return simple rule list without bbox pre-computation
        rule_list = [(None, name, body) for name, body in rules_dict.items()]
        return rule_list, None

    # Build flat rule list with pre-computed bboxes
    rule_list = build_rule_list_with_bboxes(rules_dict)

    # Build spatial grid index
    spatial_grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=grid_size_deg)

    # Log grid statistics
    latlongring_count = sum(1 for bbox, _, _ in rule_list if bbox is not None)
    if latlongring_count > 0:
        logger.info(f"Built spatial grid with {len(spatial_grid)} cells "
                   f"for {latlongring_count} latlongring rules "
                   f"(grid_size={grid_size_deg}°)")
    else:
        logger.warning("Spatial grid enabled but no latlongring rules found")

    return rule_list, spatial_grid

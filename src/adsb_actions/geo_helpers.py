"""Geographic calculation helpers for ADS-B processing."""

import math


def nm_to_lat_lon_offsets(radius_nm: float, center_lat: float) -> tuple[float, float]:
    """Convert a radius in nautical miles to lat/lon degree offsets.

    Args:
        radius_nm: Radius in nautical miles
        center_lat: Center latitude in degrees (used for longitude correction)

    Returns:
        Tuple of (lat_offset, lon_offset) in degrees

    Examples:
        >>> lat_off, lon_off = nm_to_lat_lon_offsets(60.0, 0.0)  # 60nm at equator
        >>> abs(lat_off - 1.0) < 0.01  # ~1 degree latitude
        True
        >>> abs(lon_off - 1.0) < 0.01  # ~1 degree longitude at equator
        True
    """
    lat_offset = radius_nm / 60.0  # 1 degree latitude ≈ 60 nm everywhere
    lon_offset = radius_nm / (60.0 * math.cos(math.radians(center_lat)))  # Adjusted for latitude compression
    return lat_offset, lon_offset

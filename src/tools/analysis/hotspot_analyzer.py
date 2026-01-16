"""
Hotspot analysis for LOS (Loss of Separation) events.

Provides kernel density estimation (KDE) and clustering algorithms
to identify spatial concentrations of proximity violations.
"""

from typing import List, Tuple, Optional
import numpy as np
from scipy.stats import gaussian_kde

class HotspotAnalyzer:
    """
    Analyzes spatial patterns in LOS events to identify hotspots.

    Uses Kernel Density Estimation (KDE) to compute smooth density
    surfaces showing where events are concentrated.
    """

    def __init__(self, points: List[Tuple[float, float]],
                 bounds: Optional[Tuple[float, float, float, float]] = None):
        """
        Initialize the hotspot analyzer.

        Args:
            points: List of (lat, lon) tuples
            bounds: Optional (ll_lat, ur_lat, ll_lon, ur_lon) for grid bounds.
                   If None, computed from points with padding.
        """
        if len(points) < 2:
            raise ValueError("Need at least 2 points for hotspot analysis")

        self.points = points
        self.lats = np.array([p[0] for p in points])
        self.lons = np.array([p[1] for p in points])

        # Check spatial variation
        if np.std(self.lats) < 1e-10 or np.std(self.lons) < 1e-10:
            raise ValueError("Points lack spatial variation")

        # Set bounds
        if bounds:
            self.ll_lat, self.ur_lat, self.ll_lon, self.ur_lon = bounds
        else:
            # Auto-compute with 10% padding
            lat_range = self.lats.max() - self.lats.min()
            lon_range = self.lons.max() - self.lons.min()
            self.ll_lat = self.lats.min() - 0.1 * lat_range
            self.ur_lat = self.lats.max() + 0.1 * lat_range
            self.ll_lon = self.lons.min() - 0.1 * lon_range
            self.ur_lon = self.lons.max() + 0.1 * lon_range

    def compute_kde_heatmap(self,
                           bandwidth: Optional[float] = None,
                           grid_size: int = 50,
                           threshold: float = 0.1) -> List[List[float]]:
        """
        Compute KDE-based heatmap data.

        Uses Gaussian kernel density estimation to identify spatial
        concentrations. Returns data suitable for folium.plugins.HeatMap.

        Args:
            bandwidth: KDE bandwidth in degrees. If None, uses Scott's rule.
                      Smaller values (0.01-0.02) = sharper hotspots.
                      Larger values (0.05-0.10) = smoother distributions.
            grid_size: Resolution of evaluation grid (default 50x50)
            threshold: Filter out density values below this (0-1 range)

        Returns:
            List of [lat, lon, intensity] triplets for HeatMap plugin

        Raises:
            ValueError: If insufficient data or no spatial variation
        """
        # Stack coordinates for KDE: shape (2, n_points)
        coords = np.vstack([self.lats, self.lons])

        # Compute KDE
        if bandwidth:
            kde = gaussian_kde(coords, bw_method=bandwidth)
        else:
            # Use Scott's rule (automatic bandwidth selection)
            kde = gaussian_kde(coords, bw_method='scott')

        # Create evaluation grid
        lat_range = np.linspace(self.ll_lat, self.ur_lat, grid_size)
        lon_range = np.linspace(self.ll_lon, self.ur_lon, grid_size)
        # Use indexing='ij' so lat varies along first dimension (rows) and lon along second (columns)
        lat_grid, lon_grid = np.meshgrid(lat_range, lon_range, indexing='ij')
        grid_coords = np.vstack([lat_grid.ravel(), lon_grid.ravel()])

        # Evaluate KDE on grid
        density = kde(grid_coords)

        # Normalize to 0-1 range
        density = (density - density.min()) / (density.max() - density.min() + 1e-10)

        # Build heatmap data, filtering low-density points
        heatmap_data = []
        for lat, lon, dens in zip(lat_grid.ravel(), lon_grid.ravel(), density):
            if dens > threshold:
                heatmap_data.append([float(lat), float(lon), float(dens)])

        return heatmap_data

    def get_statistics(self) -> dict:
        """
        Get summary statistics about the point distribution.

        Returns:
            Dict with keys: n_points, lat_range, lon_range, center
        """
        return {
            'n_points': len(self.points),
            'lat_range': (float(self.lats.min()), float(self.lats.max())),
            'lon_range': (float(self.lons.min()), float(self.lons.max())),
            'center': (float(self.lats.mean()), float(self.lons.mean())),
            'lat_std': float(np.std(self.lats)),
            'lon_std': float(np.std(self.lons))
        }


def compute_hotspot_heatmap(points: List[Tuple[float, float]],
                           bounds: Optional[Tuple[float, float, float, float]] = None,
                           bandwidth: Optional[float] = None,
                           grid_size: int = 50,
                           threshold: float = 0.1) -> List[List[float]]:
    """
    Convenience function to compute KDE heatmap in one call.

    Args:
        points: List of (lat, lon) tuples
        bounds: Optional (ll_lat, ur_lat, ll_lon, ur_lon)
        bandwidth: KDE bandwidth (None = auto)
        grid_size: Grid resolution (number of points per axis)
        threshold: Density threshold for filtering

    Returns:
        List of [lat, lon, intensity] for folium.plugins.HeatMap
        Returns empty list if insufficient data or error.
    """
    try:
        analyzer = HotspotAnalyzer(points, bounds)
        return analyzer.compute_kde_heatmap(bandwidth, grid_size, threshold)
    except ValueError as e:
        print(f"Hotspot analysis skipped: {e}")
        return []

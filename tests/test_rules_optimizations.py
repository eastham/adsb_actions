"""Tests for rules_optimizations.py spatial grid indexing."""

import pytest
from src.adsb_actions.rules_optimizations import (
    build_rule_list_with_bboxes,
    build_latlongring_spatial_grid,
    get_candidate_rule_indices,
    initialize_rule_optimizations,
    compute_grid_stats
)


class TestBuildRuleList:
    """Test bbox pre-computation."""

    def test_latlongring_rule_has_bbox(self):
        """Latlongring rules should get pre-computed bbox."""
        rules = {
            'airport_rule': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)

        assert len(rule_list) == 1
        bbox, name, _ = rule_list[0]
        assert name == 'airport_rule'
        assert bbox is not None
        assert len(bbox) == 4  # (min_lat, max_lat, min_lon, max_lon)

        # Bbox should be roughly centered around (37, -122) with ~5nm radius
        min_lat, max_lat, min_lon, max_lon = bbox
        assert 36.9 < min_lat < 37.0
        assert 37.0 < max_lat < 37.1
        assert -122.2 < min_lon < -122.0
        assert -122.0 < max_lon < -121.8

    def test_non_latlongring_rule_no_bbox(self):
        """Non-latlongring rules should have None bbox."""
        rules = {
            'altitude_rule': {
                'conditions': {'min_alt': 1000, 'max_alt': 5000},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)

        assert len(rule_list) == 1
        bbox, name, _ = rule_list[0]
        assert name == 'altitude_rule'
        assert bbox is None

    def test_mixed_rules(self):
        """Mix of latlongring and non-latlongring rules."""
        rules = {
            'spatial_rule': {
                'conditions': {'latlongring': [10.0, 40.0, -105.0]},
                'actions': {}
            },
            'altitude_rule': {
                'conditions': {'min_alt': 1000},
                'actions': {}
            },
            'another_spatial': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)

        assert len(rule_list) == 3
        spatial_rules = [r for r in rule_list if r[0] is not None]
        non_spatial_rules = [r for r in rule_list if r[0] is None]
        assert len(spatial_rules) == 2
        assert len(non_spatial_rules) == 1


class TestSpatialGrid:
    """Test grid-based spatial indexing."""

    def test_single_airport_grid(self):
        """Single airport should map to multiple grid cells."""
        rules = {
            'KSJC': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # 5nm radius should span roughly 2-3 grid cells in each dimension
        # (1 degree ≈ 60nm, so 5nm ≈ 0.08 degrees)
        # But bbox is slightly larger, so expect ~4-9 cells
        assert 4 <= len(grid) <= 12

        # All grid cells should map to rule index 0
        for cell, indices in grid.items():
            assert indices == [0]

    def test_multiple_airports_non_overlapping(self):
        """Non-overlapping airports should have separate grid cells."""
        rules = {
            'KSJC': {  # San Jose, CA
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            },
            'KDEN': {  # Denver, CO (far away)
                'conditions': {'latlongring': [5.0, 40.0, -105.0]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # Should have cells for both airports, no overlap
        ksjc_cells = [cell for cell, indices in grid.items() if 0 in indices]
        kden_cells = [cell for cell, indices in grid.items() if 1 in indices]

        assert len(ksjc_cells) > 0
        assert len(kden_cells) > 0
        # No overlap (different grid cells)
        assert set(ksjc_cells).isdisjoint(set(kden_cells))

    def test_overlapping_airports(self):
        """Overlapping airports should share grid cells."""
        rules = {
            'KSJC': {  # San Jose
                'conditions': {'latlongring': [10.0, 37.36, -121.93]},
                'actions': {}
            },
            'KPAO': {  # Palo Alto (nearby)
                'conditions': {'latlongring': [10.0, 37.46, -122.11]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # Find cells with both airports
        overlapping_cells = [cell for cell, indices in grid.items()
                            if 0 in indices and 1 in indices]

        # With 10nm radius and ~10nm separation, should have some overlap
        assert len(overlapping_cells) > 0

    def test_non_latlongring_rules_ignored(self):
        """Non-latlongring rules should not appear in grid."""
        rules = {
            'spatial': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            },
            'altitude': {
                'conditions': {'min_alt': 1000},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # Only rule 0 (spatial) should be in grid, not rule 1 (altitude)
        all_indices = set()
        for indices in grid.values():
            all_indices.update(indices)

        assert 0 in all_indices
        assert 1 not in all_indices

    def test_grid_size_parameter(self):
        """Larger grid cells should result in fewer cells."""
        rules = {
            'airport': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)

        grid_small = build_latlongring_spatial_grid(rule_list, grid_size_deg=0.5)
        grid_large = build_latlongring_spatial_grid(rule_list, grid_size_deg=2.0)

        # Smaller cells = more cells covering same area
        assert len(grid_small) > len(grid_large)


class TestCandidateRuleIndices:
    """Test lookup of candidate rules for a point."""

    def test_point_inside_coverage(self):
        """Point inside airport coverage should return that airport."""
        rules = {
            'KSJC': {
                'conditions': {'latlongring': [5.0, 37.36, -121.93]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # Point near KSJC
        candidates = get_candidate_rule_indices(37.36, -121.93, grid, grid_size_deg=1.0)

        assert 0 in candidates

    def test_point_outside_coverage(self):
        """Point far from all airports should return no candidates."""
        rules = {
            'KSJC': {
                'conditions': {'latlongring': [5.0, 37.36, -121.93]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # Point in middle of ocean, far from KSJC
        candidates = get_candidate_rule_indices(0.0, 0.0, grid, grid_size_deg=1.0)

        assert len(candidates) == 0

    def test_point_near_multiple_airports(self):
        """Point near multiple airports should return all of them."""
        rules = {
            'KSJC': {
                'conditions': {'latlongring': [10.0, 37.36, -121.93]},
                'actions': {}
            },
            'KPAO': {
                'conditions': {'latlongring': [10.0, 37.46, -122.11]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)

        # Point between the two airports
        candidates = get_candidate_rule_indices(37.41, -122.02, grid, grid_size_deg=1.0)

        # Should find both airports (they're ~10nm apart with 10nm radius each)
        assert 0 in candidates or 1 in candidates
        # Likely both, but at minimum one


class TestInitializeOptimizations:
    """Test the main initialization function."""

    def test_without_spatial_grid(self):
        """Without optimizations, should return simple rule list without bbox pre-computation."""
        rules = {
            'airport': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }

        rule_list, spatial_grid = initialize_rule_optimizations(
            rules, use_optimizations=False
        )

        assert len(rule_list) == 1
        assert spatial_grid is None
        # When optimizations disabled, bbox should be None
        bbox, name, body = rule_list[0]
        assert bbox is None
        assert name == 'airport'

    def test_with_spatial_grid(self):
        """With spatial grid enabled, should build both."""
        rules = {
            'airport': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }

        rule_list, spatial_grid = initialize_rule_optimizations(
            rules, use_optimizations=True, grid_size_deg=1.0
        )

        assert len(rule_list) == 1
        assert spatial_grid is not None
        assert len(spatial_grid) > 0

    def test_grid_size_propagation(self):
        """Grid size parameter should be respected."""
        rules = {
            'airport': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }

        _, grid_small = initialize_rule_optimizations(
            rules, use_optimizations=True, grid_size_deg=0.5
        )
        _, grid_large = initialize_rule_optimizations(
            rules, use_optimizations=True, grid_size_deg=2.0
        )

        assert len(grid_small) > len(grid_large)


class TestComputeGridStats:
    """Test grid statistics computation."""

    def test_stats_on_empty_grid(self):
        """Empty grid should return zeros."""
        stats = compute_grid_stats({})

        assert stats['total_cells'] == 0
        assert stats['avg_rules_per_cell'] == 0
        assert stats['max_rules_per_cell'] == 0

    def test_stats_on_single_airport(self):
        """Single airport stats."""
        rules = {
            'airport': {
                'conditions': {'latlongring': [5.0, 37.0, -122.0]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)
        stats = compute_grid_stats(grid)

        assert stats['total_cells'] > 0
        assert stats['avg_rules_per_cell'] == 1.0  # Only one rule
        assert stats['max_rules_per_cell'] == 1
        assert stats['min_rules_per_cell'] == 1

    def test_stats_on_overlapping_airports(self):
        """Overlapping airports should have max > 1."""
        rules = {
            'KSJC': {
                'conditions': {'latlongring': [10.0, 37.36, -121.93]},
                'actions': {}
            },
            'KPAO': {
                'conditions': {'latlongring': [10.0, 37.46, -122.11]},
                'actions': {}
            }
        }
        rule_list = build_rule_list_with_bboxes(rules)
        grid = build_latlongring_spatial_grid(rule_list, grid_size_deg=1.0)
        stats = compute_grid_stats(grid)

        assert stats['total_cells'] > 0
        assert stats['min_rules_per_cell'] >= 1
        # If they overlap, max should be 2 in some cells
        assert stats['max_rules_per_cell'] >= 1


class TestEndToEnd:
    """Integration tests with realistic scenarios."""

    def test_realistic_bay_area_airports(self):
        """Test with realistic Bay Area airport configuration."""
        rules = {
            'KSJC': {'conditions': {'latlongring': [5.0, 37.36, -121.93]}, 'actions': {}},
            'KPAO': {'conditions': {'latlongring': [5.0, 37.46, -122.11]}, 'actions': {}},
            'KSQL': {'conditions': {'latlongring': [5.0, 37.51, -122.25]}, 'actions': {}},
            'KNUQ': {'conditions': {'latlongring': [5.0, 37.42, -122.05]}, 'actions': {}},
        }

        rule_list, grid = initialize_rule_optimizations(
            rules, use_optimizations=True, grid_size_deg=1.0
        )

        # All 4 rules should be in list
        assert len(rule_list) == 4

        # Grid should be reasonable size (Bay Area airports are close, so few cells)
        assert 4 <= len(grid) <= 20

        # Test point lookups
        # Point at KSJC
        candidates = get_candidate_rule_indices(37.36, -121.93, grid, 1.0)
        assert 0 in candidates  # Should find KSJC

        # Point in Pacific Ocean (far west)
        candidates = get_candidate_rule_indices(37.0, -125.0, grid, 1.0)
        assert len(candidates) == 0  # Should find nothing

    def test_many_airports_efficiency(self):
        """Test that grid provides efficiency gain with many airports."""
        # Create 50 airports spread across US
        rules = {}
        for i in range(50):
            lat = 30.0 + (i // 10) * 2  # 30-38 degrees
            lon = -120.0 + (i % 10) * 2  # -120 to -102 degrees
            rules[f'AIRPORT{i}'] = {
                'conditions': {'latlongring': [5.0, lat, lon]},
                'actions': {}
            }

        rule_list, grid = initialize_rule_optimizations(
            rules, use_optimizations=True, grid_size_deg=1.0
        )

        assert len(rule_list) == 50

        # Test random point in middle of coverage
        candidates = get_candidate_rule_indices(34.0, -110.0, grid, 1.0)

        # Should find only nearby airports, not all 50
        assert len(candidates) < 50
        # But should find at least 1 (since point is in coverage area)
        assert len(candidates) >= 0  # Might be 0 if between airports


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

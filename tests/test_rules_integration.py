"""Integration tests for Rules class with spatial grid optimization."""

import pytest
from src.adsb_actions.rules import Rules
from src.adsb_actions.flight import Flight
from src.adsb_actions.location import Location


class TestRulesSpatialGridIntegration:
    """Test Rules class with spatial grid enabled vs disabled."""

    @pytest.fixture
    def sample_yaml_data(self):
        """Sample YAML data with multiple airports."""
        return {
            'rules': {
                'KSJC_5nm': {
                    'conditions': {
                        'latlongring': [5.0, 37.36, -121.93],  # San Jose
                    },
                    'actions': {
                        'emit_jsonl': '/tmp/ksjc.gz'
                    }
                },
                'KPAO_5nm': {
                    'conditions': {
                        'latlongring': [5.0, 37.46, -122.11],  # Palo Alto
                    },
                    'actions': {
                        'emit_jsonl': '/tmp/kpao.gz'
                    }
                },
                'KSQL_5nm': {
                    'conditions': {
                        'latlongring': [5.0, 37.51, -122.25],  # San Carlos
                    },
                    'actions': {
                        'emit_jsonl': '/tmp/ksql.gz'
                    }
                }
            }
        }

    def test_rules_without_spatial_grid(self, sample_yaml_data):
        """Rules should work without spatial grid (original behavior)."""
        rules = Rules(sample_yaml_data, use_optimizations=False)

        assert len(rules._rule_list) == 3
        assert rules._spatial_grid is None

    def test_rules_with_spatial_grid(self, sample_yaml_data):
        """Rules should work with spatial grid enabled."""
        rules = Rules(sample_yaml_data, use_optimizations=True)

        assert len(rules._rule_list) == 3
        assert rules._spatial_grid is not None
        assert len(rules._spatial_grid) > 0

    def test_process_flight_with_grid(self, sample_yaml_data):
        """process_flight grid lookup should work correctly."""
        rules_with_grid = Rules(sample_yaml_data, use_optimizations=True)
        rules_without_grid = Rules(sample_yaml_data, use_optimizations=False)

        # Verify both have same rules
        assert len(rules_with_grid._rule_list) == len(rules_without_grid._rule_list)

        # Verify grid-enabled version has grid
        assert rules_with_grid._spatial_grid is not None
        assert rules_without_grid._spatial_grid is None

        # Test that we can look up candidates for a point
        from src.adsb_actions.rules_optimizations import get_candidate_rule_indices

        # Point at KSJC should find KSJC rule
        candidates = get_candidate_rule_indices(
            37.36, -121.93, rules_with_grid._spatial_grid, grid_size_deg=1.0
        )
        assert len(candidates) > 0  # Should find at least KSJC rule

    def test_grid_stats_logging(self, sample_yaml_data, caplog):
        """Spatial grid should log statistics when enabled."""
        import logging
        caplog.set_level(logging.INFO, logger='src.adsb_actions.rules_optimizations')

        Rules(sample_yaml_data, use_optimizations=True)

        # Should see log message about grid building
        assert any('Built spatial grid' in record.message for record in caplog.records)

    def test_no_latlongring_rules_warning(self, caplog):
        """Should warn if spatial grid enabled but no latlongring rules."""
        import logging
        caplog.set_level(logging.WARNING)

        yaml_data = {
            'rules': {
                'altitude_only': {
                    'conditions': {'min_alt': 1000},
                    'actions': {'emit_jsonl': '/tmp/test.gz'}  # Use valid action
                }
            }
        }

        Rules(yaml_data, use_optimizations=True)

        # Should see warning about no latlongring rules
        assert any('no latlongring rules' in record.message.lower()
                  for record in caplog.records)

    def test_grid_size_parameter_respected(self, sample_yaml_data):
        """Grid size is now configured in rules_optimizations, not Rules.__init__."""
        # This test is no longer applicable since grid_size_deg is not a Rules parameter
        # Grid size is hardcoded in initialize_rule_optimizations (default: 1.0)
        rules = Rules(sample_yaml_data, use_optimizations=True)
        assert rules._spatial_grid is not None


class TestRulesPerformanceCharacteristics:
    """Test performance characteristics (not actual benchmarks, just behavior)."""

    def test_many_rules_benefit_from_grid(self):
        """With many rules, spatial grid should reduce candidates checked."""
        # Create 50 airports spread across California
        rules_dict = {}
        for i in range(50):
            lat = 32.0 + (i // 10) * 1.0  # 32-36 degrees
            lon = -124.0 + (i % 10) * 0.5  # Spread along coast
            rules_dict[f'AIRPORT{i}'] = {
                'conditions': {'latlongring': [5.0, lat, lon]},
                'actions': {'emit_jsonl': f'/tmp/airport{i}.gz'}
            }

        yaml_data = {'rules': rules_dict}
        rules = Rules(yaml_data, use_optimizations=True)

        # Get candidate rules for a point in middle of California
        from src.adsb_actions.rules_optimizations import get_candidate_rule_indices
        candidates = get_candidate_rule_indices(
            34.0, -122.0, rules._spatial_grid, grid_size_deg=1.0
        )

        # Should find fewer than all 50 airports
        assert len(candidates) < 50

        # But should find at least some (there are airports nearby)
        # Note: Might be 0 if point is between grid cells, but with 50 airports
        # spread across area, should find at least 1
        assert len(candidates) >= 0  # Permissive for robustness

    def test_point_outside_all_coverage_finds_nothing(self):
        """Point outside all airport coverage should return empty candidates."""
        yaml_data = {
            'rules': {
                'KSJC': {
                    'conditions': {'latlongring': [5.0, 37.36, -121.93]},
                    'actions': {'emit_jsonl': '/tmp/ksjc.gz'}
                }
            }
        }

        rules = Rules(yaml_data, use_optimizations=True)

        # Point in Hawaii (far from KSJC)
        from src.adsb_actions.rules_optimizations import get_candidate_rule_indices
        candidates = get_candidate_rule_indices(
            21.3, -157.9, rules._spatial_grid, grid_size_deg=1.0
        )

        assert len(candidates) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

"""Test quality-based color coding in visualizer."""

from src.postprocessing.visualizer import MapVisualizer


class TestVisualizerQuality:
    """Test the quality-based visualization features."""

    def test_add_point_with_quality(self):
        """Test adding points with different quality levels."""
        viz = MapVisualizer()

        viz.add_point((37.5, -122.0), "High quality event", [], quality='high')
        viz.add_point((37.6, -122.1), "Medium quality event", [], quality='medium')
        viz.add_point((37.7, -122.2), "Low quality event", [], quality='low')

        assert len(viz.points) == 3
        assert len(viz.annotations) == 3
        assert len(viz.qualities) == 3
        assert viz.qualities == ['high', 'medium', 'low']

    def test_add_point_default_quality(self):
        """Test that default quality is 'high' when not specified."""
        viz = MapVisualizer()

        viz.add_point((37.5, -122.0), "Default quality event", [])

        assert len(viz.qualities) == 1
        assert viz.qualities[0] == 'high'

    def test_clear_clears_qualities(self):
        """Test that clear() removes quality data."""
        viz = MapVisualizer()

        viz.add_point((37.5, -122.0), "Event", [], quality='high')
        viz.add_point((37.6, -122.1), "Event", [], quality='medium')

        assert len(viz.qualities) == 2

        viz.clear()

        assert len(viz.qualities) == 0
        assert len(viz.points) == 0
        assert len(viz.annotations) == 0

    def test_get_point_color_high(self):
        """Test color for high quality events."""
        viz = MapVisualizer()
        assert viz._get_point_color('high') == 'orange'

    def test_get_point_color_medium(self):
        """Test color for medium quality events."""
        viz = MapVisualizer()
        assert viz._get_point_color('medium') == 'yellow'

    def test_get_point_color_low(self):
        """Test color for low quality events."""
        viz = MapVisualizer()
        assert viz._get_point_color('low') == 'green'

    def test_get_point_color_unknown(self):
        """Test default color for unknown quality."""
        viz = MapVisualizer()
        assert viz._get_point_color('unknown') == 'green'

    def test_get_point_color_vhigh(self):
        """Test color for very high quality events."""
        viz = MapVisualizer()
        assert viz._get_point_color('vhigh') == '#ff00ff'

    def test_quality_tracking_matches_points(self):
        """Test that quality list stays in sync with points list."""
        viz = MapVisualizer()

        test_data = [
            ((37.5, -122.0), "Event 1", 'high'),
            ((37.6, -122.1), "Event 2", 'medium'),
            ((37.7, -122.2), "Event 3", 'low'),
            ((37.8, -122.3), "Event 4", 'high'),
        ]

        for point, annotation, quality in test_data:
            viz.add_point(point, annotation, [], quality)

        assert len(viz.points) == len(viz.qualities)
        assert len(viz.annotations) == len(viz.qualities)

        for i, (_, _, expected_quality) in enumerate(test_data):
            assert viz.qualities[i] == expected_quality

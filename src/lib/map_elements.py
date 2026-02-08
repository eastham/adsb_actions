"""HTML and JavaScript elements for map visualization.

This module contains HTML legends, controls, and custom Folium elements
used by the visualizer.
"""

import json

from folium.elements import MacroElement
from jinja2 import Template


LEGEND_HTML = """
    <div id="legend-box" style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 270px;
        height: 190px;
        background-color: white;
        border: 2px solid black;
        z-index: 1000;
        padding: 10px;
        font-size: 14px;
    ">
        <b>YEAR: <a href="2022.html">2022</a>
        <a href="2023.html">2023</a>
        <a href="2024.html">2024</a></b><br/><br/>
        <b>Click on an event for details.</b><br>
        <b>LOS event types:</b><br>
        <i style="background: blue; width: 10px; height: 10px; display: inline-block;"></i> Overtake<br>
        <i style="background: orange; width: 10px; height: 10px; display: inline-block;"></i> T-Bone<br>
        <i style="background: red; width: 10px; height: 10px; display: inline-block;"></i> Head-On<br>
        <i style="background: green; width: 10px; height: 10px; display: inline-block;"></i> Other<br>
        <br>
        <a href="#" onclick="if(typeof showAllPoints==='function'){showAllPoints();}else{location.reload();} return false;" style="font-size:12px;">Restore hidden points</a>
    </div>
    <script>
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                var legend = document.getElementById('legend-box');
                var coordDisplay = document.querySelector('.coord-display');
                if (legend) {
                    legend.style.display = legend.style.display === 'none' ? 'block' : 'none';
                }
                if (coordDisplay) {
                    coordDisplay.style.display = coordDisplay.style.display === 'none' ? 'block' : 'none';
                }
            }
        });
    </script>
    """

STATIC_LEGEND_HTML = """
    <div id="legend-box" style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 270px;
        height: 190px;
        background-color: white;
        border: 2px solid black;
        z-index: 1000;
        padding: 10px;
        font-size: 14px;
    ">
        <b>LOS criteria:</b> within .3nm laterally AND 400 feet vertically<br>
        <br/>
        <b>LOS event types:</b><br>
        <i style="background: blue; width: 10px; height: 10px; display: inline-block;"></i> Overtake<br>
        <i style="background: orange; width: 10px; height: 10px; display: inline-block;"></i> T-Bone<br>
        <i style="background: red; width: 10px; height: 10px; display: inline-block;"></i> Head-On<br>
        <i style="background: green; width: 10px; height: 10px; display: inline-block;"></i> Other<br>
    </div>
    <script>
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                var legend = document.getElementById('legend-box');
                var coordDisplay = document.querySelector('.coord-display');
                if (legend) {
                    legend.style.display = legend.style.display === 'none' ? 'block' : 'none';
                }
                if (coordDisplay) {
                    coordDisplay.style.display = coordDisplay.style.display === 'none' ? 'block' : 'none';
                }
            }
        });
    </script>
    """


class CoordinateDisplay(MacroElement):
    """A Leaflet control that displays current viewport bounds.

    This control shows the latitude and longitude of the four corners
    of the current map viewport, updating dynamically as the user
    zooms or pans the map.
    """

    def __init__(self):
        super(CoordinateDisplay, self).__init__()
        self._template = Template("""
        {% macro script(this, kwargs) %}
            var coordDisplay = L.control({position: 'topright'});
            coordDisplay.onAdd = function(map) {
                this._div = L.DomUtil.create('div', 'coord-display');
                this._div.style.backgroundColor = 'white';
                this._div.style.border = '2px solid black';
                this._div.style.padding = '10px';
                this._div.style.fontFamily = 'monospace';
                this._div.style.fontSize = '12px';
                this._div.style.minWidth = '300px';
                this._div.style.pointerEvents = 'auto';
                this._div.style.userSelect = 'text';
                this._div.style.cursor = 'text';

                // Disable map dragging when interacting with this control
                L.DomEvent.disableClickPropagation(this._div);
                L.DomEvent.disableScrollPropagation(this._div);
                this._div.addEventListener('mousedown', function(e) {
                    L.DomEvent.stopPropagation(e);
                });
                this._div.addEventListener('dblclick', function(e) {
                    L.DomEvent.stopPropagation(e);
                });

                this.update();
                return this._div;
            };

            coordDisplay.update = function() {
                var bounds = {{this._parent.get_name()}}.getBounds();
                var sw = bounds.getSouthWest();
                var ne = bounds.getNorthEast();
                var se = L.latLng(sw.lat, ne.lng);
                var nw = L.latLng(ne.lat, sw.lng);

                this._div.innerHTML = '<b>Current Viewport Bounds: (esc to hide)</b><br>' +
                    'Lower-Left:  lat=' + sw.lat.toFixed(7) + ', lon=' + sw.lng.toFixed(7) + '<br>' +
                    'Upper-Right: lat=' + ne.lat.toFixed(7) + ', lon=' + ne.lng.toFixed(7) + '<br>' +
                    'Lower-Right: lat=' + se.lat.toFixed(7) + ', lon=' + se.lng.toFixed(7) + '<br>' +
                    'Upper-Left:  lat=' + nw.lat.toFixed(7) + ', lon=' + nw.lng.toFixed(7);
            };

            coordDisplay.addTo({{this._parent.get_name()}});

            {{this._parent.get_name()}}.on('moveend', function() { coordDisplay.update(); });
            {{this._parent.get_name()}}.on('zoomend', function() { coordDisplay.update(); });
        {% endmacro %}
        """)


def build_hide_script(points_json, map_name,
                      heatmap_radius=20, heatmap_blur=25,
                      heatmap_min_opacity=0.3):
    """Build JavaScript for hiding/showing map points and rebuilding heatmaps.

    Args:
        points_json: List of [lat, lon] for all points.
        map_name: Folium map JS variable name from m.get_name().
        heatmap_radius: Heatmap point radius.
        heatmap_blur: Heatmap blur amount.
        heatmap_min_opacity: Heatmap minimum opacity.
    """
    return (
        "<script>\n"
        "var hiddenPoints = [];\n"
        "var allPoints = " + json.dumps(points_json) + ";\n"
        "var nativeHeatmapLayer = null;\n"
        "\n"
        "function getMap() {\n"
        "    return " + map_name + ";\n"
        "}\n"
        "\n"
        "function rebuildNativeHeatmap() {\n"
        "    var map = getMap();\n"
        "    if (!map || !nativeHeatmapLayer) return;\n"
        "    var visiblePoints = allPoints.filter(function(_, idx) {\n"
        "        return hiddenPoints.indexOf(idx) === -1;\n"
        "    });\n"
        "    map.removeLayer(nativeHeatmapLayer);\n"
        "    if (visiblePoints.length > 0) {\n"
        "        nativeHeatmapLayer = L.heatLayer(visiblePoints, {\n"
        "            radius: " + str(heatmap_radius) + ",\n"
        "            blur: " + str(heatmap_blur) + ",\n"
        "            minOpacity: " + str(heatmap_min_opacity) + ",\n"
        "            gradient: {0.0: 'blue', 0.3: 'cyan', 0.5: 'lime', "
        "0.7: 'yellow', 0.9: 'orange', 1.0: 'red'}\n"
        "        });\n"
        "        nativeHeatmapLayer.addTo(map);\n"
        "        console.log('Rebuilt heatmap with ' + visiblePoints.length + ' points');\n"
        "    }\n"
        "}\n"
        "\n"
        "function hidePoint(idx) {\n"
        "    var map = getMap();\n"
        "    if (!map) { console.error('Could not find map'); return; }\n"
        "    var found = false;\n"
        "    map.eachLayer(function(layer) {\n"
        "        var content = null;\n"
        "        if (layer._popup) {\n"
        "            var popupContent = layer._popup._content;\n"
        "            if (popupContent) {\n"
        "                if (typeof popupContent === 'string') {\n"
        "                    content = popupContent;\n"
        "                } else if (popupContent.innerHTML) {\n"
        "                    content = popupContent.innerHTML;\n"
        "                } else if (popupContent.outerHTML) {\n"
        "                    content = popupContent.outerHTML;\n"
        "                }\n"
        "            }\n"
        "        }\n"
        "        if (content && content.includes('hidePoint(' + idx + ')')) {\n"
        "            map.removeLayer(layer);\n"
        "            hiddenPoints.push(idx);\n"
        "            found = true;\n"
        "            console.log('Hidden point ' + idx);\n"
        "        }\n"
        "    });\n"
        "    if (!found) { console.log('Point ' + idx + ' not found in layers'); }\n"
        "    if (nativeHeatmapLayer) { rebuildNativeHeatmap(); }\n"
        "    map.closePopup();\n"
        "}\n"
        "\n"
        "function showAllPoints() { location.reload(); }\n"
        "</script>\n"
    )


NATIVE_HEATMAP_SCRIPT = """
<script>
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(function() {
        var map = getMap();
        if (map) {
            map.eachLayer(function(layer) {
                if (layer._heat) {
                    nativeHeatmapLayer = layer;
                    console.log('Found native heatmap layer with ' + allPoints.length + ' points');
                }
            });
        }
    }, 500);
});
</script>
"""


_QUALITY_COLORS = {"green": "#2ecc40", "yellow": "#ffdc00", "red": "#ff4136"}
_QUALITY_LABELS = {"green": "Good", "yellow": "Fair", "red": "Poor"}


def _build_quality_indicator(data_quality):
    """Build the data quality indicator HTML for the busyness panel.

    Returns an HTML string (possibly empty if data_quality is None).
    """
    if not data_quality:
        return ''

    score = data_quality.get("score", "yellow")
    color = _QUALITY_COLORS.get(score, "#ffdc00")
    label = _QUALITY_LABELS.get(score, "Unknown")

    completion = data_quality.get("completionRate")
    if completion is not None:
        comp_str = f"{completion:.0%} of low-altitude tracks fully tracked"
    else:
        comp_str = "Insufficient low-altitude track data"

    gap = data_quality.get("medianGapS")
    gap_str = f"Median {gap:.1f}s between position reports" if gap else ""

    num_dates = data_quality.get("numDates", 0)

    tooltip_lines = [comp_str]
    if gap_str:
        tooltip_lines.append(gap_str)
    tooltip_lines.append(f"Based on {num_dates} days of data")
    tooltip = '&#10;'.join(tooltip_lines)

    return (
        '<div id="quality-indicator" style="'
        'border-top: 1px solid #ddd; margin-top: 6px; '
        'padding-top: 6px; cursor: help;" '
        'title="' + tooltip + '">'
        '<span style="display: inline-block; width: 12px; '
        'height: 12px; border-radius: 50%; '
        'background-color: ' + color + '; '
        'border: 1px solid #333; vertical-align: middle;'
        '"></span> '
        '<span style="font-size: 11px; color: #555;">'
        'Data Quality: ' + label
        + ' (mouseover for details)</span></div>\n'
    )


def build_busyness_html(busyness_data, data_quality=None):
    """Build the busyness chart panel HTML/CSS/JS.

    Args:
        busyness_data: Dict with keys 'data', 'globalMax', 'hasWeather',
                       'icao', 'numDates', 'weatherCategories'.
        data_quality: Optional dict with keys 'score', 'completionRate',
                      'medianGapS', 'numDates', etc. If provided, a
                      quality indicator is shown below the chart.
    Returns:
        HTML string to inject via folium.Element().
    """
    busyness_json = json.dumps(busyness_data)
    has_weather = busyness_data.get("hasWeather", False)
    icao = busyness_data.get("icao", "")
    num_dates = busyness_data.get("numDates", 0)

    weather_buttons = ""
    if has_weather:
        weather_buttons = (
            '<div style="margin-bottom: 6px;">'
            '<span style="font-size: 10px; color: #666; margin-right: 4px;">Weather:</span>'
            '<button class="busy-btn weather-btn active" data-val="VMC">VMC</button>'
            '<button class="busy-btn weather-btn" data-val="MVMC">MVMC</button>'
            '<button class="busy-btn weather-btn" data-val="IMC">IMC</button>'
            '<button class="busy-btn weather-btn" data-val="ALL">All</button>'
            '</div>'
        )

    panel_html = (
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>\n'
        '<div id="busyness-panel" style="'
        "position: fixed; bottom: 20px; right: 20px; width: 340px; "
        "background-color: white; border: 2px solid #333; border-radius: 5px; "
        "padding: 10px; font-family: Arial, sans-serif; font-size: 12px; "
        'z-index: 9999; box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">\n'
        '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">'
        '<span style="font-weight: bold; font-size: 13px;">Typical Traffic &mdash; ' + icao + '</span>'
        '<button id="busy-toggle" style="border: none; background: none; cursor: pointer; '
        'font-size: 16px; padding: 0 4px;" title="Minimize">&#x2212;</button>'
        '</div>\n'
        '<div id="busy-controls">'
        '<div style="margin-bottom: 6px;">'
        '<span style="font-size: 10px; color: #666; margin-right: 4px;">Day:</span>'
        '<button class="busy-btn day-btn" data-val="weekday">Weekday</button>'
        '<button class="busy-btn day-btn active" data-val="weekend">Weekend</button>'
        '<button class="busy-btn day-btn" data-val="all">All</button>'
        '</div>'
        + weather_buttons +
        '</div>\n'
        '<div id="busy-chart-wrap" style="position: relative; height: 140px;">'
        '<canvas id="busyness-chart"></canvas>'
        '</div>\n'
        '<div id="busy-subtitle" style="text-align: center; font-size: 10px; color: #888; margin-top: 4px;">'
        'Based on ' + str(num_dates) + ' days of data'
        '</div>\n'
        + _build_quality_indicator(data_quality) +
        '</div>\n'
    )

    style_css = (
        "<style>\n"
        ".busy-btn { border: 1px solid #999; background: #f0f0f0; border-radius: 3px; "
        "padding: 2px 8px; margin-right: 3px; cursor: pointer; font-size: 11px; }\n"
        ".busy-btn.active { background: #4a90d9; color: white; border-color: #357abd; }\n"
        ".busy-btn:hover { background: #ddd; }\n"
        ".busy-btn.active:hover { background: #357abd; }\n"
        "</style>\n"
    )

    default_weather = '"VMC"' if has_weather else '"ALL"'
    chart_js = (
        "<script>\n"
        "(function() {\n"
        "var busynessData = " + busyness_json + ";\n"
        "var hasWeather = busynessData.hasWeather;\n"
        'var currentDay = "weekend";\n'
        "var currentWeather = hasWeather ? " + default_weather + ' : "ALL";\n'
        "var globalMax = busynessData.globalMax;\n"
        "\n"
        "var ctx = document.getElementById('busyness-chart').getContext('2d');\n"
        "var chart = new Chart(ctx, {\n"
        "    type: 'bar',\n"
        "    data: { labels: [], datasets: [{\n"
        "        data: [],\n"
        "        backgroundColor: 'rgba(74, 144, 217, 0.7)',\n"
        "        borderColor: 'rgba(74, 144, 217, 1)',\n"
        "        borderWidth: 1\n"
        "    }]},\n"
        "    options: {\n"
        "        responsive: true,\n"
        "        maintainAspectRatio: false,\n"
        "        plugins: {\n"
        "            legend: { display: false },\n"
        "            tooltip: { callbacks: { label: function(ctx) {\n"
        "                var key = ctx.dataIndex + 5;\n"
        "                if (key >= 24) key -= 24;\n"
        '                var bucketKey = key + ":" + currentDay + ":" + currentWeather;\n'
        "                var entry = busynessData.data[bucketKey];\n"
        "                var n = entry ? entry.n : 0;\n"
        "                return ctx.parsed.y.toFixed(1) + ' aircraft (n=' + n + ')';\n"
        "            }}}\n"
        "        },\n"
        "        scales: {\n"
        "            y: { beginAtZero: true, max: Math.ceil(globalMax * 1.1),\n"
        "                 title: { display: true, text: 'Avg aircraft', font: { size: 10 } } },\n"
        "            x: { title: { display: true, text: 'Hour (UTC)', font: { size: 10 } } }\n"
        "        }\n"
        "    }\n"
        "});\n"
        "\n"
        "function updateChart() {\n"
        "    var labels = [];\n"
        "    var values = [];\n"
        "    for (var i = 5; i < 29; i++) {\n"
        "        var hour = i % 24;\n"
        "        labels.push(hour.toString().padStart(2, '0'));\n"
        '        var key = hour + ":" + currentDay + ":" + currentWeather;\n'
        "        var entry = busynessData.data[key];\n"
        "        values.push(entry ? entry.avg : 0);\n"
        "    }\n"
        "    chart.data.labels = labels;\n"
        "    chart.data.datasets[0].data = values;\n"
        "    chart.update();\n"
        "    var dayLabel = currentDay.charAt(0).toUpperCase() + currentDay.slice(1);\n"
        '    var wxLabel = currentWeather === "ALL" ? "All weather" : currentWeather;\n'
        "    var sub = dayLabel;\n"
        "    if (hasWeather) sub += ', ' + wxLabel;\n"
        "    sub += ' \\u2014 ' + busynessData.numDates + ' days of data';\n"
        "    document.getElementById('busy-subtitle').textContent = sub;\n"
        "}\n"
        "\n"
        "document.querySelectorAll('.day-btn').forEach(function(btn) {\n"
        "    btn.addEventListener('click', function() {\n"
        "        document.querySelectorAll('.day-btn').forEach(function(b) { b.classList.remove('active'); });\n"
        "        this.classList.add('active');\n"
        "        currentDay = this.getAttribute('data-val');\n"
        "        updateChart();\n"
        "    });\n"
        "});\n"
        "\n"
        "document.querySelectorAll('.weather-btn').forEach(function(btn) {\n"
        "    btn.addEventListener('click', function() {\n"
        "        document.querySelectorAll('.weather-btn').forEach(function(b) { b.classList.remove('active'); });\n"
        "        this.classList.add('active');\n"
        "        currentWeather = this.getAttribute('data-val');\n"
        "        updateChart();\n"
        "    });\n"
        "});\n"
        "\n"
        "var minimized = false;\n"
        "document.getElementById('busy-toggle').addEventListener('click', function() {\n"
        "    minimized = !minimized;\n"
        "    document.getElementById('busy-controls').style.display = minimized ? 'none' : '';\n"
        "    document.getElementById('busy-chart-wrap').style.display = minimized ? 'none' : '';\n"
        "    this.innerHTML = minimized ? '&#x25A1;' : '&#x2212;';\n"
        "    this.title = minimized ? 'Expand' : 'Minimize';\n"
        "});\n"
        "\n"
        "updateChart();\n"
        "})();\n"
        "</script>\n"
    )

    return panel_html + style_css + chart_js


def build_help_html(cmd_args):
    """Build the help window and bounds display HTML/JS.

    Args:
        cmd_args: Command-line arguments string for display.
    """
    help_panel = (
        '<div id="help-window" style="'
        "position: fixed; bottom: 20px; left: 20px; width: 300px; "
        "background-color: white; border: 2px solid #333; border-radius: 5px; "
        "padding: 10px; font-family: Arial, sans-serif; font-size: 12px; "
        'z-index: 9999; box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">\n'
        '<div style="font-weight: bold; font-size: 14px; margin-bottom: 8px;">Help</div>\n'
        '<div style="margin-bottom: 4px;">&bull; Click red dots for incident info</div>\n'
        '<div style="margin-bottom: 8px;">&bull; Press <kbd>b</kbd> to toggle viewport bounds</div>\n'
        '<div style="border-top: 1px solid #ccc; padding-top: 8px; margin-top: 8px;">'
        '<div style="font-weight: bold; font-size: 11px; margin-bottom: 4px;">Generated with:</div>'
        "<div style=\"font-family: 'Courier New', monospace; font-size: 10px; color: #555; "
        'word-wrap: break-word;">' + cmd_args + '</div>'
        '</div>\n'
        '</div>\n'
    )

    bounds_html = """
<div id="bounds-window" style="
    display: none; position: fixed; top: 80px; right: 20px;
    background-color: white; border: 2px solid #333; border-radius: 5px;
    padding: 10px; font-family: 'Courier New', monospace; font-size: 11px;
    z-index: 9999; box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
    <div style="font-weight: bold; margin-bottom: 5px;">Viewport Bounds</div>
    <div id="bounds-content">Pan/zoom to update</div>
</div>

<script>
document.addEventListener('keydown', function(e) {
    if (e.key === 'b' || e.key === 'B') {
        var boundsWindow = document.getElementById('bounds-window');
        if (boundsWindow.style.display === 'none') {
            boundsWindow.style.display = 'block';
            updateBounds();
        } else {
            boundsWindow.style.display = 'none';
        }
    }
});

function updateBounds() {
    var map = document.querySelector('.folium-map').__leaflet__;
    var bounds = map.getBounds();
    var sw = bounds.getSouthWest();
    var ne = bounds.getNorthEast();
    var content = 'SW: ' + sw.lat.toFixed(4) + ', ' + sw.lng.toFixed(4) + '<br>' +
                 'NE: ' + ne.lat.toFixed(4) + ', ' + ne.lng.toFixed(4);
    document.getElementById('bounds-content').innerHTML = content;
}

var map = document.querySelector('.folium-map').__leaflet__;
map.on('moveend', function() {
    if (document.getElementById('bounds-window').style.display !== 'none') {
        updateBounds();
    }
});
</script>
"""

    return help_panel + bounds_html

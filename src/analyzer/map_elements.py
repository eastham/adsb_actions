"""HTML and JavaScript elements for map visualization.

This module contains HTML legends, controls, and custom Folium elements
used by the visualizer.
"""

from folium.elements import MacroElement
from jinja2 import Template


LEGEND_HTML = """
    <div style="
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

    </div>
    """

STATIC_LEGEND_HTML = """
    <div style="
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

                this._div.innerHTML = '<b>Current Viewport Bounds:</b><br>' +
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

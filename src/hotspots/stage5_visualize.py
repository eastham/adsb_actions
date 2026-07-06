#!/usr/bin/env python3
"""
Stage 5: Visualization

Loads a regional Parquet event database (from Stage 4) and produces a static
HTML map using MapLibre GL JS with FAA sectional basemap overlay.

Features:
  - FAA VFR Sectional basemap (ArcGIS tiles)
  - Color-coded event dots by quality (magenta/orange/yellow/green)
  - Heatmap layer
  - Click-to-animate: click an event dot to see animated flight tracks
  - Tooltip with event details
  - Altitude band info panel
  - Escape key to dismiss animation

Output: data/v2/maps/{region}_{start}_{end}.html

Usage:
    python src/hotspots/stage5_visualize.py \
        --input data/v2/regional/CA_20260101.parquet \
        --output data/v2/maps/CA_20260101.html

    # With explicit map center override:
    python src/hotspots/stage5_visualize.py \
        --input data/v2/regional/CA_20260101.parquet \
        --center 37.5 -120.0 --zoom 7
"""

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

# Output dir — single source of truth in hotspots.config (honors
# $ADSB_V2_DATA_ROOT so a test sandbox can redirect all writes).
from hotspots.config import MAPS_DIR

# Quality -> CSS color
QUALITY_COLORS = {
    "vhigh":  "rgba(255,0,255,0.85)",     # magenta
    "high":   "rgba(255,140,0,0.85)",      # orange
    "medium": "rgba(255,255,0,0.78)",      # yellow
    "low":    "rgba(0,200,0,0.7)",         # green
}
DEFAULT_COLOR = "rgba(100,100,100,0.7)"

# Quality -> circle radius in pixels at zoom ~9
QUALITY_RADIUS = {"vhigh": 4, "high": 4, "medium": 4, "low": 4}
DEFAULT_RADIUS = 4

# FAA VFR Sectional tiles via ArcGIS
FAA_TILE_URL = "https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}"


def load_events(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    df = df.dropna(subset=["lat", "lon"])
    return df


def build_tooltip_html(row: pd.Series, event_id: int = None) -> str:
    """Build an HTML tooltip string for an event row."""
    lat_nm = row.get("lateral_nm", 0)
    alt_sep = row.get("alt_sep_ft", 0)
    try:
        lat_nm = f"{float(lat_nm):.3f}"
        alt_sep = f"{float(alt_sep):.0f}"
    except (TypeError, ValueError):
        lat_nm = alt_sep = "?"

    dt_display = str(row.get("datetime_utc", "")).replace("T", " ") + " UTC"
    id_suffix = f" <b>#{event_id}</b>" if event_id is not None else ""
    quality = row.get('quality', '?')
    dot_color = QUALITY_COLORS.get(quality, DEFAULT_COLOR)
    quality_dot = (
        f'<span style="display:inline-block;width:10px;height:10px;'
        f'border-radius:50%;background:{dot_color};'
        f'margin-right:4px;vertical-align:middle;"></span>'
    )
    return (
        f"{dt_display}{id_suffix}<br>"
        f"<b>{row.get('flight1','?')} / {row.get('flight2','?')}</b><br><br>"
        f"Quality: {quality_dot}{quality}"
        + (f" ({row.get('quality_explanation','')})" if row.get('quality_explanation') else "") + "<br><br>"
        f"Min lateral sep: {lat_nm} nm | Min alt sep: {alt_sep} ft"
    )


def _parse_date_range_from_stem(stem: str) -> tuple[str, str] | None:
    """Extract (MM/DD/YY, MM/DD/YY) date range from a stem like
    `24_50_-125_-65_20250601_20250831`. Returns None if no valid YYYYMMDD
    pair is found at the end.
    """
    parts = stem.split("_")
    if len(parts) < 2:
        return None
    a, b = parts[-2], parts[-1]
    if len(a) != 8 or len(b) != 8 or not (a.isdigit() and b.isdigit()):
        return None
    def _fmt(s):
        return f"{s[4:6]}/{s[6:8]}/{s[2:4]}"
    return _fmt(a), _fmt(b)


# US-airport lookup table built once per process. Keys are ICAO/local codes
# (uppercased); values are [lon, lat] pairs.
_us_airports_cache: dict | None = None

def build_us_airports_lookup() -> dict:
    """Return a {IDENT: [lon, lat]} dict for US public airports from the
    OurAirports CSV. Indexes by `ident` only (the canonical ICAO-ish code),
    not gps_code/local_code aliases — keeps the inlined HTML payload small.
    The jumpToAirport JS retries with a "K" prefix when a 3-char lookup misses,
    so users typing `WVI` or `C83` still resolve to KWVI/KC83 without aliases.
    Cached per-process.
    """
    global _us_airports_cache
    if _us_airports_cache is not None:
        return _us_airports_cache

    # Reuse the cached CSV downloader from generate_airport_config.
    from tools.generate_airport_config import (
        AIRPORTS_URL, download_with_cache,
    )
    import csv

    airports_path = download_with_cache(AIRPORTS_URL, "airports.csv")
    keep_types = {"large_airport", "medium_airport", "small_airport"}
    table: dict = {}
    with open(airports_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("iso_country") != "US":
                continue
            if row.get("type") not in keep_types:
                continue
            try:
                lat = float(row["latitude_deg"])
                lon = float(row["longitude_deg"])
            except (KeyError, ValueError):
                continue
            ident = (row.get("ident") or "").upper().strip()
            if ident:
                table[ident] = [lon, lat]

    _us_airports_cache = table
    return table


def _date_range_header_html(date_range: tuple[str, str] | None) -> str:
    """Render the 'Data: MM/DD/YY - MM/DD/YY' header line for the upper-right panel.
    Returns empty string if no date range is supplied.
    """
    if not date_range:
        return ""
    start, end = date_range
    return (
        '<div style="margin-bottom:8px; padding-bottom:6px;'
        ' border-bottom:1px solid #555;">\n'
        f'<b>LOS data: {start} - {end}</b>\n'
        '</div>\n'
    )


def _airport_jump_panel_html(with_search: bool = False) -> str:
    """Upper-left panel: airport-code jump input + small acknowledgement footer.
    The 0.8rem font matches v1's `.stat-label` (smallest acknowledgement-style
    font in v1, see html/index_template.html).

    When `with_search` is True (PMTiles mode only, where the search JS exists),
    a tail-number search field + results list is added below the airport-jump
    field.
    """
    # Tail-number search field + (initially hidden) results list. Only emitted
    # when the page has the search JS wired up (PMTiles mode).
    search_html = (
        (
            '<label style="display:flex; align-items:center; justify-content:center; gap:6px; margin-top:6px;">\n'
            '  Aircraft ID / Tail&nbsp;#:\n'
            '  <input type="text" id="tail-search" maxlength="10" autocomplete="off"\n'
            '         placeholder="e.g. N12345"\n'
            '         style="width:80px; padding:2px 4px;">\n'
            '  <button onclick="searchTail()" style="padding:2px 8px;">Go</button>\n'
            '</label>\n'
            '<div id="tail-results" style="display:none; margin-top:6px;'
            ' max-height:40vh; overflow:auto; font-size:11px;"></div>\n'
        ) if with_search else ''
    )
    return (
        '<div id="airport-jump-box">\n'
        '<div style="color:#ff0; text-align:center;">\n'
        '<b><a href="https://airbornehotspots.org" style="color:#fff; text-decoration:none; font-size:1.5em;">airbornehotspots.org</a></b>\n'
        '<div style="height:8px;"></div>\n'
        '<span style="color:#4a90ff;">Blue</span>/<span style="color:#b060f0;">purple</span>: low-altitude traffic patterns\n'
        '<div style="height:8px;"></div>\n'
        'Colored dots'
        '<span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:#ff0; margin-left:4px; vertical-align:middle;"></span>'
        '<span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:#ffa500; margin-left:2px; vertical-align:middle;"></span>'
        '<span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:#f0f; margin-left:2px; vertical-align:middle;"></span>'
        ': Loss Of Separation events.  \n'
        'Zoom in and click on a dot to replay that event.</div>\n'
        '<hr style="border:0; border-top:1px solid #555; margin:8px 0;">\n'
        '<label style="display:flex; align-items:center; justify-content:center; gap:6px;">\n'
        '  Go To Airport Code:\n'
        '  <input type="text" id="airport-jump" maxlength="4" autocomplete="off"\n'
        '         style="width:60px; padding:2px 4px;">\n'
        '  <button onclick="jumpToAirport()" style="padding:2px 8px;">Go</button>\n'
        '</label>\n'
        + search_html +
        '<hr style="border:0; border-top:1px solid #555; margin:8px 0;">\n'
        '<div style="font-size:10px; line-height:1.3;">\n'
        'Data courtesy <a href="https://adsb.lol" target="_blank"'
        ' style="color:#9cf;text-decoration:underline">adsb.lol</a>,'
        ' via the Open Database License.<br>\n'
        'Data is crowdsourced and incomplete. For informational purposes only.\n'
        '</div>\n'
        '</div>\n'
    )


def _bounds_js(bounds: tuple[float, float, float, float] | None, auto_fit: bool) -> str:
    """Emit JS globals DATA_BOUNDS and AUTO_FIT used by the load-time viewport
    handler. DATA_BOUNDS is in MapLibre's [[lon_min, lat_min], [lon_max, lat_max]]
    form. AUTO_FIT controls whether the page fits to those bounds when no
    ?airport= URL param is set.
    """
    if bounds is None:
        return 'var DATA_BOUNDS = null;\nvar AUTO_FIT = false;\n'
    lon_min, lat_min, lon_max, lat_max = bounds
    bb = json.dumps([[lon_min, lat_min], [lon_max, lat_max]])
    return f'var DATA_BOUNDS = {bb};\nvar AUTO_FIT = {"true" if auto_fit else "false"};\n'


def _airport_quality_js(airport_quality: dict | None,
                        sidecar_url: str | None = None) -> str:
    """JS that injects airport quality icons + labels + hover popups onto the
    map. The map must already exist as a global `map` (MapLibre instance) and
    `AIRPORTS` must be populated.

    If `sidecar_url` is given, AIRPORT_QUALITY is fetched at runtime (used
    in PMTiles mode where the shell HTML stays small). Otherwise the dict is
    inlined verbatim (used in self-contained mode).
    """
    if sidecar_url is not None:
        loader = (
            'fetch(' + json.dumps(sidecar_url) + ')\n'
            '  .then(function(r) { return r.ok ? r.json() : {}; })\n'
            '  .catch(function() { return {}; })\n'
            '  .then(function(AIRPORT_QUALITY) {\n'
            '    addAirportQuality(AIRPORT_QUALITY);\n'
            '  });\n'
        )
    else:
        inlined = json.dumps(airport_quality or {})
        loader = (
            'addAirportQuality(' + inlined + ');\n'
        )

    # Per-color airplane-icon images (registered with map.addImage at load) +
    # text labels + invisible hit-target circles. We use raster icons (canvas-
    # rendered SVGs) rather than MapLibre's text-symbol path because the demo
    # font doesn't contain airplane glyphs, and icons are also more legible
    # at small sizes. Hit-target circle on top makes hovering forgiving.
    return (
        'function addAirportQuality(AIRPORT_QUALITY) {\n'
        '  // Airplane SVG — a simple "up-and-to-the-right" silhouette.\n'
        '  // Drawn with `currentColor` so we can recolor per score by setting\n'
        '  // `color:` in the wrapper. 24x24 viewBox.\n'
        '  function airplaneSVG(stroke) {\n'
        '    return \'<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"\'\n'
        '         + \' viewBox="0 0 24 24" fill="currentColor" stroke="\' + stroke + \'"\'\n'
        '         + \' stroke-width="1" stroke-linejoin="round" stroke-linecap="round">\'\n'
        '         + \'<path d="M21 16v-2l-8-5V3.5a1.5 1.5 0 0 0-3 0V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5z"/>\'\n'
        '         + \'</svg>\';\n'
        '  }\n'
        '  // Render an SVG to a 32x32 ImageBitmap-compatible image we can\n'
        '  // hand to map.addImage. Returns a Promise.\n'
        '  function svgToImage(svgStr, fillColor) {\n'
        '    return new Promise(function(resolve, reject) {\n'
        '      var wrapped = \'<div xmlns="http://www.w3.org/1999/xhtml" style="color:\' + fillColor + \'">\' + svgStr + \'</div>\';\n'
        '      var blob = new Blob([svgStr.replace("currentColor", fillColor)], {type:"image/svg+xml"});\n'
        '      var url = URL.createObjectURL(blob);\n'
        '      var img = new Image();\n'
        '      img.onload = function() {\n'
        '        var c = document.createElement("canvas");\n'
        '        c.width = 32; c.height = 32;\n'
        '        var ctx = c.getContext("2d");\n'
        '        ctx.drawImage(img, 0, 0, 32, 32);\n'
        '        URL.revokeObjectURL(url);\n'
        '        var data = ctx.getImageData(0, 0, 32, 32);\n'
        '        resolve({width: 32, height: 32, data: new Uint8Array(data.data)});\n'
        '      };\n'
        '      img.onerror = reject;\n'
        '      img.src = url;\n'
        '    });\n'
        '  }\n'
        '\n'
        '  var labels = {green:"Good", yellow:"Fair", red:"Poor", none:"No data"};\n'
        '  var colors = {green:"#2ecc71", yellow:"#e0c020",\n'
        '                red:"#e74c3c", none:"#888"};\n'
        '\n'
        '  // Register one icon image per score color.\n'
        '  var iconNames = {};\n'
        '  var registerPromises = [];\n'
        '  Object.keys(colors).forEach(function(s) {\n'
        '    var name = "aq-plane-" + s;\n'
        '    iconNames[s] = name;\n'
        '    registerPromises.push(\n'
        '      svgToImage(airplaneSVG("#000"), colors[s]).then(function(img) {\n'
        '        if (!map.hasImage(name)) map.addImage(name, img);\n'
        '      })\n'
        '    );\n'
        '  });\n'
        '\n'
        '  Promise.all(registerPromises).then(function() {\n'
        '    var qFeatures = [];\n'
        '    for (var icao in AIRPORT_QUALITY) {\n'
        '      var loc = AIRPORTS[icao]; if (!loc) continue;\n'
        '      var q = AIRPORT_QUALITY[icao];\n'
        '      var s = q.score || "none";\n'
        '      qFeatures.push({type:"Feature",\n'
        '        geometry:{type:"Point", coordinates:loc},\n'
        '        properties:{icao:icao, score:s,\n'
        '          icon: iconNames[s] || iconNames["none"],\n'
        '          label:icao + " — " + (labels[s] || "?"),\n'
        '          color:colors[s] || "#888",\n'
        '          lostRate:q.lostRate, medianGapS:q.medianGapS,\n'
        '          completionRate:q.completionRate, numDates:q.numDates,\n'
        '          totalLowAltTracks:q.totalLowAltTracks,\n'
        '          // MapLibre flattens feature properties, so nested arrays do\n'
        '          // not survive as JS arrays. Stringify runwayUsage here and\n'
        '          // JSON.parse it back in the hover handler.\n'
        '          runwayUsage: JSON.stringify(q.runwayUsage || [])}});\n'
        '    }\n'
        '    if (!qFeatures.length) return;\n'
        '    map.addSource("airport-quality", {type:"geojson",\n'
        '      data:{type:"FeatureCollection", features:qFeatures}});\n'
        '\n'
        '    // Airplane icon, colored per score.\n'
        '    map.addLayer({id:"aq-dot", type:"symbol", source:"airport-quality",\n'
        '      minzoom: 8,\n'
        '      layout:{"icon-image":["get","icon"],\n'
        '              "icon-size":0.7,\n'
        '              "icon-allow-overlap":true,\n'
        '              "icon-ignore-placement":true}});\n'
        '\n'
        '    /* Label rendered below the icon — hidden for now.\n'
        '    map.addLayer({id:"aq-label", type:"symbol", source:"airport-quality",\n'
        '      minzoom: 8,\n'
        '      layout:{"text-field":["get","label"], "text-size":11,\n'
        '              "text-offset":[0, 1.2], "text-anchor":"top",\n'
        '              "text-allow-overlap":false,\n'
        '              "text-ignore-placement":false},\n'
        '      paint:{"text-color":["case",\n'
        '                            ["==", ["get","score"], "none"], "#fff",\n'
        '                            ["get","color"]],\n'
        '             "text-halo-color":"#000","text-halo-width":1.6}});\n'
        '    */\n'
        '\n'
        '    // Invisible hit-target circle on top for forgiving hover.\n'
        '    map.addLayer({id:"aq-hit", type:"circle", source:"airport-quality",\n'
        '      minzoom: 8,\n'
        '      paint:{"circle-color":"rgba(0,0,0,0)",\n'
        '             "circle-radius":14,\n'
        '             "circle-stroke-width":0}});\n'
        '  });\n'
        '\n'
        '  // Hover popup with explainer + per-airport metrics. Black panel\n'
        '  // styling matches the other UI panels.\n'
        '  var aqPopup = new maplibregl.Popup({closeButton:false, closeOnClick:false,\n'
        '                                      maxWidth:"320px",\n'
        '                                      className:"aq-popup"});\n'
        '  function pct(x) {\n'
        '    return (x === null || x === undefined) ? "n/a" : (Math.round(x*100) + "%");\n'
        '  }\n'
        '  function num(x, suffix) {\n'
        '    if (x === null || x === undefined) return "n/a";\n'
        '    return x + (suffix || "");\n'
        '  }\n'
        '  function aqHtml(p) {\n'
        '    var headers = {green:"Good ADS-B coverage",\n'
        '                   yellow:"Fair ADS-B coverage",\n'
        '                   red:"Poor ADS-B coverage",\n'
        '                   none:"No approach data"};\n'
        '    var explain = {\n'
        '      green:"Most arrivals/departures are seen past <500 feet AGL.",\n'
        '      yellow:"Some arrivals/departures are not seen below 500 feet AGL - events may be incomplete.",\n'
        '      red:"Many arrivals/departures are not seen below 500 feet AGL — events may be incomplete.",\n'
        '      none:"No aircraft were observed approaching/departing this airport in the date range, so coverage cannot be scored."};\n'
        '    var html = "<div style=\\"font:12px sans-serif;line-height:1.4;color:#fff\\">"\n'
        '             + "<div style=\\"font-weight:bold;margin-bottom:2px;color:" + p.color + "\\">"\n'
        '             +   p.icao + " &mdash; " + (headers[p.score] || "Unknown")\n'
        '             + "</div>"\n'
        '             + "<div style=\\"color:#ccc;margin-bottom:6px\\">"\n'
        '             +   (explain[p.score] || "") + "</div>";\n'
        '    if (p.score !== "none") {\n'
        '      html += "<div><b>Approach/departure completeness:</b> " + pct(p.completionRate)\n'
        '            + " <span style=\\"color:#aaa\\">(of " + (p.totalLowAltTracks || 0)\n'
        '            + " low-altitude tracks)</span></div>"\n'
        '            + "<div><b>Median gap between reports:</b> " + num(p.medianGapS, " s")\n'
        '            + " <span style=\\"color:#aaa\\">(lower = better)</span></div>";\n'
        '      // Runway usage: runwayUsage arrives JSON-stringified (see above).\n'
        '      var ru = []; try { ru = JSON.parse(p.runwayUsage || "[]"); } catch(e) {}\n'
        '      if (ru.length) {\n'
        '        var parts = ru.map(function(u){ return u.runway + ": " + u.pct + "%"; });\n'
        '        html += "<div><b>Approaches by runway:</b> " + parts.join(", ") + "</div>";\n'
        '      }\n'
        '    }\n'
        '    html += "<div style=\\"color:#aaa;margin-top:4px\\">"\n'
        '          +   "Based on " + (p.numDates || 0) + " day(s) of data."\n'
        '          + "</div>"\n'
        '          + "</div>";\n'
        '    return html;\n'
        '  }\n'
        '  map.on("mouseenter", "aq-hit", function(e) {\n'
        '    map.getCanvas().style.cursor = "pointer";\n'
        '    var p = e.features[0].properties;\n'
        '    aqPopup.setLngLat(e.features[0].geometry.coordinates)\n'
        '           .setHTML(aqHtml(p)).addTo(map);\n'
        '  });\n'
        '  map.on("mouseleave", "aq-hit", function() {\n'
        '    map.getCanvas().style.cursor = "";\n'
        '    aqPopup.remove();\n'
        '  });\n'
        '}\n'
        + loader
    )


# CSS for the airport-quality popup. Black panel + white text matches the
# styling of the other UI panels (#alt-band-info, #airport-jump-box).
_AQ_POPUP_CSS = (
    '.aq-popup .maplibregl-popup-content {\n'
    '  background: rgba(0,0,0,0.85); color: #fff;\n'
    '  padding: 10px 12px; border-radius: 6px;\n'
    '  font-family: sans-serif;\n'
    '}\n'
    '.aq-popup .maplibregl-popup-tip {\n'
    '  border-top-color: rgba(0,0,0,0.85) !important;\n'
    '  border-bottom-color: rgba(0,0,0,0.85) !important;\n'
    '  border-left-color: rgba(0,0,0,0.85) !important;\n'
    '  border-right-color: rgba(0,0,0,0.85) !important;\n'
    '}\n'
)


def _airport_jump_js() -> str:
    """JS for the airport-code jump box: normalizes 3-letter US codes to ICAO
    (mirroring v1's searchAirport() in html/index_template.html), looks up
    [lon, lat] in AIRPORTS, and flyTo's the map. Caller must define a global
    `AIRPORTS` object and a global `map` (MapLibre instance).
    The ?airport= URL param is consumed inside the on-load callback so flyTo
    runs after the map is ready.
    """
    return (
        'function jumpToAirport(code) {\n'
        '  var input = document.getElementById("airport-jump");\n'
        '  code = (code || (input ? input.value : "")).trim().toUpperCase();\n'
        '  if (!code) return;\n'
        '  // AIRPORTS is keyed by OurAirports `ident` (e.g. KWVI, KC83). Users\n'
        '  // commonly type the 3-char local/IATA code (WVI, C83); try the\n'
        '  // K-prefixed form too if the raw code misses.\n'
        '  var loc = AIRPORTS[code];\n'
        '  if (!loc && code.length === 3 && code[0] !== "K") {\n'
        '    loc = AIRPORTS["K" + code];\n'
        '  }\n'
        '  if (loc) {\n'
        '    map.flyTo({center: loc, zoom: 11});\n'
        '  } else {\n'
        '    alert("Airport " + code + " not in this map.");\n'
        '  }\n'
        '}\n'
        'document.addEventListener("DOMContentLoaded", function() {\n'
        '  var input = document.getElementById("airport-jump");\n'
        '  if (input) {\n'
        '    input.addEventListener("keydown", function(e) {\n'
        '      if (e.key === "Enter") jumpToAirport();\n'
        '    });\n'
        '  }\n'
        '});\n'
    )


def _build_geojson(df: pd.DataFrame) -> dict:
    """Build GeoJSON FeatureCollection from event DataFrame."""
    features = []
    for i, (_, row) in enumerate(df.iterrows()):
        props = {
            "flight1": str(row.get("flight1", "")),
            "flight2": str(row.get("flight2", "")),
            "quality": str(row.get("quality", "")).lower(),
            "lateral_nm": float(row.get("lateral_nm", 0)),
            "alt_sep_ft": float(row.get("alt_sep_ft", 0)),
            "alt_ft": float(row.get("alt_ft", 0)),
            "alt_band": str(row.get("alt_band", "")),
            "datetime_utc": str(row.get("datetime_utc", "")),
            "html": build_tooltip_html(row, event_id=i + 1),
            "track1": row.get("track1", "") if isinstance(row.get("track1"), str) else "",
            "track2": row.get("track2", "") if isinstance(row.get("track2"), str) else "",
            "color": QUALITY_COLORS.get(
                str(row.get("quality", "")).lower(), DEFAULT_COLOR),
            "radius": QUALITY_RADIUS.get(
                str(row.get("quality", "")).lower(), DEFAULT_RADIUS),
        }
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["lon"]), float(row["lat"])],
            },
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


def generate_html(df: pd.DataFrame, center_lat: float, center_lon: float,
                  zoom: float, faa_basemap: bool = True,
                  traffic_tile_dir: str = None,
                  date_range: tuple[str, str] | None = None,
                  airports_lookup: dict | None = None,
                  airport_quality: dict | None = None,
                  bounds: tuple[float, float, float, float] | None = None,
                  auto_fit: bool = True) -> str:
    """Generate a standalone MapLibre GL HTML page with event data.

    `bounds` is (lon_min, lat_min, lon_max, lat_max) of the event data; when
    `auto_fit` is True and no ?airport= URL param is set, the map fits these
    bounds on load. Otherwise the static center/zoom are used.
    """
    geojson = _build_geojson(df)
    geojson_json = json.dumps(geojson)

    alt_bands = set(df["alt_band"].dropna().unique().tolist()) if "alt_band" in df.columns else set()
    all_bands_ordered = ["0k-3k", "3k-6k", "6k-10k", "10k-18k"]
    # Extra bands not in canonical list (shouldn't normally occur)
    extra_bands = sorted(b for b in alt_bands if b not in all_bands_ordered)

    def _band_checkbox(b):
        has_data = b in alt_bands
        disabled = "" if has_data else ' disabled'
        style = "display:block;margin:3px 0;cursor:pointer" if has_data else "display:block;margin:3px 0;color:#aaa"
        checked = " checked" if has_data else ""
        return (f'<label style="{style}">'
                f'<input type="checkbox" class="band-cb" value="{b}"{checked}{disabled}> {b}</label>')

    alt_band_checkboxes = "".join(_band_checkbox(b) for b in all_bands_ordered + extra_bands)

    # MapLibre style with OSM base + FAA sectional overlay
    # MapLibre needs a glyphs URL to render symbol layers (text labels).
    # MapTiler provides free glyph PBFs compatible with MapLibre.
    glyphs_url = "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf"

    sources = {
        "osm": {
            "type": "raster",
            "tiles": ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            "tileSize": 256,
            "attribution": "&copy; OpenStreetMap contributors",
            "maxzoom": 19,
        },
    }
    # OSM stays full opacity until z6, then fades out by z7. The long overlap with FAA
    # gives sectional tiles time to load over the network so there's no visible gap.
    layers = [{"id": "osm-layer", "type": "raster", "source": "osm",
               "paint": {"raster-opacity": ["interpolate", ["linear"], ["zoom"], 6.0, 1.0, 7.0, 0.0]}}]

    if faa_basemap:
        sources["faa-sectional"] = {
            "type": "raster",
            "tiles": [FAA_TILE_URL],
            "tileSize": 256,
            "attribution": "FAA VFR Sectional Charts via ArcGIS/Esri",
            "minzoom": 4,
            "maxzoom": 12,
        }
        layers.append({"id": "faa-layer", "type": "raster", "source": "faa-sectional",
                        "minzoom": 4,
                        "paint": {"raster-opacity": ["interpolate", ["linear"], ["zoom"], 4.0, 0.0, 5.0, 0.6, 6.0, 1.0],
                                  "raster-resampling": "linear"}})

    if traffic_tile_dir:
        # Tile URL relative to the HTML output (tiles live next to the HTML or at a known path).
        # minzoom/maxzoom match what traffic_tiles.py generates: keeps MapLibre from
        # 404-spamming outside that range, and lets it overzoom past maxzoom.
        sources["traffic"] = {
            "type": "raster",
            "tiles": [traffic_tile_dir.rstrip("/") + "/{z}/{x}/{y}.png"],
            "tileSize": 256,
            "minzoom": 5,
            "maxzoom": 11,
            "attribution": "Traffic Density",
        }
        layers.append({"id": "traffic-layer", "type": "raster", "source": "traffic",
                        "paint": {"raster-opacity": 0.7}})

    style_json = json.dumps({"version": 8, "glyphs": glyphs_url,
                              "sources": sources, "layers": layers})

    # The JS animation code as a separate string to avoid f-string brace issues
    animation_js = (
        'var _animRaf = null, _animT = 0, _tooltip = null, _animSpeed = 1, _paused = false;\n'
        'var _trackSources = [], _fetchGen = 0;\n'
        '\n'
        'function clearAnimation() {\n'
        '  _fetchGen++;\n'
        '  if (_animRaf) { cancelAnimationFrame(_animRaf); _animRaf = null; }\n'
        '  _trackSources.forEach(function(id) {\n'
        '    if (map.getLayer(id)) map.removeLayer(id);\n'
        '    if (map.getSource(id)) map.removeSource(id);\n'
        '  });\n'
        '  _trackSources = [];\n'
        '  if (_tooltip) _tooltip.style.display = "none";\n'
        '}\n'
        '\n'
        'function showTooltip(html) {\n'
        '  if (!_tooltip) {\n'
        '    _tooltip = document.createElement("div");\n'
        '    _tooltip.style.cssText = "position:fixed;top:10px;left:10px;background:rgba(0,0,0,0.85);color:#fff;padding:10px 14px;border-radius:6px;font-size:13px;font-family:sans-serif;z-index:1001;max-width:320px;pointer-events:auto";\n'
        '    var close = document.createElement("span");\n'
        '    close.textContent = " \\u2715";\n'
        '    close.style.cssText = "cursor:pointer;float:right;margin-left:8px";\n'
        '    close.onclick = clearAnimation;\n'
        '    _tooltip.appendChild(close);\n'
        '    var body = document.createElement("div");\n'
        '    _tooltip.appendChild(body);\n'
        '    var controls = document.createElement("div");\n'
        '    controls.style.cssText = "margin-top:8px;text-align:center;display:flex;justify-content:center;gap:12px";\n'
        '    var btnStyle = "cursor:pointer;font-size:18px;user-select:none;line-height:1";\n'
        '    var lblStyle = "font-size:12px;color:#ccc;user-select:none;align-self:center";\n'
        '    var slowerLbl = document.createElement("span");\n'
        '    slowerLbl.textContent = "slower";\n'
        '    slowerLbl.style.cssText = lblStyle;\n'
        '    var fasterLbl = document.createElement("span");\n'
        '    fasterLbl.textContent = "faster";\n'
        '    fasterLbl.style.cssText = lblStyle;\n'
        '    var slower = document.createElement("span");\n'
        '    slower.textContent = "\\u23EA";\n'
        '    slower.title = "Slower";\n'
        '    slower.style.cssText = btnStyle;\n'
        '    slower.onclick = function() { _animSpeed = Math.max(0.1, _animSpeed / 2); };\n'
        '    var pauseBtn = document.createElement("span");\n'
        '    pauseBtn.textContent = "\\u23F8";\n'
        '    pauseBtn.title = "Pause/Resume";\n'
        '    pauseBtn.style.cssText = btnStyle;\n'
        '    pauseBtn.onclick = function() { _paused = !_paused; };\n'
        '    var faster = document.createElement("span");\n'
        '    faster.textContent = "\\u23E9";\n'
        '    faster.title = "Faster";\n'
        '    faster.style.cssText = btnStyle;\n'
        '    faster.onclick = function() { _animSpeed = Math.min(3, (_animSpeed || 1) * 2); };\n'
        '    controls.appendChild(slowerLbl);\n'
        '    controls.appendChild(slower);\n'
        '    controls.appendChild(pauseBtn);\n'
        '    controls.appendChild(faster);\n'
        '    controls.appendChild(fasterLbl);\n'
        '    _tooltip.appendChild(controls);\n'
        '    document.body.appendChild(_tooltip);\n'
        '  }\n'
        '  _tooltip.querySelector("div").innerHTML = html;\n'
        '  _tooltip.style.display = "block";\n'
        '}\n'
        '\n'
        'function startAnimation(props) {\n'
        '  clearAnimation();\n'
        '  _animSpeed = 1;\n'
        '  _paused = false;\n'
        '  var t1 = props.track1 ? JSON.parse(props.track1) : [];\n'
        '  var t2 = props.track2 ? JSON.parse(props.track2) : [];\n'
        '  if (!t1.length && !t2.length) return;\n'
        '  if (props.html) showTooltip(props.html);\n'
        '\n'
        '  var tracks = [\n'
        '    {pts: t1, color: "rgba(30,144,255,0.6)", id: "track1", name: props.flight1 || "?"},\n'
        '    {pts: t2, color: "rgba(255,80,80,0.6)", id: "track2", name: props.flight2 || "?"}\n'
        '  ].filter(function(t) { return t.pts.length > 0; });\n'
        '\n'
        '  var allTs = [];\n'
        '  tracks.forEach(function(t) {\n'
        '    t.pts.forEach(function(p) { allTs.push(p[0]); });\n'
        '  });\n'
        '  var minT = Math.min.apply(null, allTs);\n'
        '  var maxT = Math.max.apply(null, allTs);\n'
        '  _animT = minT;\n'
        '\n'
        '  // Add line + dot + label sources/layers for each track.\n'
        '  // Label uses its own source (separate from dot) so setData drives text reliably.\n'
        '  tracks.forEach(function(track) {\n'
        '    var lineId = "anim-" + track.id;\n'
        '    var dotId = "anim-" + track.id + "-dot";\n'
        '    var labelId = "anim-" + track.id + "-label";\n'
        '    map.addSource(lineId, {type:"geojson", data:{type:"FeatureCollection",features:[]}});\n'
        '    map.addLayer({id:lineId, type:"line", source:lineId,\n'
        '      paint:{"line-color":["get","color"], "line-width":["get","width"], "line-opacity":0.9}});\n'
        '    map.addSource(dotId, {type:"geojson", data:{type:"FeatureCollection",features:[]}});\n'
        '    map.addLayer({id:dotId, type:"circle", source:dotId,\n'
        '      paint:{"circle-radius":6, "circle-color":track.color,\n'
        '             "circle-stroke-color":"#000", "circle-stroke-width":2}});\n'
        '    map.addSource(labelId, {type:"geojson", data:{type:"FeatureCollection",features:[]}});\n'
        '    map.addLayer({id:labelId, type:"symbol", source:labelId,\n'
        '      layout:{"text-field":["get","label"], "text-size":13,\n'
        '              "text-anchor":"bottom-left", "text-offset":[0.8,-0.5],\n'
        '              "text-allow-overlap":true, "text-ignore-placement":true},\n'
        '      paint:{"text-color":"#fff", "text-halo-color":"#000", "text-halo-width":2}});\n'
        '    _trackSources.push(lineId);\n'
        '    _trackSources.push(dotId);\n'
        '    _trackSources.push(labelId);\n'
        '    track._lineId = lineId;\n'
        '    track._dotId = dotId;\n'
        '    track._labelId = labelId;\n'
        '  });\n'
        '\n'
        '  var GAP_THRESHOLD_S = 5;\n'
        '  function buildSegments(track, upToT) {\n'
        '    // p = [timestamp, lat, lon, alt, resampled(0/1)]\n'
        '    // Pre-pass: mark indices that are part of a significant data gap.\n'
        '    // Case 1: run of resampled points spanning > GAP_THRESHOLD_S seconds.\n'
        '    // Case 2: raw time jump > GAP_THRESHOLD_S (gap too large to interpolate).\n'
        '    var pts = track.pts, n = pts.length;\n'
        '    var inGap = new Array(n).fill(false);\n'
        '    var i = 0;\n'
        '    while (i < n) {\n'
        '      if (pts[i][4] === 1) {\n'
        '        var runStart = i;\n'
        '        while (i < n && pts[i][4] === 1) i++;\n'
        '        if (pts[i-1][0] - pts[runStart][0] > GAP_THRESHOLD_S)\n'
        '          for (var j = runStart; j < i; j++) inGap[j] = true;\n'
        '      } else { i++; }\n'
        '    }\n'
        '    for (var i = 1; i < n; i++)\n'
        '      if (pts[i][0] - pts[i-1][0] > GAP_THRESHOLD_S) inGap[i] = true;\n'
        '    // Segment-building pass: emit polylines, splitting on gap/non-gap transitions.\n'
        '    var features = [], seg = [];\n'
        '    var prevIsGap = inGap[0];\n'
        '    function flushSeg(isGapSeg) {\n'
        '      if (seg.length >= 2) features.push({type:"Feature",\n'
        '        properties:{color: isGapSeg ? "rgba(160,160,160,0.8)" : track.color,\n'
        '                    width: isGapSeg ? 2 : 3},\n'
        '        geometry:{type:"LineString",coordinates:seg}});\n'
        '    }\n'
        '    for (var i = 0; i < n; i++) {\n'
        '      var p = pts[i];\n'
        '      if (p[0] > upToT) break;\n'
        '      var g = inGap[i];\n'
        '      if (g !== prevIsGap) {\n'
        '        flushSeg(prevIsGap);\n'
        '        seg = seg.length ? [seg[seg.length-1]] : [];\n'
        '        prevIsGap = g;\n'
        '      }\n'
        '      seg.push([p[2], p[1]]);\n'
        '    }\n'
        '    flushSeg(prevIsGap);\n'
        '    return {type:"FeatureCollection",features:features};\n'
        '  }\n'
        '\n'
        '  function resetTracks() {\n'
        '    // Clear all tracks back to empty so both restart together on loop\n'
        '    tracks.forEach(function(track) {\n'
        '      map.getSource(track._lineId).setData({type:"FeatureCollection",features:[]});\n'
        '      map.getSource(track._dotId).setData({type:"FeatureCollection",features:[]});\n'
        '      map.getSource(track._labelId).setData({type:"FeatureCollection",features:[]});\n'
        '    });\n'
        '  }\n'
        '  function frame() {\n'
        '    if (!_paused) _animT += 0.5 * _animSpeed;\n'
        '    if (_animT > maxT + 5) {\n'
        '      // Both tracks finished — reset everything before restarting\n'
        '      _animT = minT;\n'
        '      resetTracks();\n'
        '    }\n'
        '    tracks.forEach(function(track) {\n'
        '      var lastCoord = null, lastAlt = 0;\n'
        '      for (var i = 0; i < track.pts.length; i++) {\n'
        '        var p = track.pts[i];\n'
        '        if (p[0] > _animT) break;\n'
        '        lastCoord = [p[2], p[1]];\n'
        '        lastAlt = p[3] || 0;\n'
        '      }\n'
        '      map.getSource(track._lineId).setData(buildSegments(track, _animT));\n'
        '      if (lastCoord) {\n'
        '        map.getSource(track._dotId).setData({type:"Feature",properties:{},geometry:{type:"Point",coordinates:lastCoord}});\n'
        '        var label = track.name + "\\n" + Math.round(lastAlt) + "ft";\n'
        '        map.getSource(track._labelId).setData({type:"Feature",properties:{label:label},geometry:{type:"Point",coordinates:lastCoord}});\n'
        '      }\n'
        '    });\n'
        '    _animRaf = requestAnimationFrame(frame);\n'
        '  }\n'
        '  _animRaf = requestAnimationFrame(frame);\n'
        '}\n'
        '\n'
        'document.addEventListener("keydown", function(e) {\n'
        '  if (e.key === "Escape") clearAnimation();\n'
        '});\n'
    )

    html = (
        '<!DOCTYPE html>\n'
        '<html>\n'
        '<head>\n'
        '<meta charset="utf-8">\n'
        '<title>LOS Events Map</title>\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        '<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>\n'
        '<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">\n'
        '<style>\n'
        'body { margin: 0; padding: 0; }\n'
        '#map { position: absolute; top: 0; bottom: 0; width: 100%; }\n'
        '#alt-band-info { position: fixed; top: 10px; right: 10px;\n'
        '  background: rgba(0,0,0,0.75); color: #fff; padding: 8px 12px;\n'
        '  border-radius: 6px; font-size: 12px; font-family: sans-serif; z-index: 1000; }\n'
        '#airport-jump-box { position: fixed; top: 10px; left: 10px;\n'
        '  background: rgba(0,0,0,0.75); color: #fff; padding: 8px 12px;\n'
        '  border-radius: 6px; font-size: 12px; font-family: sans-serif;\n'
        '  z-index: 1000; max-width: 260px; }\n'
        '@media (max-width: 480px) {\n'
        '  #alt-band-info { top: auto; bottom: 10px; }\n'
        '}\n'
        + _AQ_POPUP_CSS +
        '</style>\n'
        '</head>\n'
        '<body>\n'
        '<div id="map"></div>\n'
        + _airport_jump_panel_html()
    )

    date_header_html = _date_range_header_html(date_range)

    if alt_band_checkboxes:
        html += (
            '<div id="alt-band-info">\n'
            + date_header_html +
            '<b>LOS Altitude Filter (MSL)</b><br>\n'
            + alt_band_checkboxes +
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;cursor:pointer">'
            '<input type="checkbox" id="show-low-cb"> Show low-quality LOS events</label>\n'
            '</div>\n'
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;font-size:11px;margin-bottom:2px">LOS Heatmap Opacity: <span id="heatmap-opacity-val">0</span>%</label>\n'
            '<input type="range" id="heatmap-opacity-slider" min="0" max="75" value="0" style="width:100%">\n'
            '</div>\n'
            '</div>\n'
        )
    elif date_header_html:
        html += (
            '<div id="alt-band-info">\n'
            + date_header_html +
            '</div>\n'
        )

    airports_json = json.dumps(airports_lookup or {})
    bounds_js = _bounds_js(bounds, auto_fit)

    html += (
        '<script>\n'
        'var AIRPORTS = ' + airports_json + ';\n'
        + bounds_js
        + _airport_jump_js() +
        'var EVENTS_GEOJSON = ' + geojson_json + ';\n'
        '\n'
        'var map = new maplibregl.Map({\n'
        '  container: "map",\n'
        '  style: ' + style_json + ',\n'
        '  center: [' + str(center_lon) + ', ' + str(center_lat) + '],\n'
        '  zoom: ' + str(zoom) + ',\n'
        '  dragRotate: false,\n'
        '  pitchWithRotate: false,\n'
        '  touchPitch: false,\n'
        '  attributionControl: false\n'
        '});\n'
        'map.touchZoomRotate.disableRotation();\n'
        'map.addControl(new maplibregl.NavigationControl({visualizePitch: false}), "bottom-left");\n'
        'map.addControl(new maplibregl.AttributionControl({compact: true}));\n'
        '\n'
        '// Zoom-level readout (debug) — uncomment to enable\n'
        '// var zoomBox = document.createElement("div");\n'
        '// zoomBox.style.cssText = "position:absolute;bottom:8px;left:8px;z-index:1000;'
        'background:rgba(0,0,0,0.7);color:#fff;padding:4px 8px;font:12px monospace;border-radius:3px;";\n'
        '// document.body.appendChild(zoomBox);\n'
        '// function updateZoom() { zoomBox.textContent = "zoom: " + map.getZoom().toFixed(2); }\n'
        '// map.on("zoom", updateZoom); map.on("load", updateZoom);\n'
        '\n'
        + animation_js +
        '\n'
        'map.on("load", function() {\n'
        '  // Add events source\n'
        '  map.addSource("events", {type: "geojson", data: EVENTS_GEOJSON});\n'
        '\n'
        '  // Heatmap layer (visible at lower zooms); excludes low-quality (green) events\n'
        '  map.addLayer({\n'
        '    id: "events-heat",\n'
        '    type: "heatmap",\n'
        '    source: "events",\n'
        '    filter: ["!=", ["get", "quality"], "low"],\n'
        '    paint: {\n'
        '      "heatmap-weight": 1,\n'
        '      "heatmap-intensity": 1,\n'
        '      "heatmap-radius": ["interpolate", ["exponential", 2], ["zoom"], 8, 20, 9, 40, 10, 80, 11, 160, 12, 320],\n'
        '      "heatmap-opacity": 0\n'
        '    }\n'
        '  });\n'
        '\n'
        '  // Circle layer for individual events, colored by quality.\n'
        '  // Uses data-driven styling via feature properties.\n'
        '  map.addLayer({\n'
        '    id: "events-circles",\n'
        '    type: "circle",\n'
        '    source: "events",\n'
        '    paint: {\n'
        '      "circle-color": ["get", "color"],\n'
        '      "circle-radius": ["get", "radius"],\n'
        '      "circle-stroke-color": "#000",\n'
        '      "circle-stroke-width": 1\n'
        '    }\n'
        '  });\n'
        '\n'
        '  // Invisible larger hit-target layer on top for easier mobile tapping\n'
        '  map.addLayer({\n'
        '    id: "events-hit",\n'
        '    type: "circle",\n'
        '    source: "events",\n'
        '    paint: {\n'
        '      "circle-color": "rgba(0,0,0,0)",\n'
        '      "circle-radius": 16,\n'
        '      "circle-stroke-width": 0\n'
        '    }\n'
        '  });\n'
        '\n'
        '  // Hover: change cursor\n'
        '  map.on("mouseenter", "events-hit", function() {\n'
        '    map.getCanvas().style.cursor = "pointer";\n'
        '  });\n'
        '  map.on("mouseleave", "events-hit", function() {\n'
        '    map.getCanvas().style.cursor = "";\n'
        '  });\n'
        '\n'
        '  // Click: show tooltip + animate tracks\n'
        '  map.on("click", "events-hit", function(e) {\n'
        '    if (!e.features || !e.features.length) return;\n'
        '    var props = e.features[0].properties;\n'
        '    startAnimation(props);\n'
        '  });\n'
        '\n'
        '  // Click on empty space: clear animation\n'
        '  map.on("click", function(e) {\n'
        '    var features = map.queryRenderedFeatures(e.point, {layers: ["events-hit"]});\n'
        '    if (!features.length) clearAnimation();\n'
        '  });\n'
        '\n'
        '  // Altitude band filter checkboxes\n'
        '  function updateBandFilter() {\n'
        '    var checked = [];\n'
        '    document.querySelectorAll(".band-cb:checked").forEach(function(cb) {\n'
        '      checked.push(cb.value);\n'
        '    });\n'
        '    // Always-false branch must use ["literal", ...] so MapLibre parses\n'
        '    // the enclosing ["all", ...] in expression mode (matching the "in"\n'
        '    // branch); a bare ["==","1","0"] triggers legacy-filter parsing,\n'
        '    // which then rejects the ["!=", ["get","quality"],...] sibling.\n'
        '    var bandFilter = checked.length\n'
        '      ? ["in", ["get", "alt_band"], ["literal", checked]]\n'
        '      : ["==", ["literal", 1], ["literal", 0]]; // nothing matches if all unchecked\n'
        '    var showLow = document.getElementById("show-low-cb").checked;\n'
        '    var dotFilter = showLow\n'
        '      ? bandFilter\n'
        '      : ["all", bandFilter, ["!=", ["get", "quality"], "low"]];\n'
        '    var heatFilter = ["all", bandFilter, ["!=", ["get", "quality"], "low"]];\n'
        '    map.setFilter("events-circles", dotFilter);\n'
        '    map.setFilter("events-hit", dotFilter);\n'
        '    map.setFilter("events-heat", heatFilter);\n'
        '  }\n'
        '  document.querySelectorAll(".band-cb").forEach(function(cb) {\n'
        '    cb.addEventListener("change", updateBandFilter);\n'
        '  });\n'
        '  var lowCb = document.getElementById("show-low-cb");\n'
        '  if (lowCb) lowCb.addEventListener("change", updateBandFilter);\n'
        '  // Apply once at load so dot/hit filters reflect the initial\n'
        '  // (unchecked) low-quality state from frame 1.\n'
        '  updateBandFilter();\n'
        '\n'
        '  // Heatmap opacity slider\n'
        '  var heatSlider = document.getElementById("heatmap-opacity-slider");\n'
        '  if (heatSlider) {\n'
        '    heatSlider.addEventListener("input", function() {\n'
        '      var v = parseInt(this.value) / 100;\n'
        '      map.setPaintProperty("events-heat", "heatmap-opacity", v);\n'
        '      document.getElementById("heatmap-opacity-val").textContent = this.value;\n'
        '    });\n'
        '  }\n'
        '\n'
        + ('  // Airport-quality icons (per-airport ADS-B coverage score)\n'
           + _airport_quality_js(airport_quality)
           if airport_quality else '') +
        '\n'
        '  // Initial viewport: ?airport=ICAO wins (zooms in tight); otherwise\n'
        '  // fit the data bounds so the whole region is in view.\n'
        '  var _params = new URLSearchParams(window.location.search);\n'
        '  var _initial = _params.get("airport");\n'
        '  if (_initial) {\n'
        '    jumpToAirport(_initial);\n'
        '  } else if (typeof DATA_BOUNDS !== "undefined" && DATA_BOUNDS && AUTO_FIT) {\n'
        '    map.fitBounds(DATA_BOUNDS, {padding: 30, animate: false, duration: 0});\n'
        '  }\n'
        '});\n'
        '</script>\n'
        '</body>\n'
        '</html>\n'
    )
    return html


def _build_geojson_for_tiles(df: pd.DataFrame) -> dict:
    """
    Build GeoJSON for tippecanoe ingestion: metadata only, no track JSON.
    Adds an `event_id` (row index as string) so the shell HTML can fetch
    the corresponding sidecar file on click.
    """
    features = []
    for i, (idx, row) in enumerate(df.iterrows()):
        props = {
            "event_id": str(idx),
            "flight1": str(row.get("flight1", "")),
            "flight2": str(row.get("flight2", "")),
            "quality": str(row.get("quality", "")).lower(),
            "lateral_nm": float(row.get("lateral_nm", 0)),
            "alt_sep_ft": float(row.get("alt_sep_ft", 0)),
            "alt_ft": float(row.get("alt_ft", 0)),
            "alt_band": str(row.get("alt_band", "")),
            "datetime_utc": str(row.get("datetime_utc", "")),
            "html": build_tooltip_html(row, event_id=i + 1),
            "color": QUALITY_COLORS.get(str(row.get("quality", "")).lower(), DEFAULT_COLOR),
            "radius": QUALITY_RADIUS.get(str(row.get("quality", "")).lower(), DEFAULT_RADIUS),
        }
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point",
                         "coordinates": [float(row["lon"]), float(row["lat"])]},
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


def write_event_sidecars(df: pd.DataFrame, sidecar_dir: str) -> None:
    """
    Write tracks.ndjson (one JSON object per line) and tracks.index.json.gz
    (event_id -> [byte_offset, length]). JS fetches the index once, then does
    a Range request for just the clicked event's line.
    """
    import gzip
    os.makedirs(sidecar_dir, exist_ok=True)
    print(f"  Writing track blob for {len(df):,} events...", flush=True)

    ndjson_path = os.path.join(sidecar_dir, "tracks.ndjson")
    index = {}
    offset = 0
    with open(ndjson_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            entry = json.dumps({
                "track1": row.get("track1", "") if isinstance(row.get("track1"), str) else "",
                "track2": row.get("track2", "") if isinstance(row.get("track2"), str) else "",
            }) + "\n"
            length = len(entry.encode("utf-8"))
            index[str(idx)] = [offset, length]
            f.write(entry)
            offset += length

    index_path = os.path.join(sidecar_dir, "tracks.index.json.gz")
    with gzip.open(index_path, "wt", encoding="utf-8") as f:
        json.dump(index, f)

    ndjson_mb = os.path.getsize(ndjson_path) / 1024 / 1024
    index_kb = os.path.getsize(index_path) / 1024
    print(f"  tracks.ndjson: {ndjson_mb:.1f} MB  index: {index_kb:.0f} KB", flush=True)


# Column order of each row in search_index.json.gz. Kept in one place so the
# writer here and the JS reader in generate_pmtiles_html() stay in sync.
SEARCH_INDEX_FIELDS = ["event_id", "flight1", "flight2", "lon", "lat",
                       "quality", "datetime_utc", "lateral_nm", "alt_sep_ft",
                       "quality_explanation"]


def write_search_index(df: pd.DataFrame, sidecar_dir: str) -> None:
    """
    Write search_index.json.gz: a compact array of [event_id, flight1, flight2,
    lon, lat, quality, datetime_utc] rows (positional, not objects, to keep the
    payload small). The page fetches this once on first search and does a
    client-side substring match on flight1/flight2 — needed because PMTiles
    events live in vector tiles and can't be enumerated client-side.

    event_id is str(df index) to match the `event_id` baked into the PMTiles
    features by _build_geojson_for_tiles(), so a clicked result can reuse the
    existing startAnimation() track-fetch path keyed on event_id.
    """
    import gzip
    os.makedirs(sidecar_dir, exist_ok=True)

    rows = []
    for idx, row in df.iterrows():
        rows.append([
            str(idx),
            str(row.get("flight1", "")).strip(),
            str(row.get("flight2", "")).strip(),
            round(float(row["lon"]), 5),
            round(float(row["lat"]), 5),
            str(row.get("quality", "")).lower(),
            str(row.get("datetime_utc", "")),
            round(float(row.get("lateral_nm", 0)), 3),
            round(float(row.get("alt_sep_ft", 0)), 0),
            str(row.get("quality_explanation", "")),
        ])

    index_path = os.path.join(sidecar_dir, "search_index.json.gz")
    with gzip.open(index_path, "wt", encoding="utf-8") as f:
        json.dump({"fields": SEARCH_INDEX_FIELDS, "rows": rows}, f)

    index_kb = os.path.getsize(index_path) / 1024
    print(f"  search_index.json.gz: {index_kb:.0f} KB ({len(rows):,} events)",
          flush=True)


def generate_pmtiles(df: pd.DataFrame, output_path: str) -> str:
    """
    Convert event DataFrame to a .pmtiles file via tippecanoe.
    Writes a temporary GeoJSON then invokes tippecanoe.
    Returns path to the generated .pmtiles file.
    """
    import subprocess
    import tempfile

    geojson = _build_geojson_for_tiles(df)
    pmtiles_path = output_path.replace(".html", ".pmtiles")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson",
                                     delete=False) as f:
        json.dump(geojson, f)
        tmp_geojson = f.name

    try:
        cmd = [
            "tippecanoe",
            "-o", pmtiles_path,
            "--force",            # overwrite if exists
            "-z14",               # max zoom (neighborhood level)
            "-Z0",                # min zoom 0 — show all events at every zoom
            "--no-tile-size-limit",
            "-r1",                # rate=1: include every feature, no dropping
            "-l", "events",       # layer name in the vector tile
            tmp_geojson,
        ]
        print(f"  Running tippecanoe ({len(df):,} events)...", flush=True)
        result = subprocess.run(cmd, capture_output=False, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"tippecanoe failed (exit {result.returncode})")
    finally:
        os.unlink(tmp_geojson)

    size_mb = os.path.getsize(pmtiles_path) / 1024 / 1024
    print(f"  PMTiles: {pmtiles_path} ({size_mb:.2f} MB)")
    return pmtiles_path


def generate_pmtiles_html(pmtiles_path: str, sidecar_dir: str,
                           center_lat: float, center_lon: float,
                           zoom: float, alt_bands: list,
                           faa_basemap: bool = True,
                           traffic_tile_dir: str = None,
                           date_range: tuple[str, str] | None = None,
                           airports_lookup: dict | None = None,
                           airport_quality_url: str | None = None,
                           asset_stem: str | None = None,
                           bounds: tuple[float, float, float, float] | None = None,
                           auto_fit: bool = True) -> str:
    """
    Generate a shell HTML page that loads events from a .pmtiles file.
    Tracks are fetched on click from per-event JSON sidecars.
    Requires HTTP serving (not file://).

    `asset_stem` overrides the auto-derived filename for the inlined
    `.pmtiles` and `_tracks` references — used when the deployer publishes
    a stable-named alias (e.g. conus.html → conus.pmtiles + conus_tracks/).
    """
    # Paths relative to the HTML file (both live in MAPS_DIR)
    if asset_stem:
        pmtiles_rel = f"{asset_stem}.pmtiles"
        sidecar_rel = f"{asset_stem}_tracks"
    else:
        pmtiles_rel = os.path.basename(pmtiles_path)
        sidecar_rel = os.path.basename(sidecar_dir)

    all_bands_ordered = ["0k-3k", "3k-6k", "6k-10k", "10k-18k"]
    alt_bands_set = set(alt_bands)
    extra_bands = sorted(b for b in alt_bands_set if b not in all_bands_ordered)

    def _band_checkbox(b):
        has_data = b in alt_bands_set
        disabled = "" if has_data else ' disabled'
        style = "display:block;margin:3px 0;cursor:pointer" if has_data else "display:block;margin:3px 0;color:#aaa"
        checked = " checked" if has_data else ""
        return (f'<label style="{style}">'
                f'<input type="checkbox" class="band-cb" value="{b}"{checked}{disabled}> {b}</label>')

    alt_band_checkboxes = "".join(_band_checkbox(b) for b in all_bands_ordered + extra_bands)

    glyphs_url = "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf"
    pm_sources = {
        "osm": {"type": "raster",
                "tiles": ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                "tileSize": 256, "maxzoom": 19,
                "attribution": "&copy; OpenStreetMap contributors"},
    }
    # OSM stays full opacity until z6, then fades out by z7. The long overlap with FAA
    # gives sectional tiles time to load over the network so there's no visible gap.
    pm_layers = [{"id": "osm-layer", "type": "raster", "source": "osm",
                  "paint": {"raster-opacity": ["interpolate", ["linear"], ["zoom"], 6.0, 1.0, 7.0, 0.0]}}]
    if faa_basemap:
        pm_sources["faa-sectional"] = {
            "type": "raster", "tiles": [FAA_TILE_URL],
            "tileSize": 256, "minzoom": 4, "maxzoom": 12,
            "attribution": "FAA VFR Sectional Charts via ArcGIS/Esri",
        }
        pm_layers.append({"id": "faa-layer", "type": "raster", "source": "faa-sectional",
                           "minzoom": 4,
                           "paint": {"raster-opacity": ["interpolate", ["linear"], ["zoom"], 4.0, 0.0, 5.0, 0.6, 6.0, 1.0],
                                     "raster-resampling": "linear"}})
    if traffic_tile_dir:
        # See generate_html(): minzoom/maxzoom match what traffic_tiles.py generates.
        pm_sources["traffic"] = {
            "type": "raster",
            "tiles": [traffic_tile_dir.rstrip("/") + "/{z}/{x}/{y}.png"],
            "tileSize": 256,
            "minzoom": 5,
            "maxzoom": 11,
            "attribution": "Traffic Density",
        }
        pm_layers.append({"id": "traffic-layer", "type": "raster", "source": "traffic",
                           "paint": {"raster-opacity": 0.7}})
    style_json = json.dumps({"version": 8, "glyphs": glyphs_url,
                              "sources": pm_sources, "layers": pm_layers})

    # Animation JS: index fetched once, then per-click Range request for just that event's track line.
    animation_js = (
        'var _animRaf = null, _animT = 0, _tooltip = null, _animSpeed = 1, _paused = false;\n'
        'var _trackSources = [], _fetchGen = 0, _tracksIndex = null;\n'
        'var _focusMarker = null;\n'
        '\n'
        '// Drop a temporary marker at a focused event so it is visible even when\n'
        '// the PMTiles dot is hidden by the quality/altitude filters (e.g. a green\n'
        '// event reached via search while "show low-quality" is off). Cleared by\n'
        '// clearAnimation(), so it does not disturb the global filter state.\n'
        'function setFocusMarker(lon, lat, quality) {\n'
        '  if (_focusMarker) { _focusMarker.remove(); _focusMarker = null; }\n'
        '  if (lon == null || lat == null) return;\n'
        '  var color = QUALITY_COLORS[quality] || DEFAULT_COLOR;\n'
        '  var el = document.createElement("div");\n'
        '  el.style.cssText = "width:16px;height:16px;border-radius:50%;background:"\n'
        '    + color + ";border:2px solid #fff;box-shadow:0 0 6px rgba(0,0,0,0.6);";\n'
        '  _focusMarker = new maplibregl.Marker({element: el})\n'
        '    .setLngLat([Number(lon), Number(lat)]).addTo(map);\n'
        '}\n'
        '\n'
        'function clearAnimation() {\n'
        '  _fetchGen++;\n'
        '  if (_animRaf) { cancelAnimationFrame(_animRaf); _animRaf = null; }\n'
        '  _trackSources.forEach(function(id) {\n'
        '    if (map.getLayer(id)) map.removeLayer(id);\n'
        '    if (map.getSource(id)) map.removeSource(id);\n'
        '  });\n'
        '  _trackSources = [];\n'
        '  if (_focusMarker) { _focusMarker.remove(); _focusMarker = null; }\n'
        '  if (_tooltip) _tooltip.style.display = "none";\n'
        '}\n'
        '\n'
        'function showTooltip(html) {\n'
        '  if (!_tooltip) {\n'
        '    _tooltip = document.createElement("div");\n'
        '    _tooltip.style.cssText = "position:fixed;top:10px;left:10px;background:rgba(0,0,0,0.85);color:#fff;padding:10px 14px;border-radius:6px;font-size:13px;font-family:sans-serif;z-index:1001;max-width:320px;pointer-events:auto";\n'
        '    var close = document.createElement("span");\n'
        '    close.textContent = " \\u2715";\n'
        '    close.style.cssText = "cursor:pointer;float:right;margin-left:8px";\n'
        '    close.onclick = clearAnimation;\n'
        '    _tooltip.appendChild(close);\n'
        '    var body = document.createElement("div");\n'
        '    _tooltip.appendChild(body);\n'
        '    var controls = document.createElement("div");\n'
        '    controls.style.cssText = "margin-top:8px;text-align:center;display:flex;justify-content:center;gap:12px";\n'
        '    var btnStyle = "cursor:pointer;font-size:18px;user-select:none;line-height:1";\n'
        '    var lblStyle = "font-size:12px;color:#ccc;user-select:none;align-self:center";\n'
        '    var slowerLbl = document.createElement("span");\n'
        '    slowerLbl.textContent = "slower";\n'
        '    slowerLbl.style.cssText = lblStyle;\n'
        '    var fasterLbl = document.createElement("span");\n'
        '    fasterLbl.textContent = "faster";\n'
        '    fasterLbl.style.cssText = lblStyle;\n'
        '    var slower = document.createElement("span");\n'
        '    slower.textContent = "\\u23EA";\n'
        '    slower.title = "Slower";\n'
        '    slower.style.cssText = btnStyle;\n'
        '    slower.onclick = function() { _animSpeed = Math.max(0.1, _animSpeed / 2); };\n'
        '    var pauseBtn = document.createElement("span");\n'
        '    pauseBtn.textContent = "\\u23F8";\n'
        '    pauseBtn.title = "Pause/Resume";\n'
        '    pauseBtn.style.cssText = btnStyle;\n'
        '    pauseBtn.onclick = function() { _paused = !_paused; };\n'
        '    var faster = document.createElement("span");\n'
        '    faster.textContent = "\\u23E9";\n'
        '    faster.title = "Faster";\n'
        '    faster.style.cssText = btnStyle;\n'
        '    faster.onclick = function() { _animSpeed = Math.min(3, (_animSpeed || 1) * 2); };\n'
        '    controls.appendChild(slowerLbl);\n'
        '    controls.appendChild(slower);\n'
        '    controls.appendChild(pauseBtn);\n'
        '    controls.appendChild(faster);\n'
        '    controls.appendChild(fasterLbl);\n'
        '    _tooltip.appendChild(controls);\n'
        '    document.body.appendChild(_tooltip);\n'
        '  }\n'
        '  _tooltip.querySelector("div").innerHTML = html;\n'
        '  _tooltip.style.display = "block";\n'
        '}\n'
        '\n'
        'function runAnimation(props) {\n'
        '  try {\n'
        '  var t1 = props.track1 ? JSON.parse(props.track1) : [];\n'
        '  var t2 = props.track2 ? JSON.parse(props.track2) : [];\n'
        '  if (!t1.length && !t2.length) return;\n'
        '  if (props.html) showTooltip(props.html);\n'
        '\n'
        '  var tracks = [\n'
        '    {pts: t1, color: "rgba(30,144,255,0.6)", id: "track1", name: props.flight1 || "?"},\n'
        '    {pts: t2, color: "rgba(255,80,80,0.6)", id: "track2", name: props.flight2 || "?"}\n'
        '  ].filter(function(t) { return t.pts.length > 0; });\n'
        '\n'
        '  var allTs = [];\n'
        '  tracks.forEach(function(t) { t.pts.forEach(function(p) { allTs.push(p[0]); }); });\n'
        '  var minT = Math.min.apply(null, allTs);\n'
        '  var maxT = Math.max.apply(null, allTs);\n'
        '  _animT = minT;\n'
        '\n'
        '  tracks.forEach(function(track) {\n'
        '    var lineId = "anim-" + track.id;\n'
        '    var dotId = "anim-" + track.id + "-dot";\n'
        '    var labelId = "anim-" + track.id + "-label";\n'
        '    map.addSource(lineId, {type:"geojson", data:{type:"FeatureCollection",features:[]}});\n'
        '    map.addLayer({id:lineId, type:"line", source:lineId,\n'
        '      paint:{"line-color":["get","color"], "line-width":["get","width"], "line-opacity":0.9}});\n'
        '    map.addSource(dotId, {type:"geojson", data:{type:"FeatureCollection",features:[]}});\n'
        '    map.addLayer({id:dotId, type:"circle", source:dotId,\n'
        '      paint:{"circle-radius":6, "circle-color":track.color,\n'
        '             "circle-stroke-color":"#000", "circle-stroke-width":2}});\n'
        '    map.addSource(labelId, {type:"geojson", data:{type:"FeatureCollection",features:[]}});\n'
        '    map.addLayer({id:labelId, type:"symbol", source:labelId,\n'
        '      layout:{"text-field":["get","label"], "text-size":13,\n'
        '              "text-anchor":"bottom-left", "text-offset":[0.8,-0.5],\n'
        '              "text-allow-overlap":true, "text-ignore-placement":true},\n'
        '      paint:{"text-color":"#fff", "text-halo-color":"#000", "text-halo-width":2}});\n'
        '    _trackSources.push(lineId, dotId, labelId);\n'
        '    track._lineId = lineId; track._dotId = dotId; track._labelId = labelId;\n'
        '  });\n'
        '\n'
        '  var GAP_THRESHOLD_S = 5;\n'
        '  function buildSegments(track, upToT) {\n'
        '    // p = [timestamp, lat, lon, alt, resampled(0/1)]\n'
        '    // Pre-pass: mark indices that are part of a significant data gap.\n'
        '    // Case 1: run of resampled points spanning > GAP_THRESHOLD_S seconds.\n'
        '    // Case 2: raw time jump > GAP_THRESHOLD_S (gap too large to interpolate).\n'
        '    var pts = track.pts, n = pts.length;\n'
        '    var inGap = new Array(n).fill(false);\n'
        '    var i = 0;\n'
        '    while (i < n) {\n'
        '      if (pts[i][4] === 1) {\n'
        '        var runStart = i;\n'
        '        while (i < n && pts[i][4] === 1) i++;\n'
        '        if (pts[i-1][0] - pts[runStart][0] > GAP_THRESHOLD_S)\n'
        '          for (var j = runStart; j < i; j++) inGap[j] = true;\n'
        '      } else { i++; }\n'
        '    }\n'
        '    for (var i = 1; i < n; i++)\n'
        '      if (pts[i][0] - pts[i-1][0] > GAP_THRESHOLD_S) inGap[i] = true;\n'
        '    // Segment-building pass: emit polylines, splitting on gap/non-gap transitions.\n'
        '    var features = [], seg = [];\n'
        '    var prevIsGap = inGap[0];\n'
        '    function flushSeg(isGapSeg) {\n'
        '      if (seg.length >= 2) features.push({type:"Feature",\n'
        '        properties:{color: isGapSeg ? "rgba(160,160,160,0.8)" : track.color,\n'
        '                    width: isGapSeg ? 2 : 3},\n'
        '        geometry:{type:"LineString",coordinates:seg}});\n'
        '    }\n'
        '    for (var i = 0; i < n; i++) {\n'
        '      var p = pts[i];\n'
        '      if (p[0] > upToT) break;\n'
        '      var g = inGap[i];\n'
        '      if (g !== prevIsGap) {\n'
        '        flushSeg(prevIsGap);\n'
        '        seg = seg.length ? [seg[seg.length-1]] : [];\n'
        '        prevIsGap = g;\n'
        '      }\n'
        '      seg.push([p[2], p[1]]);\n'
        '    }\n'
        '    flushSeg(prevIsGap);\n'
        '    return {type:"FeatureCollection",features:features};\n'
        '  }\n'
        '\n'
        '  function resetTracks() {\n'
        '    tracks.forEach(function(track) {\n'
        '      map.getSource(track._lineId).setData({type:"FeatureCollection",features:[]});\n'
        '      map.getSource(track._dotId).setData({type:"FeatureCollection",features:[]});\n'
        '      map.getSource(track._labelId).setData({type:"FeatureCollection",features:[]});\n'
        '    });\n'
        '  }\n'
        '  function frame() {\n'
        '    if (!_paused) _animT += 0.5 * _animSpeed;\n'
        '    if (_animT > maxT + 5) { _animT = minT; resetTracks(); }\n'
        '    tracks.forEach(function(track) {\n'
        '      var lastCoord = null, lastAlt = 0;\n'
        '      for (var i = 0; i < track.pts.length; i++) {\n'
        '        var p = track.pts[i];\n'
        '        if (p[0] > _animT) break;\n'
        '        lastCoord = [p[2], p[1]]; lastAlt = p[3] || 0;\n'
        '      }\n'
        '      map.getSource(track._lineId).setData(buildSegments(track, _animT));\n'
        '      if (lastCoord) {\n'
        '        map.getSource(track._dotId).setData({type:"Feature",properties:{},geometry:{type:"Point",coordinates:lastCoord}});\n'
        '        map.getSource(track._labelId).setData({type:"Feature",properties:{label:track.name+"\\n"+Math.round(lastAlt)+"ft"},geometry:{type:"Point",coordinates:lastCoord}});\n'
        '      }\n'
        '    });\n'
        '    _animRaf = requestAnimationFrame(frame);\n'
        '  }\n'
        '  _animRaf = requestAnimationFrame(frame);\n'
        '  } catch(err) { console.error("runAnimation error:", err); }\n'
        '}\n'
        '\n'
        # Index is fetched once and cached. Kicked off on map load so the
        # first event click doesn't pay the cold-fetch cost.
        'var _indexPromise = null;\n'
        'function loadTracksIndex() {\n'
        '  if (_tracksIndex) return Promise.resolve(_tracksIndex);\n'
        '  if (_indexPromise) return _indexPromise;\n'
        '  // The index is gzipped on R2 but Cloudflare CDN sometimes\n'
        '  // forwards the body without Content-Encoding: gzip, so we\n'
        '  // decompress in JS using DecompressionStream (Chrome 80+,\n'
        '  // Firefox 113+, Safari 16.4+) for cross-CDN robustness.\n'
        '  _indexPromise = fetch("' + sidecar_rel + '/tracks.index.json.gz")\n'
        '    .then(function(r) {\n'
        '      // If the CDN already decompressed for us, the body is\n'
        '      // plain JSON; otherwise it\\u2019s gzip bytes we have to\n'
        '      // decompress ourselves. Detect via the gzip magic 1f 8b.\n'
        '      return r.arrayBuffer().then(function(buf) {\n'
        '        var bytes = new Uint8Array(buf);\n'
        '        if (bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b) {\n'
        '          var blob = new Blob([buf]);\n'
        '          var ds = new DecompressionStream("gzip");\n'
        '          return new Response(blob.stream().pipeThrough(ds)).json();\n'
        '        }\n'
        '        return new Response(buf).json();\n'
        '      });\n'
        '    })\n'
        '    .then(function(idx) { _tracksIndex = idx; return idx; })\n'
        '    .catch(function(e) { console.warn("index fetch failed:", e); _indexPromise = null; throw e; });\n'
        '  return _indexPromise;\n'
        '}\n'
        '\n'
        # On click: ensure index is loaded, then Range-fetch this event's track line
        'function startAnimation(props) {\n'
        '  clearAnimation();\n'
        '  // After clearAnimation() (which removes the previous marker) so the\n'
        '  // focus marker survives; shows the event even when its dot is filtered.\n'
        '  setFocusMarker(props.lon, props.lat, props.quality);\n'
        '  _animSpeed = 1;\n'
        '  _paused = false;\n'
        '  var baseHtml = props.html || "";\n'
        '  if (baseHtml) showTooltip(baseHtml);\n'
        '  var gen = _fetchGen;\n'
        '  var eid = String(props.event_id);\n'
        '  // Only show the spinner if the fetch is slow enough to notice.\n'
        '  var spinnerTimer = setTimeout(function() {\n'
        '    if (gen !== _fetchGen) return;\n'
        '    showTooltip(baseHtml + \'<div style="margin-top:6px;color:#aaa;font-style:italic">Loading track data\\u2026</div>\');\n'
        '  }, 250);\n'
        '  function clearSpinner() { clearTimeout(spinnerTimer); if (gen === _fetchGen && baseHtml) showTooltip(baseHtml); }\n'
        '  loadTracksIndex().then(function(idx) {\n'
        '    if (gen !== _fetchGen) { clearTimeout(spinnerTimer); return; }\n'
        '    var loc = idx[eid];\n'
        '    if (!loc) { clearSpinner(); console.warn("no index entry for event_id", eid); return; }\n'
        '    var blobUrl = "' + sidecar_rel + '/tracks.ndjson";\n'
        '    fetch(blobUrl, {headers: {"Range": "bytes=" + loc[0] + "-" + (loc[0]+loc[1]-1)}})\n'
        '      .then(function(r) { return r.text(); })\n'
        '      .then(function(text) {\n'
        '        if (gen !== _fetchGen) { clearTimeout(spinnerTimer); return; }\n'
        '        clearSpinner();\n'
        '        var data = JSON.parse(text);\n'
        '        data.flight1 = props.flight1;\n'
        '        data.flight2 = props.flight2;\n'
        '        runAnimation(data);\n'
        '      })\n'
        '      .catch(function(e) { clearSpinner(); console.warn("track fetch failed:", e); });\n'
        '  }).catch(function() { clearSpinner(); });\n'
        '}\n'
        '\n'
        'document.addEventListener("keydown", function(e) {\n'
        '  if (e.key === "Escape") clearAnimation();\n'
        '});\n'
    )

    # Tail-number search JS. Fetches search_index.json.gz once (same sidecar
    # dir + gzip-fallback pattern as the tracks index), does a client-side
    # substring match on flight1/flight2, and renders a results list. Clicking
    # a result — like clicking a dot — routes through focusEvent(), which flies
    # to the event, replays it, and writes a shareable ?event=<id> URL.
    quality_colors_json = json.dumps(QUALITY_COLORS)
    default_color_json = json.dumps(DEFAULT_COLOR)
    search_js = (
        'var QUALITY_COLORS = ' + quality_colors_json + ';\n'
        'var DEFAULT_COLOR = ' + default_color_json + ';\n'
        'var _searchIndex = null, _searchPromise = null;\n'
        '\n'
        '// Fetch + decode the gzipped search index once. Mirrors\n'
        '// loadTracksIndex() incl. the Cloudflare gzip-passthrough fallback.\n'
        'function loadSearchIndex() {\n'
        '  if (_searchIndex) return Promise.resolve(_searchIndex);\n'
        '  if (_searchPromise) return _searchPromise;\n'
        '  _searchPromise = fetch("' + sidecar_rel + '/search_index.json.gz")\n'
        '    .then(function(r) {\n'
        '      return r.arrayBuffer().then(function(buf) {\n'
        '        var bytes = new Uint8Array(buf);\n'
        '        if (bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b) {\n'
        '          var ds = new DecompressionStream("gzip");\n'
        '          return new Response(new Blob([buf]).stream().pipeThrough(ds)).json();\n'
        '        }\n'
        '        return new Response(buf).json();\n'
        '      });\n'
        '    })\n'
        '    .then(function(data) {\n'
        '      // Normalize positional rows into objects keyed by field name.\n'
        '      var fields = data.fields, rows = data.rows;\n'
        '      _searchIndex = rows.map(function(row) {\n'
        '        var o = {};\n'
        '        for (var i = 0; i < fields.length; i++) o[fields[i]] = row[i];\n'
        '        return o;\n'
        '      });\n'
        '      return _searchIndex;\n'
        '    })\n'
        '    .catch(function(e) { console.warn("search index fetch failed:", e); _searchPromise = null; throw e; });\n'
        '  return _searchPromise;\n'
        '}\n'
        '\n'
        '// Build the tooltip HTML for a search-index event (no track data yet).\n'
        'function _searchTooltipHtml(ev) {\n'
        '  var color = QUALITY_COLORS[ev.quality] || DEFAULT_COLOR;\n'
        '  var dot = \'<span style="display:inline-block;width:10px;height:10px;\'\n'
        '    + \'border-radius:50%;background:\' + color + \';margin-right:4px;\'\n'
        '    + \'vertical-align:middle;"></span>\';\n'
        '  // Strip any trailing " UTC" before re-appending so we never double it\n'
        '  // (stored datetime_utc may or may not already carry the suffix).\n'
        '  var dt = String(ev.datetime_utc || "").replace("T", " ")\n'
        '    .replace(/\\s*UTC\\s*$/i, "") + " UTC";\n'
        '  var lat = (ev.lateral_nm != null ? Number(ev.lateral_nm).toFixed(3) : "?");\n'
        '  var alt = (ev.alt_sep_ft != null ? Number(ev.alt_sep_ft).toFixed(0) : "?");\n'
        '  var expl = ev.quality_explanation ? " (" + ev.quality_explanation + ")" : "";\n'
        '  return dt + "<br><b>" + (ev.flight1 || "?") + " / " + (ev.flight2 || "?")\n'
        '    + "</b><br><br>Quality: " + dot + (ev.quality || "?") + expl\n'
        '    + "<br><br>Min lateral sep: " + lat + " nm | Min alt sep: " + alt + " ft";\n'
        '}\n'
        '\n'
        '// Single focus path shared by search results and dot clicks: fly to\n'
        '// the event, replay it, and write a shareable ?event=<id> URL once the\n'
        '// camera settles (no visible "share" button — the address bar is it).\n'
        'function focusEvent(props, doFly) {\n'
        '  function replay() { startAnimation(props); }\n'
        '  if (doFly && props.lon != null && props.lat != null) {\n'
        '    map.once("moveend", replay);\n'
        '    map.flyTo({center: [Number(props.lon), Number(props.lat)], zoom: 12});\n'
        '  } else {\n'
        '    replay();\n'
        '  }\n'
        '  if (props.event_id != null) {\n'
        '    try {\n'
        '      var u = new URL(window.location);\n'
        '      u.searchParams.set("event", props.event_id);\n'
        '      u.searchParams.delete("airport");\n'
        '      history.replaceState(null, "", u);\n'
        '    } catch (e) {}\n'
        '  }\n'
        '}\n'
        '\n'
        'function renderTailResults(matches, query) {\n'
        '  var box = document.getElementById("tail-results");\n'
        '  if (!box) return;\n'
        '  if (!matches.length) {\n'
        '    box.innerHTML = \'<div style="color:#aaa">No events for "\' + query + \'".</div>\';\n'
        '    box.style.display = "block";\n'
        '    return;\n'
        '  }\n'
        '  // Sort newest-first (datetime_utc is ISO 8601, so lexical == chrono),\n'
        '  // then cap the rendered rows so a broad query cannot build a huge DOM.\n'
        '  var MAX_RESULTS = 200;\n'
        '  var total = matches.length;\n'
        '  var sorted = matches.slice().sort(function(a, b) {\n'
        '    return String(b.datetime_utc || "").localeCompare(String(a.datetime_utc || ""));\n'
        '  });\n'
        '  var shown = sorted.slice(0, MAX_RESULTS);\n'
        '  var html = \'<div style="color:#aaa;margin-bottom:4px">\'\n'
        '    + (total > shown.length ? "showing " + shown.length + " of " + total : total)\n'
        '    + \' event\' + (total === 1 ? "" : "s") + \'</div>\';\n'
        '  shown.forEach(function(ev) {\n'
        '    var color = QUALITY_COLORS[ev.quality] || DEFAULT_COLOR;\n'
        '    var dot = \'<span style="display:inline-block;width:8px;height:8px;\'\n'
        '      + \'border-radius:50%;background:\' + color + \';margin-right:5px;\'\n'
        '      + \'vertical-align:middle;"></span>\';\n'
        '    var date = String(ev.datetime_utc || "").replace("T", " ")\n'
        '      .replace(/\\s*UTC\\s*$/i, "").slice(0, 16);\n'
        '    html += \'<div class="tail-result" data-eid="\' + ev.event_id + \'"\'\n'
        '      + \' style="cursor:pointer;padding:3px 2px;border-top:1px solid #444">\'\n'
        '      + dot + "<b>" + (ev.flight1 || "?") + " / " + (ev.flight2 || "?") + "</b>"\n'
        '      + \'<br><span style="color:#aaa">\' + date + " UTC</span></div>";\n'
        '  });\n'
        '  box.innerHTML = html;\n'
        '  box.style.display = "block";\n'
        '  // event_id -> event object, so a click can focus without re-searching.\n'
        '  var byId = {};\n'
        '  shown.forEach(function(ev) { byId[String(ev.event_id)] = ev; });\n'
        '  box.querySelectorAll(".tail-result").forEach(function(el) {\n'
        '    el.addEventListener("click", function() {\n'
        '      var ev = byId[el.getAttribute("data-eid")];\n'
        '      if (!ev) return;\n'
        '      focusEvent({event_id: ev.event_id, flight1: ev.flight1,\n'
        '                  flight2: ev.flight2, lon: ev.lon, lat: ev.lat,\n'
        '                  quality: ev.quality, html: _searchTooltipHtml(ev)}, true);\n'
        '    });\n'
        '  });\n'
        '}\n'
        '\n'
        'function searchTail(query) {\n'
        '  var input = document.getElementById("tail-search");\n'
        '  query = (query != null ? query : (input ? input.value : "")).trim();\n'
        '  var box = document.getElementById("tail-results");\n'
        '  if (!query) { if (box) box.style.display = "none"; clearAnimation(); return; }\n'
        '  // A 1-char query (e.g. "N") matches nearly every tail; require >=2.\n'
        '  if (query.length < 2) {\n'
        '    if (box) { box.innerHTML = \'<div style="color:#aaa">Enter at least 2 characters.</div>\'; box.style.display = "block"; }\n'
        '    return;\n'
        '  }\n'
        '  if (box) { box.innerHTML = \'<div style="color:#aaa">Searching\\u2026</div>\'; box.style.display = "block"; }\n'
        '  var q = query.toUpperCase();\n'
        '  loadSearchIndex().then(function(idx) {\n'
        '    var matches = idx.filter(function(ev) {\n'
        '      return (ev.flight1 && ev.flight1.toUpperCase().indexOf(q) !== -1) ||\n'
        '             (ev.flight2 && ev.flight2.toUpperCase().indexOf(q) !== -1);\n'
        '    });\n'
        '    renderTailResults(matches, query);\n'
        '  }).catch(function() {\n'
        '    if (box) box.innerHTML = \'<div style="color:#e88">Search unavailable.</div>\';\n'
        '  });\n'
        '}\n'
        '\n'
        '// ?event=<id> deep-link: fly to that specific event and replay it.\n'
        'function focusEventById(eid) {\n'
        '  loadSearchIndex().then(function(idx) {\n'
        '    var ev = idx.find(function(e) { return String(e.event_id) === String(eid); });\n'
        '    if (!ev) { console.warn("no event", eid); return; }\n'
        '    focusEvent({event_id: ev.event_id, flight1: ev.flight1,\n'
        '                flight2: ev.flight2, lon: ev.lon, lat: ev.lat,\n'
        '                quality: ev.quality, html: _searchTooltipHtml(ev)}, true);\n'
        '  }).catch(function() {});\n'
        '}\n'
        '\n'
        'document.addEventListener("DOMContentLoaded", function() {\n'
        '  var input = document.getElementById("tail-search");\n'
        '  if (input) {\n'
        '    input.addEventListener("keydown", function(e) {\n'
        '      if (e.key === "Enter") searchTail();\n'
        '    });\n'
        '  }\n'
        '});\n'
    )

    html = (
        '<!DOCTYPE html>\n'
        '<html>\n'
        '<head>\n'
        '<meta charset="utf-8">\n'
        '<title>LOS Events Map</title>\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        '<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>\n'
        '<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">\n'
        '<script src="https://unpkg.com/pmtiles@3/dist/pmtiles.js"></script>\n'
        '<style>\n'
        'body { margin: 0; padding: 0; }\n'
        '#map { position: absolute; top: 0; bottom: 0; width: 100%; }\n'
        '#alt-band-info { position: fixed; top: 10px; right: 10px;\n'
        '  background: rgba(0,0,0,0.75); color: #fff; padding: 8px 12px;\n'
        '  border-radius: 6px; font-size: 12px; font-family: sans-serif; z-index: 1000; }\n'
        '#airport-jump-box { position: fixed; top: 10px; left: 10px;\n'
        '  background: rgba(0,0,0,0.75); color: #fff; padding: 8px 12px;\n'
        '  border-radius: 6px; font-size: 12px; font-family: sans-serif;\n'
        '  z-index: 1000; max-width: 260px; }\n'
        '@media (max-width: 480px) {\n'
        '  #alt-band-info { top: auto; bottom: 10px; }\n'
        '}\n'
        + _AQ_POPUP_CSS +
        '</style>\n'
        '</head>\n'
        '<body>\n'
        '<div id="map"></div>\n'
        + _airport_jump_panel_html(with_search=True)
    )

    date_header_html = _date_range_header_html(date_range)

    if alt_band_checkboxes:
        html += (
            '<div id="alt-band-info">\n'
            + date_header_html +
            '<b>LOS Altitude Filter (MSL)</b><br>\n'
            + alt_band_checkboxes +
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;cursor:pointer">'
            '<input type="checkbox" id="show-low-cb"> Show low-quality events</label>\n'
            '</div>\n'
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;font-size:11px;margin-bottom:2px">LOS Heatmap Opacity: <span id="heatmap-opacity-val">0</span>%</label>\n'
            '<input type="range" id="heatmap-opacity-slider" min="0" max="75" value="0" style="width:100%">\n'
            '</div>\n'
            '</div>\n'
        )
    elif date_header_html:
        html += (
            '<div id="alt-band-info">\n'
            + date_header_html +
            '</div>\n'
        )

    airports_json = json.dumps(airports_lookup or {})
    bounds_js = _bounds_js(bounds, auto_fit)

    html += (
        '<script>\n'
        'var AIRPORTS = ' + airports_json + ';\n'
        + bounds_js
        + _airport_jump_js() +
        '// Register PMTiles protocol so MapLibre can load .pmtiles files.\n'
        '// MapLibre 4.x uses tilev4; pmtiles wraps it as tile for v3 compat.\n'
        'var protocol = new pmtiles.Protocol();\n'
        'maplibregl.addProtocol("pmtiles", protocol.tilev4 || protocol.tile);\n'
        '\n'
        'var map = new maplibregl.Map({\n'
        '  container: "map",\n'
        '  style: ' + style_json + ',\n'
        '  center: [' + str(center_lon) + ', ' + str(center_lat) + '],\n'
        '  zoom: ' + str(zoom) + ',\n'
        '  dragRotate: false,\n'
        '  pitchWithRotate: false,\n'
        '  touchPitch: false,\n'
        '  attributionControl: false\n'
        '});\n'
        'map.touchZoomRotate.disableRotation();\n'
        'map.addControl(new maplibregl.NavigationControl({visualizePitch: false}), "bottom-left");\n'
        'map.addControl(new maplibregl.AttributionControl({compact: true}));\n'
        '\n'
        '// Zoom-level readout (debug) — uncomment to enable\n'
        '// var zoomBox = document.createElement("div");\n'
        '// zoomBox.style.cssText = "position:absolute;bottom:8px;left:8px;z-index:1000;'
        'background:rgba(0,0,0,0.7);color:#fff;padding:4px 8px;font:12px monospace;border-radius:3px;";\n'
        '// document.body.appendChild(zoomBox);\n'
        '// function updateZoom() { zoomBox.textContent = "zoom: " + map.getZoom().toFixed(2); }\n'
        '// map.on("zoom", updateZoom); map.on("load", updateZoom);\n'
        '\n'
        + animation_js +
        '\n'
        + search_js +
        '\n'
        'map.on("load", function() {\n'
        '  // Warm the tracks index in the background so the first event\n'
        '  // click doesn\'t pay the cold-fetch cost.\n'
        '  loadTracksIndex();\n'
        '\n'
        '  // Load events from PMTiles file via range requests\n'
        '  map.addSource("events", {\n'
        '    type: "vector",\n'
        '    url: "pmtiles://' + pmtiles_rel + '",\n'
        '    attribution: "LOS Events"\n'
        '  });\n'
        '\n'
        '  // Heatmap layer; excludes low-quality (green) events\n'
        '  map.addLayer({\n'
        '    id: "events-heat", type: "heatmap",\n'
        '    source: "events", "source-layer": "events",\n'
        '    filter: ["!=", ["get", "quality"], "low"],\n'
        '    paint: {"heatmap-weight": 1,\n'
        '            "heatmap-intensity": 1,\n'
        '            "heatmap-radius": ["interpolate", ["exponential", 2], ["zoom"], 8, 20, 9, 40, 10, 80, 11, 160, 12, 320],\n'
        '            "heatmap-opacity": 0}\n'
        '  });\n'
        '\n'
        '  // Circle layer colored by quality\n'
        '  map.addLayer({\n'
        '    id: "events-circles", type: "circle",\n'
        '    source: "events", "source-layer": "events",\n'
        '    paint: {\n'
        '      "circle-color": ["get", "color"],\n'
        '      "circle-radius": ["get", "radius"],\n'
        '      "circle-stroke-color": "#000", "circle-stroke-width": 1\n'
        '    }\n'
        '  });\n'
        '\n'
        '  // Invisible larger hit-target layer on top for easier mobile tapping\n'
        '  map.addLayer({\n'
        '    id: "events-hit", type: "circle",\n'
        '    source: "events", "source-layer": "events",\n'
        '    paint: {"circle-color": "rgba(0,0,0,0)", "circle-radius": 16, "circle-stroke-width": 0}\n'
        '  });\n'
        '\n'
        '  map.on("mouseenter", "events-hit", function() {\n'
        '    map.getCanvas().style.cursor = "pointer";\n'
        '  });\n'
        '  map.on("mouseleave", "events-hit", function() {\n'
        '    map.getCanvas().style.cursor = "";\n'
        '  });\n'
        '\n'
        '  var _justClickedDot = false;\n'
        '  map.on("click", "events-hit", function(e) {\n'
        '    if (!e.features || !e.features.length) return;\n'
        '    _justClickedDot = true;\n'
        '    // Route through focusEvent (no fly — the dot is already on screen)\n'
        '    // so the shareable ?event=<id> URL is written for dot clicks too.\n'
        '    focusEvent(e.features[0].properties, false);\n'
        '  });\n'
        '\n'
        '  map.on("click", function(e) {\n'
        '    if (_justClickedDot) { _justClickedDot = false; return; }\n'
        '    var f = map.queryRenderedFeatures(e.point, {layers: ["events-hit"]});\n'
        '    if (!f.length) clearAnimation();\n'
        '  });\n'
        '\n'
        '  // Altitude band filter\n'
        '  function updateBandFilter() {\n'
        '    var checked = [];\n'
        '    document.querySelectorAll(".band-cb:checked").forEach(function(cb) {\n'
        '      checked.push(cb.value);\n'
        '    });\n'
        '    // Always-false branch must use ["literal", ...] so MapLibre parses\n'
        '    // the enclosing ["all", ...] in expression mode (matching the "in"\n'
        '    // branch); a bare ["==","1","0"] triggers legacy-filter parsing,\n'
        '    // which then rejects the ["!=", ["get","quality"],...] sibling.\n'
        '    var bandFilter = checked.length\n'
        '      ? ["in", ["get", "alt_band"], ["literal", checked]]\n'
        '      : ["==", ["literal", 1], ["literal", 0]];\n'
        '    var showLow = document.getElementById("show-low-cb").checked;\n'
        '    var dotFilter = showLow\n'
        '      ? bandFilter\n'
        '      : ["all", bandFilter, ["!=", ["get", "quality"], "low"]];\n'
        '    var heatFilter = ["all", bandFilter, ["!=", ["get", "quality"], "low"]];\n'
        '    map.setFilter("events-circles", dotFilter);\n'
        '    map.setFilter("events-hit", dotFilter);\n'
        '    map.setFilter("events-heat", heatFilter);\n'
        '  }\n'
        '  document.querySelectorAll(".band-cb").forEach(function(cb) {\n'
        '    cb.addEventListener("change", updateBandFilter);\n'
        '  });\n'
        '  var lowCb = document.getElementById("show-low-cb");\n'
        '  if (lowCb) lowCb.addEventListener("change", updateBandFilter);\n'
        '  // Apply once at load so dot/hit filters reflect the initial\n'
        '  // (unchecked) low-quality state from frame 1.\n'
        '  updateBandFilter();\n'
        '\n'
        '  // Heatmap opacity slider\n'
        '  var heatSlider = document.getElementById("heatmap-opacity-slider");\n'
        '  if (heatSlider) {\n'
        '    heatSlider.addEventListener("input", function() {\n'
        '      var v = parseInt(this.value) / 100;\n'
        '      map.setPaintProperty("events-heat", "heatmap-opacity", v);\n'
        '      document.getElementById("heatmap-opacity-val").textContent = this.value;\n'
        '    });\n'
        '  }\n'
        '\n'
        + ('  // Airport-quality icons (per-airport ADS-B coverage score)\n'
           + _airport_quality_js(None, sidecar_url=airport_quality_url)
           if airport_quality_url else '') +
        '\n'
        '  // Initial viewport / deep-links. Precedence:\n'
        '  //   ?event=<id> (fly + replay a specific event) >\n'
        '  //   ?tail=<id>  (run the tail search) >\n'
        '  //   ?airport=<ICAO> (fly to airport) >\n'
        '  //   auto-fit data bounds.\n'
        '  var _params = new URLSearchParams(window.location.search);\n'
        '  var _event = _params.get("event");\n'
        '  var _tail = _params.get("tail");\n'
        '  var _airport = _params.get("airport");\n'
        '  if (_event) {\n'
        '    focusEventById(_event);\n'
        '  } else if (_tail) {\n'
        '    var _ti = document.getElementById("tail-search");\n'
        '    if (_ti) _ti.value = _tail;\n'
        '    searchTail(_tail);\n'
        '    if (typeof DATA_BOUNDS !== "undefined" && DATA_BOUNDS && AUTO_FIT) {\n'
        '      map.fitBounds(DATA_BOUNDS, {padding: 30, animate: false, duration: 0});\n'
        '    }\n'
        '  } else if (_airport) {\n'
        '    jumpToAirport(_airport);\n'
        '  } else if (typeof DATA_BOUNDS !== "undefined" && DATA_BOUNDS && AUTO_FIT) {\n'
        '    map.fitBounds(DATA_BOUNDS, {padding: 30, animate: false, duration: 0});\n'
        '  }\n'
        '});\n'
        '</script>\n'
        '</body>\n'
        '</html>\n'
    )
    return html


def main():
    parser = argparse.ArgumentParser(
        description="Stage 5: Generate map from regional Parquet event DB.")
    parser.add_argument("--input", required=True,
                        help="Input regional Parquet file")
    parser.add_argument("--output", help="Output HTML path (default: data/v2/maps/<stem>.html)")
    parser.add_argument("--center", nargs=2, type=float, metavar=("LAT", "LON"),
                        help="Override map center lat/lon")
    parser.add_argument("--zoom", type=float, default=None,
                        help="Initial zoom level. When omitted, the map fits "
                             "the data bounds on load (whole region visible). "
                             "Pass an explicit value to override.")
    parser.add_argument("--no-faa-basemap", action="store_true",
                        help="Skip FAA sectional basemap")
    parser.add_argument("--traffic-tiles", type=str, default=None,
                        help="Path (or URL prefix) to traffic tile directory "
                             "containing {z}/{x}/{y}.png tiles")
    parser.add_argument("--pmtiles", action="store_true",
                        help="Generate PMTiles + sidecar JSON instead of self-contained HTML "
                             "(required for large datasets; needs HTTP serving, not file://)")
    parser.add_argument("--asset-stem", type=str, default=None,
                        help="Override the inlined .pmtiles / _tracks filenames in the "
                             "generated HTML (e.g. --asset-stem conus). Used when the "
                             "deployer publishes a stable-named alias separate from the "
                             "dated source files. Only takes effect with --pmtiles.")
    parser.add_argument("--airport-quality", type=str, default=None,
                        help="Path to airport_quality.json from v2_airport_quality. "
                             "Self-contained mode inlines it; PMTiles mode copies it "
                             "next to the HTML and fetches it at load.")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    stem = Path(args.input).stem
    output_path = args.output or str(MAPS_DIR / f"{stem}.html")

    print(f"Loading events from {args.input}...")
    df = load_events(args.input)
    print(f"  Loaded {len(df):,} events")
    if df.empty:
        print("No events to visualize.")
        sys.exit(0)

    if "quality" in df.columns:
        print(f"  Quality distribution: {df['quality'].value_counts().to_dict()}")
    if "alt_band" in df.columns:
        print(f"  Alt bands: {sorted(df['alt_band'].dropna().unique().tolist())}")

    if args.center:
        center_lat, center_lon = args.center
    else:
        center_lat = float(df["lat"].mean())
        center_lon = float(df["lon"].mean())

    # Auto-fit on load when --zoom not given. Bounds come from the data lat/lon
    # extents (with a tiny padding so dots near the edge aren't clipped).
    auto_fit = args.zoom is None
    zoom = args.zoom if args.zoom is not None else 7.0  # static fallback if AUTO_FIT JS fails
    bounds = (
        float(df["lon"].min()), float(df["lat"].min()),
        float(df["lon"].max()), float(df["lat"].max()),
    )

    print(f"Building map (center={center_lat:.2f},{center_lon:.2f}, "
          f"{'auto-fit' if auto_fit else f'zoom={zoom}'}, "
          f"bounds=lon[{bounds[0]:.1f},{bounds[2]:.1f}] lat[{bounds[1]:.1f},{bounds[3]:.1f}])...")

    alt_bands = df["alt_band"].dropna().unique().tolist() if "alt_band" in df.columns else []

    date_range = _parse_date_range_from_stem(stem)
    airports_lookup = build_us_airports_lookup()

    # Load airport quality if provided. In self-contained mode we pass the
    # dict for inlining; in PMTiles mode we copy the JSON next to the HTML
    # and pass its relative URL so the page fetches it at load.
    airport_quality = None
    airport_quality_url = None
    if args.airport_quality:
        if not os.path.exists(args.airport_quality):
            print(f"WARN: airport-quality file not found: {args.airport_quality}",
                  file=sys.stderr)
        else:
            with open(args.airport_quality, "r", encoding="utf-8") as f:
                airport_quality = json.load(f)
            print(f"  Loaded {len(airport_quality)} airport quality entries.")

    if args.pmtiles:
        # PMTiles mode: generate .pmtiles + per-event sidecar JSONs + shell HTML
        pmtiles_path = generate_pmtiles(df, output_path)

        sidecar_dir = output_path.replace(".html", "_tracks")
        print(f"  Writing {len(df):,} event track sidecars to {sidecar_dir}/...", flush=True)
        write_event_sidecars(df, sidecar_dir)
        # Tail-number search index (same sidecar dir, keyed on the same
        # event_id as the PMTiles features).
        write_search_index(df, sidecar_dir)

        # Copy airport-quality JSON next to the HTML so it's fetchable.
        if airport_quality is not None:
            stem_for_aq = args.asset_stem or Path(output_path).stem
            aq_filename = f"{stem_for_aq}_quality.json"
            aq_dest = Path(output_path).parent / aq_filename
            with open(aq_dest, "w", encoding="utf-8") as f:
                json.dump(airport_quality, f)
            airport_quality_url = aq_filename

        html = generate_pmtiles_html(
            pmtiles_path, sidecar_dir,
            center_lat, center_lon, zoom,
            alt_bands, faa_basemap=not args.no_faa_basemap,
            traffic_tile_dir=args.traffic_tiles,
            date_range=date_range,
            airports_lookup=airports_lookup,
            airport_quality_url=airport_quality_url,
            asset_stem=args.asset_stem,
            bounds=bounds,
            auto_fit=auto_fit,
        )
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        sep = "─" * 60
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        pmtiles_path_rel = Path(output_path).with_suffix(".pmtiles")
        sidecar_dir_rel = output_path.replace(".html", "_tracks")
        print(f"\n{sep}")
        print(f"  Map (PMTiles):  {output_path}  ({size_mb:.1f} MB)")
        print(f"  PMTiles file:   {pmtiles_path_rel}")
        print(f"  Track sidecars: {sidecar_dir_rel}/")
        print(f"  Serve: python src/hotspots/serve.py . 8080")
        print(f"  Open:  http://localhost:8080/{output_path}")
        print(sep)
    else:
        # Self-contained mode: all data embedded in HTML (practical up to ~500 events)
        html = generate_html(df, center_lat, center_lon, zoom,
                             faa_basemap=not args.no_faa_basemap,
                             traffic_tile_dir=args.traffic_tiles,
                             date_range=date_range,
                             airports_lookup=airports_lookup,
                             airport_quality=airport_quality,
                             bounds=bounds,
                             auto_fit=auto_fit)

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        sep = "─" * 60
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"\n{sep}")
        print(f"  Map (self-contained): {output_path}  ({size_mb:.1f} MB)")
        print(f"  Open: file://{os.path.abspath(output_path)}")
        print(sep)


if __name__ == "__main__":
    main()

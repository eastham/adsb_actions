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

V2_DATA_ROOT = Path("data/v2")
MAPS_DIR = V2_DATA_ROOT / "maps"

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
    return (
        f"{dt_display}{id_suffix}<br>"
        f"<b>{row.get('flight1','?')} / {row.get('flight2','?')}</b><br><br>"
        f"Quality: {row.get('quality','?')}"
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
    The jumpToAirport JS prepends "K" to bare 3-letter codes, so users typing
    `WVI` still resolve to KWVI without alias entries. Cached per-process.
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
        f'<b>Data: {start} - {end}</b>\n'
        '</div>\n'
    )


def _airport_jump_panel_html() -> str:
    """Upper-left panel: airport-code jump input + small acknowledgement footer.
    The 0.8rem font matches v1's `.stat-label` (smallest acknowledgement-style
    font in v1, see html/index_template.html).
    """
    return (
        '<div id="airport-jump-box">\n'
        '<label style="display:flex; align-items:center; gap:6px;">\n'
        '  Airport Code:\n'
        '  <input type="text" id="airport-jump" maxlength="4" autocomplete="off"\n'
        '         style="width:60px; padding:2px 4px;">\n'
        '  <button onclick="jumpToAirport()" style="padding:2px 8px;">Go</button>\n'
        '</label>\n'
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
        '  if (/^[A-Z]{3}$/.test(code)) code = "K" + code;\n'
        '  var loc = AIRPORTS[code];\n'
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
    all_bands_ordered = ["0k-3k", "3k-6k", "6k-10k"]
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
        # Tile URL relative to the HTML output (tiles live next to the HTML or at a known path)
        sources["traffic"] = {
            "type": "raster",
            "tiles": [traffic_tile_dir.rstrip("/") + "/{z}/{x}/{y}.png"],
            "tileSize": 256,
            "attribution": "Traffic Density",
        }
        layers.append({"id": "traffic-layer", "type": "raster", "source": "traffic",
                        "paint": {"raster-opacity": 0.7}})

    style_json = json.dumps({"version": 8, "glyphs": glyphs_url,
                              "sources": sources, "layers": layers})

    # The JS animation code as a separate string to avoid f-string brace issues
    animation_js = (
        'var _animRaf = null, _animT = 0, _tooltip = null, _animSpeed = 1;\n'
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
        '    var slower = document.createElement("span");\n'
        '    slower.textContent = "\\u23EA";\n'
        '    slower.title = "Slower";\n'
        '    slower.style.cssText = btnStyle;\n'
        '    slower.onclick = function() { _animSpeed = Math.max(0.1, _animSpeed / 2); };\n'
        '    var pauseBtn = document.createElement("span");\n'
        '    pauseBtn.textContent = "\\u23F8";\n'
        '    pauseBtn.title = "Pause/Resume";\n'
        '    pauseBtn.style.cssText = btnStyle;\n'
        '    pauseBtn.onclick = function() { _animSpeed = _animSpeed ? 0 : 1; };\n'
        '    var faster = document.createElement("span");\n'
        '    faster.textContent = "\\u23E9";\n'
        '    faster.title = "Faster";\n'
        '    faster.style.cssText = btnStyle;\n'
        '    faster.onclick = function() { _animSpeed = Math.min(3, (_animSpeed || 1) * 2); };\n'
        '    controls.appendChild(slower);\n'
        '    controls.appendChild(pauseBtn);\n'
        '    controls.appendChild(faster);\n'
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
        '    _animT += 0.5 * _animSpeed;\n'
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
            '<b>Event Altitude Filter (MSL)</b><br>\n'
            + alt_band_checkboxes +
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;cursor:pointer">'
            '<input type="checkbox" id="show-low-cb"> Show low-quality events</label>\n'
            '</div>\n'
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;font-size:11px;margin-bottom:2px">Heatmap Opacity: <span id="heatmap-opacity-val">30</span>%</label>\n'
            '<input type="range" id="heatmap-opacity-slider" min="0" max="75" value="30" style="width:100%">\n'
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
        '  touchPitch: false\n'
        '});\n'
        'map.touchZoomRotate.disableRotation();\n'
        'map.addControl(new maplibregl.NavigationControl({visualizePitch: false}));\n'
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
        '      "heatmap-opacity": ["interpolate", ["linear"], ["zoom"], 7, 0, 9, 0.3, 14, 0.3]\n'
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
        '    var bandFilter = checked.length\n'
        '      ? ["in", ["get", "alt_band"], ["literal", checked]]\n'
        '      : ["==", "1", "0"]; // nothing matches if all unchecked\n'
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

    all_bands_ordered = ["0k-3k", "3k-6k", "6k-10k"]
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
        pm_sources["traffic"] = {
            "type": "raster",
            "tiles": [traffic_tile_dir.rstrip("/") + "/{z}/{x}/{y}.png"],
            "tileSize": 256,
            "attribution": "Traffic Density",
        }
        pm_layers.append({"id": "traffic-layer", "type": "raster", "source": "traffic",
                           "paint": {"raster-opacity": 0.7}})
    style_json = json.dumps({"version": 8, "glyphs": glyphs_url,
                              "sources": pm_sources, "layers": pm_layers})

    # Animation JS: index fetched once, then per-click Range request for just that event's track line.
    animation_js = (
        'var _animRaf = null, _animT = 0, _tooltip = null, _animSpeed = 1;\n'
        'var _trackSources = [], _fetchGen = 0, _tracksIndex = null;\n'
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
        '    var slower = document.createElement("span");\n'
        '    slower.textContent = "\\u23EA";\n'
        '    slower.title = "Slower";\n'
        '    slower.style.cssText = btnStyle;\n'
        '    slower.onclick = function() { _animSpeed = Math.max(0.1, _animSpeed / 2); };\n'
        '    var pauseBtn = document.createElement("span");\n'
        '    pauseBtn.textContent = "\\u23F8";\n'
        '    pauseBtn.title = "Pause/Resume";\n'
        '    pauseBtn.style.cssText = btnStyle;\n'
        '    pauseBtn.onclick = function() { _animSpeed = _animSpeed ? 0 : 1; };\n'
        '    var faster = document.createElement("span");\n'
        '    faster.textContent = "\\u23E9";\n'
        '    faster.title = "Faster";\n'
        '    faster.style.cssText = btnStyle;\n'
        '    faster.onclick = function() { _animSpeed = Math.min(3, (_animSpeed || 1) * 2); };\n'
        '    controls.appendChild(slower);\n'
        '    controls.appendChild(pauseBtn);\n'
        '    controls.appendChild(faster);\n'
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
        '    _animT += 0.5 * _animSpeed;\n'
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
        # On click: load index once, then Range-fetch just this event's track line
        'function startAnimation(props) {\n'
        '  clearAnimation();\n'
        '  _animSpeed = 1;\n'
        '  if (props.html) showTooltip(props.html);\n'
        '  var gen = _fetchGen;\n'
        '  var eid = String(props.event_id);\n'
        '  function _animate() {\n'
        '    if (gen !== _fetchGen) return;\n'
        '    var loc = _tracksIndex[eid];\n'
        '    if (!loc) { console.warn("no index entry for event_id", eid); return; }\n'
        '    var blobUrl = "' + sidecar_rel + '/tracks.ndjson";\n'
        '    fetch(blobUrl, {headers: {"Range": "bytes=" + loc[0] + "-" + (loc[0]+loc[1]-1)}})\n'
        '      .then(function(r) { return r.text(); })\n'
        '      .then(function(text) {\n'
        '        if (gen !== _fetchGen) return;\n'
        '        var data = JSON.parse(text);\n'
        '        data.flight1 = props.flight1;\n'
        '        data.flight2 = props.flight2;\n'
        '        runAnimation(data);\n'
        '      })\n'
        '      .catch(function(e) { console.warn("track fetch failed:", e); });\n'
        '  }\n'
        '  if (_tracksIndex) {\n'
        '    _animate();\n'
        '  } else {\n'
        '    // The index is gzipped on R2 but Cloudflare CDN sometimes\n'
        '    // forwards the body without Content-Encoding: gzip, so we\n'
        '    // decompress in JS using DecompressionStream (Chrome 80+,\n'
        '    // Firefox 113+, Safari 16.4+) for cross-CDN robustness.\n'
        '    fetch("' + sidecar_rel + '/tracks.index.json.gz")\n'
        '      .then(function(r) {\n'
        '        // If the CDN already decompressed for us, the body is\n'
        '        // plain JSON; otherwise it\\u2019s gzip bytes we have to\n'
        '        // decompress ourselves. Detect via the gzip magic 1f 8b.\n'
        '        return r.arrayBuffer().then(function(buf) {\n'
        '          var bytes = new Uint8Array(buf);\n'
        '          if (bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b) {\n'
        '            var blob = new Blob([buf]);\n'
        '            var ds = new DecompressionStream("gzip");\n'
        '            return new Response(blob.stream().pipeThrough(ds)).json();\n'
        '          }\n'
        '          return new Response(buf).json();\n'
        '        });\n'
        '      })\n'
        '      .then(function(idx) { _tracksIndex = idx; _animate(); })\n'
        '      .catch(function(e) { console.warn("index fetch failed:", e); });\n'
        '  }\n'
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
            '<b>Event Altitude Filter</b><br>\n'
            + alt_band_checkboxes +
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;cursor:pointer">'
            '<input type="checkbox" id="show-low-cb"> Show low-quality events</label>\n'
            '</div>\n'
            '<div style="margin-top:8px;border-top:1px solid #555;padding-top:6px">\n'
            '<label style="display:block;font-size:11px;margin-bottom:2px">Heatmap Opacity: <span id="heatmap-opacity-val">30</span>%</label>\n'
            '<input type="range" id="heatmap-opacity-slider" min="0" max="75" value="30" style="width:100%">\n'
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
        '  touchPitch: false\n'
        '});\n'
        'map.touchZoomRotate.disableRotation();\n'
        'map.addControl(new maplibregl.NavigationControl({visualizePitch: false}));\n'
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
        '            "heatmap-opacity": ["interpolate", ["linear"], ["zoom"], 7, 0, 9, 0.3, 14, 0.3]}\n'
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
        '    startAnimation(e.features[0].properties);\n'
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
        '    var bandFilter = checked.length\n'
        '      ? ["in", ["get", "alt_band"], ["literal", checked]]\n'
        '      : ["==", "1", "0"];\n'
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

    if args.pmtiles:
        # PMTiles mode: generate .pmtiles + per-event sidecar JSONs + shell HTML
        pmtiles_path = generate_pmtiles(df, output_path)

        sidecar_dir = output_path.replace(".html", "_tracks")
        print(f"  Writing {len(df):,} event track sidecars to {sidecar_dir}/...", flush=True)
        write_event_sidecars(df, sidecar_dir)

        html = generate_pmtiles_html(
            pmtiles_path, sidecar_dir,
            center_lat, center_lon, zoom,
            alt_bands, faa_basemap=not args.no_faa_basemap,
            traffic_tile_dir=args.traffic_tiles,
            date_range=date_range,
            airports_lookup=airports_lookup,
            asset_stem=args.asset_stem,
            bounds=bounds,
            auto_fit=auto_fit,
        )
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"\nDone (PMTiles mode). Output: {output_path}")
        print(f"  NOTE: Requires HTTP serving from project root — open via:")
        print(f"    python src/hotspots/serve.py . 8080")
        print(f"    Then: http://localhost:8080/{output_path}")
    else:
        # Self-contained mode: all data embedded in HTML (practical up to ~500 events)
        html = generate_html(df, center_lat, center_lon, zoom,
                             faa_basemap=not args.no_faa_basemap,
                             traffic_tile_dir=args.traffic_tiles,
                             date_range=date_range,
                             airports_lookup=airports_lookup,
                             bounds=bounds,
                             auto_fit=auto_fit)

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"\nDone. Output: {output_path} ({size_mb:.1f} MB)")
        print(f"  Open in browser: file://{os.path.abspath(output_path)}")


if __name__ == "__main__":
    main()

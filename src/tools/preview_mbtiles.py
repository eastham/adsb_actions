#!/usr/bin/env python3
"""Preview one or more MBTiles raster files in a browser using MapLibre + OSM.

Starts a local HTTP server that serves tile PNG data directly from the SQLite
MBTiles files, then opens a browser with a MapLibre map showing all layers
with individual opacity sliders.

Usage:
    python src/tools/preview_mbtiles.py path/to/Traffic\ Density.mbtiles path/to/LOS\ Events.mbtiles
    python src/tools/preview_mbtiles.py --port 8081 *.mbtiles
    python src/tools/preview_mbtiles.py --zip data/v2/foreflight/pack.zip

The server runs until Ctrl-C.
"""

import argparse
import http.server
import io
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time
import webbrowser
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT / "src"), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from hotspots.serve import RangeHTTPRequestHandler

DEFAULT_PORT = 8090

# ---------------------------------------------------------------------------
# MBTiles tile server
# ---------------------------------------------------------------------------

def _tms_to_slippy(ty: int, zoom: int) -> int:
    """Convert MBTiles TMS tile_row back to slippy-map y."""
    return (2 ** zoom - 1) - ty


class MBTilesHandler(RangeHTTPRequestHandler):
    """HTTP handler that serves:
      /tiles/<layer>/<z>/<x>/<y>.png  — from registered MBTiles SQLite files
      /traffic/<z>/<x>/<y>.png        — from a slippy-map tile directory on disk
    All other paths fall through to the static file handler (serves the
    preview HTML from the temp directory set as cwd).
    """

    # Set by the server before starting: {layer_name: Path}
    mbtiles_map: dict[str, Path] = {}
    # Set by the server when a local traffic tile dir is provided
    traffic_tile_dir: Path | None = None

    _TILE_RE    = re.compile(r"^/tiles/([^/]+)/(\d+)/(\d+)/(\d+)\.png$")
    _TRAFFIC_RE = re.compile(r"^/traffic/(\d+)/(\d+)/(\d+)\.png$")

    def do_GET(self):
        m = self._TILE_RE.match(self.path)
        if m:
            self._serve_tile(*m.groups())
            return
        m = self._TRAFFIC_RE.match(self.path)
        if m:
            self._serve_traffic_tile(*m.groups())
            return
        super().do_GET()

    def _serve_tile(self, layer: str, z: str, x: str, y: str):
        z, x, y = int(z), int(x), int(y)
        # y in the URL is slippy; MBTiles stores TMS rows
        tms_row = _tms_to_slippy(y, z)

        db_path = self.mbtiles_map.get(layer)
        if db_path is None:
            self.send_error(404, f"Unknown layer: {layer}")
            return

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            row = conn.execute(
                "SELECT tile_data FROM tiles "
                "WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                (z, x, tms_row),
            ).fetchone()
            conn.close()
        except Exception as e:
            self.send_error(500, str(e))
            return

        if row is None:
            self._send_empty_tile()
            return

        data = row[0]
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_traffic_tile(self, z: str, x: str, y: str):
        if self.traffic_tile_dir is None:
            self._send_empty_tile()
            return
        tile_path = self.traffic_tile_dir / z / x / f"{y}.png"
        if not tile_path.exists():
            self._send_empty_tile()
            return
        data = tile_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_empty_tile(self):
        # Minimal valid 1×1 transparent PNG
        EMPTY_PNG = (
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01'
            b'\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89'
            b'\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01'
            b'\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82'
        )
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(EMPTY_PNG)))
        self.end_headers()
        self.wfile.write(EMPTY_PNG)

    def log_message(self, fmt, *args):
        # Suppress per-tile noise; only log non-tile requests
        if not self._TILE_RE.match(self.path):
            super().log_message(fmt, *args)


# ---------------------------------------------------------------------------
# MBTiles metadata helpers
# ---------------------------------------------------------------------------

def _read_metadata(db_path: Path) -> dict:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows = conn.execute("SELECT name, value FROM metadata").fetchall()
    conn.close()
    return dict(rows)


def _parse_bounds(meta: dict) -> tuple[float, float, float, float]:
    """Return (minlon, minlat, maxlon, maxlat) from metadata, with CONUS fallback."""
    b = meta.get("bounds", "-125.0,24.5,-66.0,49.5")
    parts = [float(v) for v in b.split(",")]
    return tuple(parts)  # minlon, minlat, maxlon, maxlat


# ---------------------------------------------------------------------------
# Preview HTML generation
# ---------------------------------------------------------------------------

_LAYER_COLORS = [
    # traffic-blue-ish legend swatch
    "rgba(80, 80, 255, 0.8)",
    # LOS warm-red swatch
    "rgba(220, 60, 0, 0.8)",
    "rgba(0, 180, 80, 0.8)",
    "rgba(200, 0, 200, 0.8)",
]

def _build_preview_html(layers: list[dict], port: int,
                        traffic_tile_dir: Path | None = None) -> str:
    """Generate a self-contained MapLibre HTML preview page.

    layers: list of {name, url_key, minzoom, maxzoom, bounds}
    traffic_tile_dir: if set, adds a traffic density layer from the local tile tree
    """
    # Compute union bounds across all layers for initial view
    all_bounds = [l["bounds"] for l in layers]
    minlon = min(b[0] for b in all_bounds)
    minlat = min(b[1] for b in all_bounds)
    maxlon = max(b[2] for b in all_bounds)
    maxlat = max(b[3] for b in all_bounds)
    center_lon = (minlon + maxlon) / 2
    center_lat = (minlat + maxlat) / 2

    # Rough zoom to fit bounds
    lat_span = maxlat - minlat
    lon_span = maxlon - minlon
    zoom = max(2, min(10, int(math.log2(360 / max(lat_span, lon_span))) + 1))

    # Detect traffic zoom range from directory structure
    traffic_minzoom, traffic_maxzoom = 5, 11
    if traffic_tile_dir:
        zoom_dirs = [int(d.name) for d in traffic_tile_dir.iterdir()
                     if d.is_dir() and d.name.isdigit()]
        if zoom_dirs:
            traffic_minzoom, traffic_maxzoom = min(zoom_dirs), max(zoom_dirs)

    # Build MapLibre sources and layers JS
    extra_sources = {
        l["url_key"]: {
            "type": "raster",
            "tiles": [f"http://localhost:{port}/tiles/{l['url_key']}/{{z}}/{{x}}/{{y}}.png"],
            "tileSize": 256,
            "minzoom": l["minzoom"],
            "maxzoom": l["maxzoom"],
        }
        for l in layers
    }
    if traffic_tile_dir:
        extra_sources["traffic"] = {
            "type": "raster",
            "tiles": [f"http://localhost:{port}/traffic/{{z}}/{{x}}/{{y}}.png"],
            "tileSize": 256,
            "minzoom": traffic_minzoom,
            "maxzoom": traffic_maxzoom,
        }

    sources_js = json.dumps({
        "osm": {
            "type": "raster",
            "tiles": ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            "tileSize": 256,
            "attribution": "© OpenStreetMap contributors",
            "maxzoom": 19,
        },
        **extra_sources,
    }, indent=2)

    extra_map_layers = [
        {"id": l["url_key"], "type": "raster", "source": l["url_key"],
         "paint": {"raster-opacity": 0.7}}
        for l in layers
    ]
    if traffic_tile_dir:
        extra_map_layers.insert(0, {
            "id": "traffic", "type": "raster", "source": "traffic",
            "paint": {"raster-opacity": 0.7},
        })

    map_layers_js = json.dumps([
        {"id": "osm", "type": "raster", "source": "osm",
         "paint": {"raster-opacity": 0.8}},
        *extra_map_layers,
    ], indent=2)

    # Opacity sliders — traffic first if present, then mbtiles layers
    slider_items = []
    if traffic_tile_dir:
        slider_items.append(("traffic", "Traffic Density", "rgba(80,80,255,0.8)", 70))
    for i, l in enumerate(layers):
        color = _LAYER_COLORS[(i + (1 if traffic_tile_dir else 0)) % len(_LAYER_COLORS)]
        slider_items.append((l["url_key"], l["name"], color, 70))

    sliders_html = "\n".join(
        f'<div style="margin-bottom:8px">'
        f'<span style="display:inline-block;width:12px;height:12px;'
        f'border-radius:2px;background:{color};margin-right:6px;vertical-align:middle"></span>'
        f'<b>{name}</b><br>'
        f'<input type="range" min="0" max="100" value="{val}" style="width:100%" '
        f'oninput="map.setPaintProperty(\'{key}\',\'raster-opacity\',this.value/100);'
        f'document.getElementById(\'lbl-{i}\').textContent=this.value+\'%\'">'
        f'<span id="lbl-{i}">{val}%</span>'
        f'</div>'
        for i, (key, name, color, val) in enumerate(slider_items)
    )

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>MBTiles Preview</title>
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
<style>
body {{ margin:0; padding:0; }}
#map {{ position:absolute; top:0; bottom:0; width:100%; }}
#panel {{
  position:fixed; top:10px; right:10px; z-index:1000;
  background:rgba(0,0,0,0.78); color:#fff;
  padding:12px 16px; border-radius:8px;
  font:13px/1.5 sans-serif; min-width:200px;
}}
#panel h3 {{ margin:0 0 10px; font-size:14px; }}
</style>
</head>
<body>
<div id="map"></div>
<div id="panel">
  <h3>MBTiles Preview</h3>
  {sliders_html}
  <div style="margin-top:10px;border-top:1px solid #555;padding-top:8px;font-size:11px;color:#aaa">
    Basemap: OpenStreetMap
  </div>
</div>
<script>
var map = new maplibregl.Map({{
  container: 'map',
  style: {{ version: 8, sources: {sources_js}, layers: {map_layers_js} }},
  center: [{center_lon}, {center_lat}],
  zoom: {zoom},
}});
map.addControl(new maplibregl.NavigationControl());
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _extract_zip_layers(zip_path: Path, tmp_dir: Path) -> list[Path]:
    """Extract .mbtiles files from a ForeFlight Content Pack zip."""
    found = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith(".mbtiles"):
                dest = tmp_dir / Path(name).name
                dest.write_bytes(zf.read(name))
                found.append(dest)
    return found


def preview(mbtiles_paths: list[Path], port: int = DEFAULT_PORT,
            traffic_tile_dir: Path | None = None) -> None:
    tmp_dir = Path(tempfile.mkdtemp(prefix="mbtiles_preview_"))

    layers = []
    for p in mbtiles_paths:
        meta = _read_metadata(p)
        bounds = _parse_bounds(meta)
        url_key = re.sub(r"[^a-zA-Z0-9_-]", "_", p.stem)
        layers.append({
            "name": p.stem,
            "url_key": url_key,
            "minzoom": int(meta.get("minzoom", 0)),
            "maxzoom": int(meta.get("maxzoom", 19)),
            "bounds": bounds,
        })

    MBTilesHandler.mbtiles_map = {l["url_key"]: p
                                   for l, p in zip(layers, mbtiles_paths)}
    MBTilesHandler.traffic_tile_dir = traffic_tile_dir

    html = _build_preview_html(layers, port, traffic_tile_dir=traffic_tile_dir)
    html_path = tmp_dir / "index.html"
    html_path.write_text(html, encoding="utf-8")

    os.chdir(tmp_dir)
    server = http.server.HTTPServer(("", port), MBTilesHandler)

    url = f"http://localhost:{port}/"
    n_layers = len(layers) + (1 if traffic_tile_dir else 0)
    print(f"Serving {n_layers} layer(s) on {url}")
    if traffic_tile_dir:
        print(f"  Traffic Density  (from {traffic_tile_dir})")
    for l in layers:
        print(f"  {l['name']}")
    print("Press Ctrl-C to stop.\n")

    threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("files", nargs="*", metavar="FILE.mbtiles",
                    help="One or more .mbtiles files to preview")
    ap.add_argument("--zip", metavar="PACK.zip",
                    help="ForeFlight Content Pack .zip — extracts and previews all layers")
    ap.add_argument("--traffic-tiles", metavar="DIR",
                    help="Local slippy-map tile tree (e.g. tiles/traffic) to include as a layer")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT,
                    help=f"Port to serve on (default: {DEFAULT_PORT})")
    args = ap.parse_args()

    if not args.files and not args.zip:
        ap.error("Provide .mbtiles files or --zip PACK.zip")

    paths: list[Path] = []

    if args.zip:
        tmp = Path(tempfile.mkdtemp(prefix="mbtiles_zip_"))
        print(f"Extracting {args.zip}…")
        paths.extend(_extract_zip_layers(Path(args.zip), tmp))
        if not paths:
            ap.error(f"No .mbtiles files found in {args.zip}")

    for f in args.files:
        p = Path(f)
        if not p.exists():
            ap.error(f"File not found: {p}")
        paths.append(p)

    traffic_dir = Path(args.traffic_tiles) if args.traffic_tiles else None
    if traffic_dir and not traffic_dir.is_dir():
        ap.error(f"--traffic-tiles dir not found: {traffic_dir}")

    preview(paths, port=args.port, traffic_tile_dir=traffic_dir)


if __name__ == "__main__":
    main()

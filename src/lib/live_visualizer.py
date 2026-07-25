"""Lightweight live aircraft visualizer for AdsbActions apps.

Serves a single OSM/Leaflet map page on a background HTTP thread. The browser
polls a JSON endpoint ~1x/sec; the server snapshots the live flight store on
each request. Aircraft appear as dots with a small data block (tail + altitude)
that moves with them.

Decoupled from any particular app and from the YAML rules -- it reads all
current aircraft straight from adsb_actions.flights.flight_dict, so any
AdsbActions-based app can enable it in a couple of lines:

    from lib.live_visualizer import LiveVisualizer
    viz = LiveVisualizer(adsb_actions, center_lat, center_lon)
    viz.start()   # ... viz.stop() on shutdown
"""

import json
import logging
import math
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

logger = logging.getLogger(__name__)

DEFAULT_PORT = 8011
POLL_INTERVAL_MS = 1000  # browser refresh cadence


def _zoom_for_width(width_nm, center_lat):
    """Pick a Leaflet zoom level so that width_nm spans roughly the viewport.

    Uses the Web Mercator relationship between zoom and ground resolution;
    assumes an ~1000px-wide viewport. Approximate is fine -- the map is
    scroll/zoomable anyway.
    """
    width_m = width_nm * 1852.0
    viewport_px = 1000.0
    # meters-per-pixel at the equator for zoom 0 is ~156543; it scales by
    # cos(lat) / 2**zoom. Solve for the zoom that fits width_m across the
    # viewport.
    lat_rad = math.radians(center_lat)
    zoom = math.log2(156543.03392 * math.cos(lat_rad) * viewport_px / width_m)
    return max(1, min(18, round(zoom)))


class LiveVisualizer:
    """Runs a background HTTP server that renders live aircraft on a map."""

    def __init__(self, adsb_actions, center_lat, center_lon,
                 width_nm=20.0, port=DEFAULT_PORT):
        self.adsb_actions = adsb_actions
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.width_nm = width_nm
        self.port = port
        self.zoom = _zoom_for_width(width_nm, center_lat)
        self._server = None
        self._thread = None

    def _aircraft_snapshot(self):
        """Return the current aircraft as a JSON-serializable payload.

        Snapshots the live flight store under its lock so we don't read a
        half-updated dict while the data thread is mutating it.
        """
        aircraft = []
        flights = self.adsb_actions.flights
        with flights.lock:
            # copy to a list first so we hold the lock only briefly
            current = list(flights.flight_dict.values())

        for f in current:
            loc = f.lastloc
            # Skip aircraft without a usable position (e.g. only ever
            # reported on-ground with no lat/lon).
            if not loc or not loc.lat or not loc.lon:
                continue
            aircraft.append({
                "tail": f.flight_id,
                "lat": loc.lat,
                "lon": loc.lon,
                "alt": loc.alt_baro,
                "track": loc.track,
                "gs": loc.gs,
            })

        return {
            "center": [self.center_lat, self.center_lon],
            "zoom": self.zoom,
            "poll_ms": POLL_INTERVAL_MS,
            "aircraft": aircraft,
        }

    def _make_handler(self):
        visualizer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.startswith("/aircraft.json"):
                    body = json.dumps(
                        visualizer._aircraft_snapshot()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path == "/" or self.path.startswith("/index"):
                    body = _PAGE_HTML.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(404)

            def log_message(self, *args):
                # Silence per-request stderr logging so it doesn't spam the
                # app console.
                pass

        return Handler

    def start(self):
        """Start the HTTP server on a daemon thread."""
        if self._server is not None:
            return
        self._server = HTTPServer(("", self.port), self._make_handler())
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True,
            name="live-visualizer")
        self._thread.start()
        logger.info("Live visualizer running at http://localhost:%d/",
                    self.port)

    def stop(self):
        """Shut down the HTTP server and its thread."""
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._server = None
        self._thread = None


# Single self-contained page. Leaflet is fetched from the CDN by the browser;
# the Python server itself has no external dependencies. Config (center, zoom,
# poll interval) comes from /aircraft.json so the page needs no templating.
_PAGE_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AdsbActions live view</title>
<link rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  html, body, #map { height: 100%; margin: 0; }
  .ac-label {
    background: rgba(0,0,0,0.65); color: #fff; border: none;
    font: 11px/1.2 monospace; padding: 1px 3px; white-space: pre;
    box-shadow: none;
  }
  .ac-label::before { display: none; }  /* hide tooltip pointer */
</style>
</head>
<body>
<div id="map"></div>
<script>
let map, pollMs = 1000;
const markers = {};   // tail -> {dot, label}

function labelText(ac) {
  const alt = (ac.alt === "ground" || ac.alt === 0) ? "gnd" : ac.alt;
  return ac.tail + "\\n" + alt;
}

function render(data) {
  const seen = {};
  for (const ac of data.aircraft) {
    seen[ac.tail] = true;
    const pos = [ac.lat, ac.lon];
    let m = markers[ac.tail];
    if (!m) {
      const dot = L.circleMarker(pos, {
        radius: 4, color: "#0a3", weight: 1,
        fillColor: "#0f6", fillOpacity: 0.9
      }).addTo(map);
      dot.bindTooltip(labelText(ac), {
        permanent: true, direction: "right", offset: [6, 0],
        className: "ac-label"
      });
      markers[ac.tail] = { dot };
    } else {
      m.dot.setLatLng(pos);
      m.dot.setTooltipContent(labelText(ac));
    }
  }
  // Remove aircraft that have expired out of the feed.
  for (const tail of Object.keys(markers)) {
    if (!seen[tail]) {
      map.removeLayer(markers[tail].dot);
      delete markers[tail];
    }
  }
}

function poll() {
  fetch("/aircraft.json")
    .then(r => r.json())
    .then(data => {
      if (!map) {
        map = L.map("map").setView(data.center, data.zoom);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          maxZoom: 19,
          attribution: "\\u00a9 OpenStreetMap contributors"
        }).addTo(map);
        pollMs = data.poll_ms || pollMs;
      }
      render(data);
    })
    .catch(e => console.warn("poll failed", e))
    .finally(() => setTimeout(poll, pollMs));
}
poll();
</script>
</body>
</html>
"""

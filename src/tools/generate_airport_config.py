#!/usr/bin/env python3
"""
Quick setup for monitoring any airport - just provide the ICAO code.

This script automatically downloads airport/runway data and generates all the
config files needed to run stripview or LOS detection for your airport.

Generated files:
- KML file with Ground, Departure, Approach, and Vicinity regions
- Rules YAML for LOS detection and takeoff/landing monitoring
- Stripview YAML for the visual flight strip UI

Usage:
    python3 src/tools/generate_airport_config.py KSQL
    python3 src/tools/generate_airport_config.py KSQL --runway 30
"""

import argparse
import csv
import math
import sys
import urllib.request
from pathlib import Path

# Constants for wedge geometry
DEPARTURE_LENGTH_NM = 3.0
APPROACH_LENGTH_NM = 5.0
DEPARTURE_HEADING_RANGE = 25  # ±25° from runway heading
APPROACH_HEADING_RANGE = 30   # ±30° from runway heading
VICINITY_RADIUS_NM = 10.0
GROUND_BUFFER_NM = 0.3  # Buffer added to ground radius to close lateral gap with app/dep wedges
# Ground region radius = (runway length / 2) + buffer

# Altitude offsets from field elevation (feet)
GROUND_MIN_ALT_OFFSET = -500  # Below field elevation to handle pressure altitude variations
GROUND_ALT_OFFSET = 500
# Departure/approach floor matches ground min to avoid altitude gaps during transitions
DEP_APP_MIN_ALT_OFFSET = GROUND_MIN_ALT_OFFSET
DEPARTURE_ALT_OFFSET = 3000
APPROACH_ALT_OFFSET = 5000
VICINITY_ALT_OFFSET = 10000

# OurAirports data URLs
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
RUNWAYS_URL = "https://davidmegginson.github.io/ourairports-data/runways.csv"

# Cache directory
CACHE_DIR = Path(__file__).parent / ".cache"


def download_with_cache(url: str, filename: str) -> Path:
    """Download file if not cached, return path to cached file."""
    CACHE_DIR.mkdir(exist_ok=True)
    cache_path = CACHE_DIR / filename

    if not cache_path.exists():
        print(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, cache_path)
        print(f"  Cached to {cache_path}")

    return cache_path


def load_airport(icao: str) -> dict | None:
    """Load airport data by ICAO code or local code.

    Searches by ident first, then gps_code, then local_code for airports where
    the identifier differs from the OurAirports ident (e.g., KREG -> KL38,
    S50 -> KS50).
    """
    airports_path = download_with_cache(AIRPORTS_URL, "airports.csv")
    icao_upper = icao.upper()

    with open(airports_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['ident'].upper() == icao_upper:
                return row
            # Also check gps_code for airports like KREG (ident=KL38, gps_code=KREG)
            if row.get('gps_code', '').upper() == icao_upper:
                return row
            # Also check local_code for airports like S50 (ident=KS50, local_code=S50)
            if row.get('local_code', '').upper() == icao_upper:
                return row
    return None


def load_runways(icao: str) -> list[dict]:
    """Load all runways for an airport."""
    runways_path = download_with_cache(RUNWAYS_URL, "runways.csv")
    runways = []

    with open(runways_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Match by airport_ident field
            if row['airport_ident'].upper() == icao.upper():
                runways.append(row)

    return runways


def get_longest_runway(runways: list[dict]) -> dict | None:
    """Return the longest runway."""
    if not runways:
        return None

    def runway_length(rwy):
        try:
            return int(rwy.get('length_ft', 0) or 0)
        except ValueError:
            return 0

    return max(runways, key=runway_length)


def get_lower_runway_end(runway: dict) -> str:
    """Return the identifier of the lower-numbered runway end."""
    le_ident = runway.get('le_ident', '')
    he_ident = runway.get('he_ident', '')

    # Extract numeric portion for comparison
    def extract_num(ident):
        num = ''.join(c for c in ident if c.isdigit())
        return int(num) if num else 99

    if extract_num(le_ident) <= extract_num(he_ident):
        return le_ident
    return he_ident


def get_runway_end_data(runway: dict, end_ident: str) -> dict | None:
    """Get data for a specific runway end (threshold coords, heading, etc.)."""
    le_ident = runway.get('le_ident', '')
    he_ident = runway.get('he_ident', '')

    if end_ident.upper() == le_ident.upper():
        prefix = 'le_'
    elif end_ident.upper() == he_ident.upper():
        prefix = 'he_'
    else:
        return None

    try:
        lat = float(runway.get(f'{prefix}latitude_deg') or 0)
        lon = float(runway.get(f'{prefix}longitude_deg') or 0)
        heading = float(runway.get(f'{prefix}heading_degT') or 0)
        elevation = float(runway.get(f'{prefix}elevation_ft') or 0)
    except (ValueError, TypeError):
        return None

    if lat == 0 and lon == 0:
        return None

    return {
        'ident': end_ident,
        'lat': lat,
        'lon': lon,
        'heading': heading,
        'elevation': elevation,
        'width_ft': float(runway.get('width_ft') or 75),
    }


def get_opposite_runway_end(runway: dict, end_ident: str) -> str | None:
    """Get the identifier of the opposite runway end."""
    le_ident = runway.get('le_ident', '')
    he_ident = runway.get('he_ident', '')

    if end_ident.upper() == le_ident.upper():
        return he_ident
    elif end_ident.upper() == he_ident.upper():
        return le_ident
    return None


def nm_to_deg_lat(nm: float) -> float:
    """Convert nautical miles to degrees latitude."""
    return nm / 60.0


def nm_to_deg_lon(nm: float, lat: float) -> float:
    """Convert nautical miles to degrees longitude at given latitude."""
    return nm / (60.0 * math.cos(math.radians(lat)))


def point_at_distance_bearing(lat: float, lon: float, distance_nm: float, bearing_deg: float) -> tuple[float, float]:
    """Calculate point at distance and bearing from origin."""
    bearing_rad = math.radians(bearing_deg)

    delta_lat = nm_to_deg_lat(distance_nm) * math.cos(bearing_rad)
    delta_lon = nm_to_deg_lon(distance_nm, lat) * math.sin(bearing_rad)

    return lat + delta_lat, lon + delta_lon


def generate_wedge_polygon(threshold_lat: float, threshold_lon: float,
                           heading: float, length_nm: float,
                           base_width_nm: float, far_width_nm: float) -> list[tuple[float, float]]:
    """Generate wedge-shaped polygon coordinates extending from runway threshold."""
    # Points at threshold (base of wedge)
    left_base = point_at_distance_bearing(threshold_lat, threshold_lon,
                                          base_width_nm / 2, heading - 90)
    right_base = point_at_distance_bearing(threshold_lat, threshold_lon,
                                           base_width_nm / 2, heading + 90)

    # Point at far end of wedge
    far_center = point_at_distance_bearing(threshold_lat, threshold_lon,
                                           length_nm, heading)

    left_far = point_at_distance_bearing(far_center[0], far_center[1],
                                         far_width_nm / 2, heading - 90)
    right_far = point_at_distance_bearing(far_center[0], far_center[1],
                                          far_width_nm / 2, heading + 90)

    # Return polygon (closed)
    return [left_base, left_far, right_far, right_base, left_base]


def generate_circle_polygon(center_lat: float, center_lon: float,
                            radius_nm: float, num_points: int = 36) -> list[tuple[float, float]]:
    """Generate circular polygon."""
    points = []
    for i in range(num_points + 1):
        bearing = (360 / num_points) * i
        point = point_at_distance_bearing(center_lat, center_lon, radius_nm, bearing)
        points.append(point)
    return points


def format_heading_range(heading: float, range_deg: float) -> tuple[int, int]:
    """Calculate heading range, handling wraparound."""
    start = int((heading - range_deg) % 360)
    end = int((heading + range_deg) % 360)
    return start, end


def polygon_to_kml_coords(polygon: list[tuple[float, float]]) -> str:
    """Convert polygon points to KML coordinate string."""
    coords = []
    for lat, lon in polygon:
        coords.append(f"{lon},{lat},0")
    return " ".join(coords)


def generate_kml(icao: str, airport_name: str, field_elevation: float,
                 app_end: dict, dep_end: dict, dep_liftoff_end: dict,
                 runway_length_ft: float,
                 center_lat: float, center_lon: float) -> str:
    """Generate complete KML file content.

    Args:
        app_end: The approach runway end (where aircraft land, threshold they cross)
        dep_end: The departure runway designation (for heading and naming)
        dep_liftoff_end: The opposite end of dep runway (where aircraft lift off/climb out)
    """

    app_ident = app_end['ident']
    dep_ident = dep_end['ident']
    app_heading = app_end['heading']
    dep_heading = dep_end['heading']
    runway_width_ft = app_end.get('width_ft', 75)

    # Approach threshold - where aircraft land
    approach_lat = app_end['lat']
    approach_lon = app_end['lon']
    # Departure liftoff point - the far end of the departure runway where aircraft climb out
    departure_lat = dep_liftoff_end['lat']
    departure_lon = dep_liftoff_end['lon']

    # Approach base width = 4x runway width (narrower at threshold)
    approach_base_nm = (runway_width_ft * 4) / 6076

    # Calculate MSL altitudes
    ground_min = int(field_elevation + GROUND_MIN_ALT_OFFSET)
    ground_max = int(field_elevation + GROUND_ALT_OFFSET)
    dep_app_min = int(field_elevation + DEP_APP_MIN_ALT_OFFSET)
    dep_max = int(field_elevation + DEPARTURE_ALT_OFFSET)
    app_max = int(field_elevation + APPROACH_ALT_OFFSET)
    vic_min = dep_app_min  # Same floor as departure/approach
    vic_max = int(field_elevation + VICINITY_ALT_OFFSET)

    # Calculate heading ranges - each uses its own runway's heading
    dep_hdg_start, dep_hdg_end = format_heading_range(dep_heading, DEPARTURE_HEADING_RANGE)
    app_hdg_start, app_hdg_end = format_heading_range(app_heading, APPROACH_HEADING_RANGE)

    # Reciprocal heading for approach polygon (extends opposite direction from approach threshold)
    app_recip_heading = (app_heading + 180) % 360

    # Generate polygons
    # Ground region radius = (runway length / 2) + buffer
    ground_radius_nm = (runway_length_ft / 6076) / 2 + GROUND_BUFFER_NM
    ground_poly = generate_circle_polygon(center_lat, center_lon, ground_radius_nm)

    # Departure extends outward from departure end in direction of departure heading
    dep_poly = generate_wedge_polygon(departure_lat, departure_lon,
                                      dep_heading, DEPARTURE_LENGTH_NM,
                                      0.15, 0.75)  # base ~900ft, far ~0.75nm

    # Approach extends outward from approach threshold (opposite direction aircraft is flying)
    # Base width = 4x runway width at threshold
    app_poly = generate_wedge_polygon(approach_lat, approach_lon,
                                      app_recip_heading, APPROACH_LENGTH_NM,
                                      approach_base_nm, 1.0)  # base = 4x runway width, far ~1nm

    vicinity_poly = generate_circle_polygon(center_lat, center_lon, VICINITY_RADIUS_NM)

    kml = f'''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
    <name>{icao} Regions</name>
    <description>Auto-generated regions for {airport_name}</description>

    <Style id="groundStyle">
        <PolyStyle><color>7d00ff00</color></PolyStyle>
        <LineStyle><color>ff00ff00</color><width>2</width></LineStyle>
    </Style>
    <Style id="depStyle">
        <PolyStyle><color>7dff0000</color></PolyStyle>
        <LineStyle><color>ffff0000</color><width>2</width></LineStyle>
    </Style>
    <Style id="appStyle">
        <PolyStyle><color>7d0000ff</color></PolyStyle>
        <LineStyle><color>ff0000ff</color><width>2</width></LineStyle>
    </Style>
    <Style id="vicStyle">
        <PolyStyle><color>3dffffff</color></PolyStyle>
        <LineStyle><color>ffffffff</color><width>1</width></LineStyle>
    </Style>

    <Folder>
        <name>{icao} Monitoring Regions</name>

        <Placemark>
            <name>RWY{dep_ident} Departure: {dep_app_min}-{dep_max} {dep_hdg_start}-{dep_hdg_end}</name>
            <styleUrl>#depStyle</styleUrl>
            <Polygon>
                <tessellate>1</tessellate>
                <outerBoundaryIs>
                    <LinearRing>
                        <coordinates>{polygon_to_kml_coords(dep_poly)}</coordinates>
                    </LinearRing>
                </outerBoundaryIs>
            </Polygon>
        </Placemark>

        <Placemark>
            <name>RWY{app_ident} Approach: {dep_app_min}-{app_max} {app_hdg_start}-{app_hdg_end}</name>
            <styleUrl>#appStyle</styleUrl>
            <Polygon>
                <tessellate>1</tessellate>
                <outerBoundaryIs>
                    <LinearRing>
                        <coordinates>{polygon_to_kml_coords(app_poly)}</coordinates>
                    </LinearRing>
                </outerBoundaryIs>
            </Polygon>
        </Placemark>

        <Placemark>
            <name>Ground: {ground_min}-{ground_max} 0-360</name>
            <styleUrl>#groundStyle</styleUrl>
            <Polygon>
                <tessellate>1</tessellate>
                <outerBoundaryIs>
                    <LinearRing>
                        <coordinates>{polygon_to_kml_coords(ground_poly)}</coordinates>
                    </LinearRing>
                </outerBoundaryIs>
            </Polygon>
        </Placemark>

        <Placemark>
            <name>Vicinity: {vic_min}-{vic_max} 0-360</name>
            <styleUrl>#vicStyle</styleUrl>
            <Polygon>
                <tessellate>1</tessellate>
                <outerBoundaryIs>
                    <LinearRing>
                        <coordinates>{polygon_to_kml_coords(vicinity_poly)}</coordinates>
                    </LinearRing>
                </outerBoundaryIs>
            </Polygon>
        </Placemark>

    </Folder>
</Document>
</kml>
'''
    return kml

def generate_prox_yaml(icao: str, top: int, bottom: int,
                       altsep: int=400, latsep: float=.3) -> str:
    """Generate proximity analysis YAML configuration."""

    return f'''# {icao} Proximity Analysis Configuration
# Auto-generated - analyzes LOS events within specified altitude range
rules:
    prox_analysis:
        conditions:
            min_alt: {bottom}
            max_alt: {top}
            proximity: [ {altsep}, {latsep} ] # alt sep in MSL, lateral sep in nm
        actions:
            callback: los_update_cb
'''


def generate_rules_yaml(icao: str, field_elevation: float, app_ident: str, dep_ident: str,
                        center_lat: float, center_lon: float) -> str:
    """Generate rules YAML for LOS and takeoff/landing detection."""

    los_min_alt = int(field_elevation + GROUND_ALT_OFFSET)
    los_max_alt = int(field_elevation + VICINITY_ALT_OFFSET)

    icao_lower = icao.lower()

    return f'''# {icao} Airport Monitor Rules
# Auto-generated - monitors takeoffs, landings, and loss-of-separation events

config:
  kmls:
    - examples/generated/{icao_lower}_regions.kml

rules:
  takeoff:
    conditions:
      latlongring: [{VICINITY_RADIUS_NM}, {center_lat}, {center_lon}]
      transition_regions: ["Ground", "RWY{dep_ident} Departure"]
    actions:
      print: True

  takeoff_popup:
    conditions:
      transition_regions: ["Ground", ~]
    actions:
      print: True

  landing:
    conditions:
      transition_regions: ["RWY{app_ident} Approach", "Ground"]
    actions:
      print: True

  vicinity_traffic:
    conditions:
      regions: ["RWY{dep_ident} Departure", "RWY{app_ident} Approach", "Vicinity", "Ground"]
      cooldown: 10
    actions:
      print: True

  region_change:
    conditions:
      changed_regions: strict
    actions:
      print: True

  prox:
    conditions:
      min_alt: {los_min_alt}
      max_alt: {los_max_alt}
      regions: ["Vicinity"]
      proximity: [400, .3]
    actions:
      print: True
'''


def generate_stripview_yaml(icao: str, field_elevation: float, app_ident: str, dep_ident: str,
                            center_lat: float, center_lon: float) -> str:
    """Generate stripview UI YAML."""

    los_min_alt = int(field_elevation + GROUND_ALT_OFFSET)
    los_max_alt = int(field_elevation + VICINITY_ALT_OFFSET)

    icao_lower = icao.lower()

    return f'''# {icao} Stripview UI Configuration
# Auto-generated - displays flight strips for airport traffic

config:
  kmls:
    - examples/generated/{icao_lower}_regions.kml
    - examples/generated/{icao_lower}_regions.kml

rules:
  ui_update:
    conditions:
      latlongring: [{VICINITY_RADIUS_NM}, {center_lat}, {center_lon}]
      regions: ["Ground", "RWY{dep_ident} Departure", "RWY{app_ident} Approach", "Vicinity"]
    actions:
      callback: aircraft_update_cb

  ui_remove:
    conditions:
      regions: []
    actions:
      callback: aircraft_remove_cb

  ui_expire:
    conditions:
      latlongring: [{VICINITY_RADIUS_NM}, {center_lat}, {center_lon}]
    actions:
      expire_callback: aircraft_remove_cb

  prox:
    conditions:
      min_alt: {los_min_alt}
      max_alt: {los_max_alt}
      regions: ["Vicinity"]
      proximity: [400, .3]
    actions:
      callback: los_update_cb
'''


def main():
    parser = argparse.ArgumentParser(
        description="Generate KML regions and YAML configs for airport monitoring.",
        epilog="Example: python3 src/tools/generate_airport_config.py KSQL --runway 30"
    )
    parser.add_argument('icao', help="ICAO airport code (e.g., KSQL, KOAK)")
    parser.add_argument('--runway', help="Runway end identifier (e.g., 12, 30L). Default: lower-numbered end of longest runway")
    parser.add_argument('--apprunway', help="Approach runway (if different from departure). Overrides --runway for approaches.")
    parser.add_argument('--deprunway', help="Departure runway (if different from approach). Overrides --runway for departures.")
    parser.add_argument('--force', action='store_true', help="Overwrite existing files")
    args = parser.parse_args()

    icao = args.icao.upper()
    icao_lower = icao.lower()

    # Load airport data
    print(f"Looking up {icao}...")
    airport = load_airport(icao)
    if not airport:
        print(f"Error: Airport '{icao}' not found in database.", file=sys.stderr)
        sys.exit(1)

    airport_name = airport.get('name', icao)
    try:
        field_elevation = float(airport.get('elevation_ft') or 0)
        center_lat = float(airport.get('latitude_deg') or 0)
        center_lon = float(airport.get('longitude_deg') or 0)
    except (ValueError, TypeError):
        print("Error: Could not parse airport coordinates.", file=sys.stderr)
        sys.exit(1)

    print(f"  {airport_name}")
    print(f"  Elevation: {field_elevation:.0f} ft MSL")
    print(f"  Location: {center_lat:.4f}, {center_lon:.4f}")

    # Load runways
    runways = load_runways(icao)
    if not runways:
        print(f"Error: No runways found for {icao}.", file=sys.stderr)
        sys.exit(1)

    print(f"  Runways: {len(runways)}")

    # Helper to normalize runway identifier (handle leading zeros: 1L -> 01L)
    def normalize_runway_ident(ident: str) -> str:
        ident = ident.upper()
        # Extract numeric part and suffix (L/R/C)
        num_part = ''.join(c for c in ident if c.isdigit())
        suffix = ''.join(c for c in ident if c.isalpha())
        if num_part:
            # Pad to 2 digits (runway numbers are 01-36)
            num_part = num_part.zfill(2)
        return num_part + suffix

    # Helper to find runway by identifier
    def find_runway_by_ident(ident: str) -> tuple[dict, str] | None:
        normalized = normalize_runway_ident(ident)
        for rwy in runways:
            le = rwy.get('le_ident', '').upper()
            he = rwy.get('he_ident', '').upper()
            if le == normalized:
                return rwy, le
            if he == normalized:
                return rwy, he
        return None

    # List available runways for error messages
    available_rwys = []
    for rwy in runways:
        available_rwys.extend([rwy.get('le_ident', ''), rwy.get('he_ident', '')])
    available_rwys = list(filter(None, available_rwys))

    # Determine approach runway
    if args.apprunway:
        result = find_runway_by_ident(args.apprunway)
        if not result:
            print(f"Error: Approach runway '{args.apprunway}' not found. Available: {', '.join(available_rwys)}", file=sys.stderr)
            sys.exit(1)
        app_runway, app_ident = result
    elif args.runway:
        result = find_runway_by_ident(args.runway)
        if not result:
            print(f"Error: Runway '{args.runway}' not found. Available: {', '.join(available_rwys)}", file=sys.stderr)
            sys.exit(1)
        app_runway, app_ident = result
    else:
        app_runway = get_longest_runway(runways)
        app_ident = get_lower_runway_end(app_runway)

    # Determine departure runway
    if args.deprunway:
        result = find_runway_by_ident(args.deprunway)
        if not result:
            print(f"Error: Departure runway '{args.deprunway}' not found. Available: {', '.join(available_rwys)}", file=sys.stderr)
            sys.exit(1)
        dep_runway, dep_ident = result
    elif args.runway:
        # Use opposite end of the specified runway for departure
        result = find_runway_by_ident(args.runway)
        dep_runway, _ = result
        dep_ident = get_opposite_runway_end(dep_runway, args.runway.upper())
    else:
        # Use opposite end of approach runway for departure
        dep_runway = app_runway
        dep_ident = get_opposite_runway_end(app_runway, app_ident)

    # Get runway end data
    app_end = get_runway_end_data(app_runway, app_ident)
    if not app_end:
        print(f"Error: Could not get data for approach runway {app_ident}.", file=sys.stderr)
        sys.exit(1)

    dep_end = get_runway_end_data(dep_runway, dep_ident)
    if not dep_end:
        print(f"Error: Could not get data for departure runway {dep_ident}.", file=sys.stderr)
        sys.exit(1)

    # For departure wedge position: aircraft departing runway XX lift off and fly over
    # the OPPOSITE end of that runway. So we need the opposite end's coordinates.
    dep_opposite_ident = get_opposite_runway_end(dep_runway, dep_ident)
    dep_liftoff_end = get_runway_end_data(dep_runway, dep_opposite_ident)
    if not dep_liftoff_end:
        print(f"Error: Could not get data for departure liftoff point {dep_opposite_ident}.", file=sys.stderr)
        sys.exit(1)

    runway_length_ft = float(app_runway.get('length_ft') or 5000)

    # Check if using split runway operations
    split_ops = (app_ident != get_opposite_runway_end(dep_runway, dep_ident))

    if split_ops:
        print(f"  Approach runway: {app_ident} (heading: {app_end['heading']:.0f}°)")
        print(f"  Departure runway: {dep_ident} (heading: {dep_end['heading']:.0f}°)")
    else:
        print(f"  Using runway: {app_ident}/{dep_ident} (length: {runway_length_ft:.0f} ft)")
        print(f"  Approach end: {app_end['lat']:.4f}, {app_end['lon']:.4f}")
        print(f"  Departure end: {dep_end['lat']:.4f}, {dep_end['lon']:.4f}")
        print(f"  Heading: {app_end['heading']:.0f}°")

    # Output paths - go up to project root (src/tools -> src -> project root)
    output_dir = Path(__file__).parent.parent.parent / "examples" / "generated"
    output_dir.mkdir(parents=True, exist_ok=True)

    kml_path = output_dir / f"{icao_lower}_regions.kml"
    rules_path = output_dir / f"{icao_lower}_rules.yaml"
    stripview_path = output_dir / f"{icao_lower}_stripview.yaml"

    # Check for existing files
    if not args.force:
        existing = [p for p in [kml_path, rules_path, stripview_path] if p.exists()]
        if existing:
            print(f"\nError: Files already exist: {', '.join(str(p) for p in existing)}", file=sys.stderr)
            print("Use --force to overwrite.", file=sys.stderr)
            sys.exit(1)

    # Generate files
    print("\nGenerating files...")

    kml_content = generate_kml(icao, airport_name, field_elevation,
                               app_end, dep_end, dep_liftoff_end, runway_length_ft,
                               center_lat, center_lon)
    with open(kml_path, 'w', encoding='utf-8') as f:
        f.write(kml_content)
    print(f"  {kml_path}")

    rules_content = generate_rules_yaml(icao, field_elevation, app_ident, dep_ident,
                                        center_lat, center_lon)
    with open(rules_path, 'w', encoding='utf-8') as f:
        f.write(rules_content)
    print(f"  {rules_path}")

    stripview_content = generate_stripview_yaml(icao, field_elevation, app_ident, dep_ident,
                                                center_lat, center_lon)
    with open(stripview_path, 'w', encoding='utf-8') as f:
        f.write(stripview_content)
    print(f"  {stripview_path}")

    # Print usage commands
    print(f"""
================================================================================
Generated files for {icao} ({airport_name}, elev {field_elevation:.0f}ft)
================================================================================

VIEW IN GOOGLE EARTH:
  Open examples/generated/{icao_lower}_regions.kml to verify regions look correct

RUN STRIPVIEW (no hardware required - uses airplanes.live API):
  source .venv/bin/activate && python3 src/applications/stripview/controller.py \\
    -- --api --rules examples/generated/{icao_lower}_stripview.yaml

RUN STRIPVIEW (with local readsb receiver):
  source .venv/bin/activate && python3 src/applications/stripview/controller.py \\
    -- --ipaddr localhost --port 30006 --rules examples/generated/{icao_lower}_stripview.yaml

RUN AIRPORT MONITOR / LOS DETECTION (no hardware required - uses airplanes.live API):
  source .venv/bin/activate && python3 src/applications/tcp_api_monitor/monitor.py \\
    examples/generated/{icao_lower}_rules.yaml

RUN AIRPORT MONITOR / LOS DETECTION (with local readsb receiver):
  source .venv/bin/activate && python3 src/applications/airport_monitor/main.py \\
    --ipaddr localhost --port 30006 --rules examples/generated/{icao_lower}_rules.yaml
""")


if __name__ == '__main__':
    main()

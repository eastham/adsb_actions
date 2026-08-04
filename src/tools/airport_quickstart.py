#!/usr/bin/env python3
"""
Quick-start wrapper for airport monitoring.

Fetches current METAR, selects wind-favored runway, generates config,
and launches the stripview GUI with live ADS-B data plus a web view.

Usage:
    python3 src/tools/airport_quickstart.py KPAO
    python3 src/tools/airport_quickstart.py KPAO --runway 13
    python3 src/tools/airport_quickstart.py KOAK --apprunway 28L --deprunway 30
"""

import argparse
import math
import re
import subprocess
import sys
import urllib.request
import webbrowser
from pathlib import Path

# Import shared functions from generate_airport_config
from generate_airport_config import (
    load_airport,
    load_runways,
    get_longest_runway,
    get_lower_runway_end,
)

# METAR API endpoint (aviationweather.gov - free, no key required)
METAR_API_URL = "https://aviationweather.gov/api/data/metar?ids={icao}"


def check_gui_prerequisites() -> None:
    """Check if Kivy and KivyMD are installed. Exit with helpful message if not."""
    missing = []

    try:
        import kivy  # noqa: F401
    except ImportError:
        missing.append('Kivy')

    try:
        import kivymd  # noqa: F401
    except ImportError:
        missing.append('KivyMD')

    if missing:
        print("Error: GUI prerequisites not installed.", file=sys.stderr)
        print(f"Missing: {', '.join(missing)}", file=sys.stderr)
        print("\nInstall with:", file=sys.stderr)
        print("  pip install -e '.[gui]'", file=sys.stderr)
        print("\nIf that fails, try installing directly:", file=sys.stderr)
        print("  pip install Kivy==2.3.0 kivymd==1.1.1", file=sys.stderr)
        sys.exit(1)


def fetch_metar(icao: str) -> str | None:
    """Fetch METAR from aviationweather.gov. Returns raw METAR text or None."""
    url = METAR_API_URL.format(icao=icao.upper())

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            text = response.read().decode('utf-8').strip()
            # API returns raw METAR text (or empty if no data)
            if text and text.startswith('METAR') or text.startswith(icao.upper()):
                return text
            elif text:
                # Some responses include "METAR" prefix, some don't
                return text
    except Exception as e:
        print(f"  Warning: Could not fetch METAR: {e}", file=sys.stderr)

    return None


def parse_wind_from_metar(metar_text: str) -> tuple[int | None, int]:
    """
    Parse wind direction and speed from METAR.

    Returns (wind_direction_deg, wind_speed_kt) or (None, speed) for calm/variable.

    Examples:
        "31008KT" -> (310, 8)
        "VRB05KT" -> (None, 5)
        "00000KT" -> (None, 0)
    """
    # Match wind pattern: 3-digit direction + 2-3 digit speed + optional gust + KT
    match = re.search(r'(\d{3}|VRB)(\d{2,3})(?:G\d{2,3})?KT', metar_text)
    if not match:
        return (None, 0)

    direction = match.group(1)
    speed = int(match.group(2))

    if direction == 'VRB':
        return (None, speed)

    direction_deg = int(direction)
    if direction_deg == 0 and speed == 0:
        return (None, 0)  # Calm

    return (direction_deg, speed)


def select_wind_favored_runway(wind_direction: int, runways: list[dict]) -> tuple[dict, str] | None:
    """
    Select the runway end that is most aligned INTO the wind.

    Aircraft land and take off into the wind, so we want the runway
    with heading closest to the wind direction.

    Returns (runway_dict, runway_end_ident) or None if no valid runway found.
    """
    if wind_direction is None:
        return None

    best_runway = None
    best_ident = None
    best_headwind = -float('inf')

    for runway in runways:
        for prefix in ['le_', 'he_']:
            ident = runway.get(f'{prefix}ident', '')
            heading_str = runway.get(f'{prefix}heading_degT', '')

            if not ident or not heading_str:
                continue

            try:
                runway_heading = float(heading_str)
            except ValueError:
                continue

            # Calculate headwind component
            # Best case: wind direction equals runway heading (full headwind)
            angle_diff = abs(wind_direction - runway_heading)
            if angle_diff > 180:
                angle_diff = 360 - angle_diff

            # Headwind component (1.0 = perfect headwind, -1.0 = tailwind)
            headwind = math.cos(math.radians(angle_diff))

            if headwind > best_headwind:
                best_headwind = headwind
                best_runway = runway
                best_ident = ident

    if best_runway:
        return (best_runway, best_ident)
    return None


def launch_stripview_and_browser(icao: str, lat: float, lon: float) -> None:
    """Launch stripview as subprocess and open globe.airplanes.live in browser."""
    icao_lower = icao.lower()
    yaml_path = f"examples/generated/{icao_lower}_stripview.yaml"

    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent

    # Build the stripview command
    stripview_cmd = [
        sys.executable,
        str(project_root / "src/applications/stripview/controller.py"),
        "--",
        "--api",
        "--rules", yaml_path
    ]

    print(f"\nLaunching stripview...")
    print(f"  {' '.join(stripview_cmd)}")

    # Launch stripview as a subprocess
    process = subprocess.Popen(
        stripview_cmd,
        cwd=str(project_root),
    )

    # Open browser to globe.airplanes.live centered on airport
    # Use new=0 to try to reuse existing window/tab rather than opening new ones
    globe_url = f"https://globe.airplanes.live/?lat={lat:.4f}&lon={lon:.4f}&zoom=12"
    print(f"\nOpening browser: {globe_url}")
    webbrowser.open(globe_url, new=0)

    print(f"\nStripview running (PID: {process.pid})")
    print("Close the stripview window to exit.")

    # Wait for stripview to finish
    try:
        process.wait()
    except KeyboardInterrupt:
        print("\nInterrupted, closing stripview...")
        process.terminate()
        process.wait()


def main():
    parser = argparse.ArgumentParser(
        description="Quick-start airport monitoring with wind-favored runway selection.",
        epilog="Example: python3 src/tools/airport_quickstart.py KPAO"
    )
    parser.add_argument('icao', help="ICAO airport code (e.g., KPAO, EGLL)")
    parser.add_argument('--runway', help="Override automatic runway selection (e.g., 31, 09L)")
    parser.add_argument('--apprunway', help="Approach runway (if different from departure)")
    parser.add_argument('--deprunway', help="Departure runway (if different from approach)")
    args = parser.parse_args()

    icao = args.icao.upper()

    # 1. Check GUI prerequisites
    print("Checking GUI prerequisites...")
    check_gui_prerequisites()
    print("  OK")

    # 2. Load airport data
    print(f"\nLooking up {icao}...")
    airport = load_airport(icao)
    if not airport:
        print(f"Error: Airport '{icao}' not found.", file=sys.stderr)
        sys.exit(1)

    airport_name = airport.get('name', icao)
    try:
        center_lat = float(airport.get('latitude_deg') or 0)
        center_lon = float(airport.get('longitude_deg') or 0)
    except (ValueError, TypeError):
        print("Error: Could not parse airport coordinates.", file=sys.stderr)
        sys.exit(1)

    print(f"  {airport_name}")
    print(f"  Location: {center_lat:.4f}, {center_lon:.4f}")

    # 3. Load runways
    runways = load_runways(icao)
    if not runways:
        print(f"Error: No runways found for {icao}.", file=sys.stderr)
        sys.exit(1)

    # List available runways
    available_rwys = []
    for rwy in runways:
        le = rwy.get('le_ident', '')
        he = rwy.get('he_ident', '')
        if le:
            available_rwys.append(le)
        if he:
            available_rwys.append(he)
    print(f"  Available runways: {', '.join(available_rwys)}")

    # 4. Determine runways to use
    app_runway = args.apprunway.upper() if args.apprunway else None
    dep_runway = args.deprunway.upper() if args.deprunway else None

    # If specific app/dep runways given, skip METAR-based selection
    if app_runway or dep_runway or args.runway:
        if args.runway and not app_runway and not dep_runway:
            # Single runway override
            runway_ident = args.runway.upper()
            print(f"\n  Using specified runway: {runway_ident}")
        else:
            # Split runway operations or partial override
            if app_runway:
                print(f"\n  Approach runway: {app_runway}")
            if dep_runway:
                print(f"  Departure runway: {dep_runway}")
            runway_ident = None  # Will use --apprunway/--deprunway directly
    else:
        # Fetch METAR and select wind-favored runway
        print(f"\nFetching METAR for {icao}...")
        metar = fetch_metar(icao)

        runway_ident = None
        if metar:
            print(f"  {metar}")
            wind_dir, wind_speed = parse_wind_from_metar(metar)

            if wind_dir is not None:
                print(f"  Wind: {wind_dir:03d}° at {wind_speed} kt")
                result = select_wind_favored_runway(wind_dir, runways)
                if result:
                    _, runway_ident = result
                    print(f"  Wind-favored runway: {runway_ident}")
            elif wind_speed > 0:
                print(f"  Wind: Variable at {wind_speed} kt (using default runway)")
            else:
                print("  Wind: Calm (using default runway)")
        else:
            print("  No METAR available (using default runway)")

        # Fall back to default: longest runway, lower-numbered end
        if not runway_ident:
            longest = get_longest_runway(runways)
            runway_ident = get_lower_runway_end(longest)
            print(f"  Default runway: {runway_ident}")

    # 5. Generate config files
    print(f"\nGenerating config files...")
    project_root = Path(__file__).parent.parent.parent

    gen_cmd = [
        sys.executable,
        str(project_root / "src/tools/generate_airport_config.py"),
        icao,
        "--force"
    ]

    # Add runway arguments
    if app_runway:
        gen_cmd.extend(["--apprunway", app_runway])
    if dep_runway:
        gen_cmd.extend(["--deprunway", dep_runway])
    if runway_ident and not app_runway and not dep_runway:
        gen_cmd.extend(["--runway", runway_ident])

    result = subprocess.run(gen_cmd, cwd=str(project_root))
    if result.returncode != 0:
        print("Error: Config generation failed.", file=sys.stderr)
        sys.exit(1)

    # 6. Launch stripview and browser
    launch_stripview_and_browser(icao, center_lat, center_lon)


if __name__ == '__main__':
    main()

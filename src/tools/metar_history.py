"""Fetch historical METAR data from Iowa Environmental Mesonet (IEM) and
classify flight categories (VFR/MVFR/IFR/LIFR) per hour.

IEM API docs: https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?help=
"""

import csv
import io
import logging
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

IEM_BASE_URL = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
)


def icao_to_faa_lid(icao: str) -> str:
    """Convert ICAO code to FAA LID for IEM queries.

    US CONUS airports starting with 'K' get the K stripped (KWVI -> WVI).
    All others pass through as-is (E16, PHNL, etc.).
    """
    if len(icao) == 4 and icao.startswith("K"):
        return icao[1:]
    return icao


def fetch_metar_history(icao: str, year: int = 2025,
                        cache_dir: Path | None = None) -> str | None:
    """Fetch a full year of hourly METAR observations from IEM.

    Returns CSV text, or None if the station has no data.
    Caches to {cache_dir}/{ICAO}_metar_{year}.csv if cache_dir is provided.
    """
    if cache_dir:
        cache_file = cache_dir / f"{icao}_metar_{year}.csv"
        if cache_file.exists() and cache_file.stat().st_size > 100:
            logger.info(f"Using cached METAR data: {cache_file}")
            return cache_file.read_text()

    faa_lid = icao_to_faa_lid(icao)
    params = (
        f"?station={faa_lid}"
        f"&data=vsby&data=skyc1&data=skyc2&data=skyc3"
        f"&data=skyl1&data=skyl2&data=skyl3"
        f"&tz=UTC&format=onlycomma"
        f"&sts={year}-01-01T00:00:00Z"
        f"&ets={year + 1}-01-01T00:00:00Z"
    )
    url = IEM_BASE_URL + params

    try:
        logger.info(f"Fetching METAR history for {icao} ({faa_lid}) year {year}")
        with urllib.request.urlopen(url, timeout=30) as response:
            text = response.read().decode("utf-8")
    except Exception as e:
        logger.warning(f"Could not fetch METAR history for {icao}: {e}")
        return None

    # Check if we got any data rows (not just the header)
    lines = text.strip().split("\n")
    if len(lines) <= 1:
        logger.warning(f"No METAR data returned for {icao} ({faa_lid})")
        return None

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"{icao}_metar_{year}.csv"
        cache_file.write_text(text)
        logger.info(f"Cached METAR data to {cache_file} ({len(lines) - 1} rows)")

    return text


def _get_ceiling(row: dict) -> float | None:
    """Extract ceiling (lowest broken or overcast layer) from a METAR row.

    Returns ceiling in feet, or None if sky is clear / no ceiling.
    """
    for i in range(1, 4):
        cover = row.get(f"skyc{i}", "").strip()
        height_str = row.get(f"skyl{i}", "").strip()
        if cover in ("BKN", "OVC", "VV") and height_str and height_str != "M":
            try:
                return float(height_str)
            except ValueError:
                continue
    return None


def classify_flight_category(visibility: float | None,
                             ceiling: float | None) -> str:
    """Classify VFR/MVFR/IFR/LIFR from visibility (sm) and ceiling (ft AGL).

    Standard FAA categories:
      LIFR: vis < 1 OR ceiling < 500
      IFR:  vis 1-3 OR ceiling 500-1000
      MVFR: vis 3-5 OR ceiling 1000-3000
      VFR:  vis > 5 AND ceiling > 3000 (or no ceiling)
    """
    if visibility is None and ceiling is None:
        return "UNKNOWN"

    cat = "VFR"

    if ceiling is not None:
        if ceiling < 500:
            cat = "LIFR"
        elif ceiling < 1000:
            cat = "IFR"
        elif ceiling < 3000:
            cat = "MVFR"

    if visibility is not None:
        if visibility < 1:
            vis_cat = "LIFR"
        elif visibility < 3:
            vis_cat = "IFR"
        elif visibility <= 5:
            vis_cat = "MVFR"
        else:
            vis_cat = "VFR"
        # Use the worse of ceiling-based and visibility-based
        rank = {"LIFR": 0, "IFR": 1, "MVFR": 2, "VFR": 3}
        if rank.get(vis_cat, 3) < rank.get(cat, 3):
            cat = vis_cat

    return cat


def parse_metar_csv(csv_text: str) -> dict[tuple[str, int], str]:
    """Parse IEM CSV into a mapping of (date_str, hour) -> flight_category.

    Takes the last observation per hour if multiple exist (SPECI reports).
    date_str is in 'YYYY-MM-DD' format, hour is 0-23 (UTC).
    """
    result: dict[tuple[str, int], str] = {}

    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        valid = row.get("valid", "").strip()
        if not valid:
            continue

        # Parse timestamp: "2025-06-01 00:15"
        parts = valid.split()
        if len(parts) < 2:
            continue
        date_str = parts[0]
        try:
            hour = int(parts[1].split(":")[0])
        except (ValueError, IndexError):
            continue

        # Parse visibility
        vis_str = row.get("vsby", "").strip()
        visibility = None
        if vis_str and vis_str != "M":
            try:
                visibility = float(vis_str)
            except ValueError:
                pass

        ceiling = _get_ceiling(row)
        category = classify_flight_category(visibility, ceiling)

        if category != "UNKNOWN":
            # Last observation per hour wins
            result[(date_str, hour)] = category

    return result


def get_flight_categories(icao: str, year: int = 2025,
                          cache_dir: Path | None = None
                          ) -> dict[tuple[str, int], str] | None:
    """High-level: fetch + parse + classify. Returns mapping or None."""
    csv_text = fetch_metar_history(icao, year, cache_dir)
    if csv_text is None:
        return None
    return parse_metar_csv(csv_text)

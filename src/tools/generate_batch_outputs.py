"""Generate batch pipeline output summary and HTML index page.

Collects LOS statistics from generated per-airport files, prints a console
summary, and generates a modern HTML landing page with collapsible sections
grouped by the airport list file's categories.
"""

import json
import math
import os
import re
from html import escape
from pathlib import Path

import generate_airport_config
from batch_helpers import faa_to_icao

NM_PER_DEG_LAT = 60.0
NEAR_AIRPORT_RADIUS_NM = 5


def _fast_distance_nm(lat1, lon1, lat2, lon2):
    """Fast approximate distance in nautical miles (equirectangular)."""
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    avg_lat = (lat1 + lat2) / 2.0
    dlon_adjusted = dlon * math.cos(math.radians(avg_lat))
    dist_deg = math.sqrt(dlat * dlat + dlon_adjusted * dlon_adjusted)
    return dist_deg * NM_PER_DEG_LAT


def _parse_event_fields(line):
    """Extract (lat, lon, quality) from a CSV event line, or None on failure.

    CSV fields after marker: timestamp,datestr,altdatestr,lat,lon,alt,
    tail1,tail2,quality,...
    """
    marker = "CSV OUTPUT FOR POSTPROCESSING: "
    idx = line.find(marker)
    if idx < 0:
        return None
    csv_part = line[idx + len(marker):]
    fields = csv_part.split(',')
    if len(fields) < 9:
        return None
    try:
        return float(fields[3]), float(fields[4]), fields[8]
    except (ValueError, IndexError):
        return None


def _get_airport_name_region(icao):
    """Look up airport name and iso_region from OurAirports data."""
    airport = generate_airport_config.load_airport(icao)
    if not airport and icao.startswith('K') and len(icao) == 4:
        airport = generate_airport_config.load_airport(icao[1:])
    if not airport:
        return icao, ''
    name = airport.get('name', icao)
    region = airport.get('iso_region', '')
    # iso_region is like "US-CA" — extract state code
    if region and '-' in region:
        region = region.split('-', 1)[1]
    return name, region


def parse_airport_sections(filepath):
    """Parse airport list file preserving section headers and commented-out entries.

    Returns list of (section_title, [icao_codes]) tuples.
    Lines starting with '# ' are section headers.
    Lines starting with '#' followed by a code (like #WVI) are
    commented-out airports — included in the section.
    """
    sections = []
    current_title = None
    current_codes = []

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Section header: "# Some Title"
            if line.startswith('#') and len(line) > 1 and line[1] == ' ':
                # Save previous section
                if current_title is not None:
                    sections.append((current_title, current_codes))
                current_title = line[2:].strip()
                current_codes = []
                continue

            # Commented-out airport code: "#WVI"
            if line.startswith('#'):
                code_part = line[1:].strip()
                match = re.match(r'[A-Za-z0-9]{2,4}', code_part)
                if match:
                    current_codes.append(faa_to_icao(match.group(0)))
                continue

            # Regular airport code
            match = re.match(r'[A-Za-z0-9]{2,4}', line)
            if match:
                if current_title is None:
                    current_title = "Airports"
                current_codes.append(faa_to_icao(match.group(0)))

    # Save last section
    if current_title is not None and current_codes:
        sections.append((current_title, current_codes))

    return sections


def collect_output_stats(icao_codes, base_dir, airport_info=None):
    """Collect output statistics from generated per-airport files.

    When airport_info is provided, only counts events within
    NEAR_AIRPORT_RADIUS_NM of the airport. airport_info maps
    ICAO -> (lat, lon, field_elev).

    Returns dict mapping ICAO to {html_path, num_events, quality_data,
    airport_name, airport_state}.
    """
    output_stats = {}
    for icao in icao_codes:
        airport_dir = base_dir / icao
        html_path = airport_dir / f"{icao}_map.html"
        combined_csv = airport_dir / f"{icao}_combined.csv.out"
        quality_json = airport_dir / f"{icao}_quality.json"

        if html_path.exists():
            num_events = 0
            if combined_csv.exists():
                apt_lat, apt_lon = None, None
                if airport_info and icao in airport_info:
                    apt_lat, apt_lon, _ = airport_info[icao]

                with open(combined_csv, 'r') as f:
                    for line in f:
                        if line.startswith('#'):
                            continue
                        result = _parse_event_fields(line)
                        if result is None:
                            continue
                        evt_lat, evt_lon, evt_quality = result
                        # Skip low-quality events
                        if evt_quality == 'low':
                            continue
                        # Filter by distance if airport location known
                        if apt_lat is not None:
                            if _fast_distance_nm(apt_lat, apt_lon,
                                                 evt_lat, evt_lon) > NEAR_AIRPORT_RADIUS_NM:
                                continue
                        num_events += 1

            quality_data = None
            if quality_json.exists():
                try:
                    quality_data = json.loads(quality_json.read_text())
                except Exception:
                    pass

            name, state = _get_airport_name_region(icao)

            output_stats[icao] = {
                'html_path': html_path,
                'num_events': num_events,
                'quality_data': quality_data,
                'airport_name': name,
                'airport_state': state,
            }

    return output_stats


def print_visualization_summary(output_stats: dict):
    """Print summary of generated HTML visualizations with stats."""
    if not output_stats:
        return

    print("\n" + "=" * 80)
    print("GENERATED VISUALIZATIONS")
    print("=" * 80)

    for icao in sorted(output_stats.keys()):
        stats = output_stats[icao]
        html_path = stats['html_path']
        num_events = stats['num_events']
        quality_data = stats.get('quality_data')

        name = stats.get('airport_name', icao)
        state = stats.get('airport_state', '')
        location_str = f" ({name}, {state})" if state else f" ({name})"

        print(f"\n{icao}{location_str}: {html_path}")
        print(f"  LOS Events (within {NEAR_AIRPORT_RADIUS_NM}nm): {num_events}",
              end="")

        if quality_data:
            score = quality_data.get('score', 'N/A')

            if score == 'green':
                score_display = '🟢 Green (Excellent)'
            elif score == 'yellow':
                score_display = '🟡 Yellow (Good)'
            elif score == 'red':
                score_display = '🔴 Red (Poor)'
            else:
                score_display = score

            print(f"  Data Quality: {score_display}")
        else:
            print(f"  Data Quality: No quality data available")

    print("\n" + "=" * 80)


def _quality_badge(score):
    """Return HTML badge for a quality score."""
    colors = {
        'green': ('#059669', '#d1fae5', 'Excellent'),
        'yellow': ('#d97706', '#fef3c7', 'Good'),
        'red': ('#dc2626', '#fee2e2', 'Poor'),
    }
    fg, bg, label = colors.get(score, ('#6b7280', '#f3f4f6', str(score)))
    return (f'<span style="display:inline-block;padding:2px 10px;'
            f'border-radius:12px;font-size:0.85em;font-weight:600;'
            f'color:{fg};background:{bg}">{escape(label)}</span>')


def _airport_row(icao, stats):
    """Build one HTML table row for an airport."""
    if stats is None:
        return (f'<tr class="no-data"><td>{escape(icao)}</td>'
                f'<td colspan="4" style="color:#9ca3af;font-style:italic">'
                f'No data available</td></tr>')

    num_events = stats['num_events']
    link = f'{icao}/{icao}_map.html'
    quality_data = stats.get('quality_data')
    name = stats.get('airport_name', icao)
    state = stats.get('airport_state', '')

    quality_html = '—'
    quality_sort = 0
    if quality_data:
        score = quality_data.get('score', '')
        quality_html = _quality_badge(score)
        quality_sort = {'green': 3, 'yellow': 2, 'red': 1}.get(score, 0)

    return (f'<tr>'
            f'<td><a href="{escape(link)}" target="_blank">{escape(icao)}</a></td>'
            f'<td>{escape(name)}</td>'
            f'<td>{escape(state)}</td>'
            f'<td data-sort="{num_events}"'
            f' style="text-align:right;font-variant-numeric:tabular-nums">'
            f'{num_events}</td>'
            f'<td data-sort="{quality_sort}"'
            f' style="text-align:center">{quality_html}</td>'
            f'</tr>')


def generate_index_html(sections, output_stats, output_path):
    """Generate a modern HTML landing page with collapsible airport sections.

    sections: list of (title, [icao_codes]) from parse_airport_sections
    output_stats: dict from collect_output_stats
    output_path: Path to write index.html
    """
    section_blocks = []
    for idx, (title, icao_codes) in enumerate(sections):
        # Sort airports by num_events descending, no-data airports last
        def sort_key(icao):
            s = output_stats.get(icao)
            if s is None:
                return (-1, icao)
            return (s['num_events'], icao)

        sorted_codes = sorted(icao_codes, key=sort_key, reverse=True)

        rows = []
        for icao in sorted_codes:
            stats = output_stats.get(icao)
            rows.append(_airport_row(icao, stats))

        # Count airports with data
        with_data = sum(1 for c in icao_codes if c in output_stats)

        section_html = f"""
    <div class="section">
      <button class="section-toggle" onclick="toggleSection('section-{idx}')">
        <span class="toggle-icon" id="icon-section-{idx}">&#9654;</span>
        <span class="section-title">{escape(title)}</span>
        <span class="section-count">{with_data} of {len(icao_codes)} airports</span>
      </button>
      <div class="section-content" id="section-{idx}" style="display:none">
        <table class="sortable">
          <thead>
            <tr>
              <th>Airport</th>
              <th>Name</th>
              <th>State</th>
              <th style="text-align:right" title="Aircraft within .3nm and 400ft vertically, within 5nm of the airport, &#39;medium&#39; event quality or better">LOS Events</th>
              <th style="text-align:center">Data Quality</th>
            </tr>
          </thead>
          <tbody>
            {"".join(rows)}
          </tbody>
        </table>
      </div>
    </div>"""
        section_blocks.append(section_html)

    # Count totals
    total_airports = sum(len(codes) for _, codes in sections)
    total_events = sum(s['num_events'] for s in output_stats.values())
    airports_with_data = len(output_stats)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ADS-B Loss of Separation Analysis</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                 "Helvetica Neue", Arial, sans-serif;
    background: #f8fafc; color: #1e293b; line-height: 1.6;
  }
  .container { max-width: 960px; margin: 0 auto; padding: 40px 24px; }
  h1 {
    font-size: 2rem; font-weight: 700; color: #0f172a;
    margin-bottom: 4px;
  }
  .tagline {
    font-size: 1.1rem; color: #64748b; margin-bottom: 20px;
  }
  .description {
    color: #475569; margin-bottom: 32px; max-width: 720px;
  }
  .stats-bar {
    display: flex; gap: 32px; margin-bottom: 32px;
    padding: 16px 24px; background: #fff;
    border: 1px solid #e2e8f0; border-radius: 10px;
  }
  .stat-item { text-align: center; }
  .stat-value {
    font-size: 1.6rem; font-weight: 700; color: #0f172a;
    font-variant-numeric: tabular-nums;
  }
  .stat-label { font-size: 0.8rem; color: #94a3b8; text-transform: uppercase;
                letter-spacing: 0.05em; }
  .section { margin-bottom: 12px; }
  .section-toggle {
    width: 100%; display: flex; align-items: center; gap: 12px;
    padding: 14px 20px; background: #fff; border: 1px solid #e2e8f0;
    border-radius: 10px; cursor: pointer; font-size: 1rem;
    transition: background 0.15s, box-shadow 0.15s;
    text-align: left;
  }
  .section-toggle:hover { background: #f1f5f9;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06); }
  .toggle-icon {
    font-size: 0.75rem; color: #94a3b8; transition: transform 0.2s;
    display: inline-block; width: 16px;
  }
  .toggle-icon.open { transform: rotate(90deg); }
  .section-title { font-weight: 600; color: #1e293b; flex: 1; }
  .section-count {
    font-size: 0.85rem; color: #94a3b8; white-space: nowrap;
  }
  .section-content {
    margin-top: 4px; padding: 0 4px;
    animation: slideDown 0.2s ease-out;
  }
  @keyframes slideDown {
    from { opacity: 0; transform: translateY(-4px); }
    to   { opacity: 1; transform: translateY(0); }
  }
  table {
    width: 100%; border-collapse: collapse; background: #fff;
    border: 1px solid #e2e8f0; border-radius: 8px;
    overflow: hidden;
  }
  thead th {
    padding: 10px 16px; font-size: 0.8rem; font-weight: 600;
    color: #64748b; text-transform: uppercase; letter-spacing: 0.04em;
    background: #f8fafc; border-bottom: 1px solid #e2e8f0;
  }
  tbody td {
    padding: 10px 16px; border-bottom: 1px solid #f1f5f9;
    font-size: 0.95rem;
  }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: #f8fafc; }
  tbody tr.no-data:hover { background: #fff; }
  a { color: #2563eb; text-decoration: none; font-weight: 500; }
  a:hover { text-decoration: underline; }
  .footer {
    margin-top: 48px; padding-top: 20px; border-top: 1px solid #e2e8f0;
    font-size: 0.85rem; color: #94a3b8;
  }
  /* Search box */
  .search-bar {
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 24px; padding: 12px 20px;
    background: #fff; border: 1px solid #e2e8f0; border-radius: 10px;
  }
  .search-bar label {
    font-size: 0.9rem; font-weight: 600; color: #475569;
    white-space: nowrap;
  }
  .search-bar input {
    width: 120px; padding: 6px 12px; font-size: 1rem;
    border: 1px solid #cbd5e1; border-radius: 6px;
    text-transform: uppercase;
  }
  .search-bar input:focus { outline: none; border-color: #2563eb; }
  .search-bar button {
    padding: 6px 16px; font-size: 0.9rem; font-weight: 600;
    color: #fff; background: #2563eb; border: none; border-radius: 6px;
    cursor: pointer;
  }
  .search-bar button:hover { background: #1d4ed8; }
  /* Sortable table headers */
  table.sortable thead th {
    cursor: pointer; user-select: none;
  }
  table.sortable thead th:after {
    content: " \\25B4\\25BE"; font-size: 0.6em; color: #c0c8d0;
    margin-left: 4px;
  }
  table.sortable thead th[aria-sort="ascending"]:after {
    content: " \\25B4"; color: #2563eb;
  }
  table.sortable thead th[aria-sort="descending"]:after {
    content: " \\25BE"; color: #2563eb;
  }
  /* Tooltip for th[title] */
  thead th[title] {
    text-decoration: underline dotted #94a3b8;
    text-underline-offset: 3px;
  }
</style>
<script src="https://cdn.jsdelivr.net/gh/tofsjonas/sortable@3.2.3/sortable.min.js"></script>
</head>
<body>
<div class="container">
  <h1>ADS-B Loss of Separation Analysis</h1>
  <p class="tagline">Automated detection of aircraft proximity events at US airports</p>
  <p class="description">
    This dashboard summarizes Loss of Separation (LOS) events detected from
    ADS-B surveillance data. Each airport link below opens a detailed map
    showing individual events, traffic patterns, and data quality metrics.
  </p>

  <div class="stats-bar">
    <div class="stat-item">
      <div class="stat-value">""" + str(airports_with_data) + """</div>
      <div class="stat-label">Airports Analyzed</div>
    </div>
    <div class="stat-item">
      <div class="stat-value">""" + str(total_events) + """</div>
      <div class="stat-label">LOS Events</div>
    </div>
    <div class="stat-item">
      <div class="stat-value">7.2 Billion</div>
      <div class="stat-label">Crowdsourced data points analyzed</div>
    </div>
  </div>

  <div class="search-bar">
    <label for="airport-search">Airport Identifier:</label>
    <input type="text" id="airport-search" maxlength="4"
           placeholder="e.g. WVI" autocomplete="off">
    <button onclick="searchAirport()">Go</button>
  </div>

""" + "\n".join(section_blocks) + """

  <div class="footer">
    Generated by adsb-actions batch pipeline
  </div>
</div>
<script>
function toggleSection(id) {
  var el = document.getElementById(id);
  var icon = document.getElementById('icon-' + id);
  if (el.style.display === 'none') {
    el.style.display = 'block';
    icon.classList.add('open');
  } else {
    el.style.display = 'none';
    icon.classList.remove('open');
  }
}

function searchAirport() {
  var raw = document.getElementById('airport-search').value.trim().toUpperCase();
  if (!raw) return;
  // Normalize to ICAO: prepend K for 3-letter US identifiers
  var icao = raw;
  if (/^[A-Z]{3}$/.test(raw) || /^[A-Z][0-9][A-Z0-9]$/.test(raw)) {
    icao = 'K' + raw;
  }
  var url = icao + '/' + icao + '_map.html';
  fetch(url, {method: 'HEAD'}).then(function(resp) {
    if (resp.ok) {
      window.open(url, '_blank');
    } else {
      window.location.href = 'unavailable.html?id=' + encodeURIComponent(icao);
    }
  }).catch(function() {
    window.location.href = 'unavailable.html?id=' + encodeURIComponent(icao);
  });
}

// Allow Enter key in search box
document.getElementById('airport-search').addEventListener('keydown', function(e) {
  if (e.key === 'Enter') searchAirport();
});
</script>
</body>
</html>
"""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    print(f"\nIndex page written to {output_path}")

    # Generate the companion unavailable.html in the same directory
    _generate_unavailable_html(output_path.parent / "unavailable.html")


def _generate_unavailable_html(output_path):
    """Generate unavailable.html — shown when a searched airport has no data.

    Uses Netlify Forms for the "Request Analysis" submission.  Netlify's
    build bot detects the <form data-netlify="true"> at deploy time and
    wires up serverless form handling automatically.  Submissions appear
    in the Netlify dashboard under Forms > airport-request.
    """
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Airport Not Available</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                 "Helvetica Neue", Arial, sans-serif;
    background: #f8fafc; color: #1e293b; line-height: 1.6;
  }
  .container {
    max-width: 560px; margin: 80px auto; padding: 40px 32px;
    background: #fff; border: 1px solid #e2e8f0; border-radius: 12px;
    text-align: center;
  }
  h1 { font-size: 1.5rem; color: #0f172a; margin-bottom: 8px; }
  .icao { font-size: 2rem; font-weight: 700; color: #2563eb; margin: 12px 0; }
  p { color: #475569; margin-bottom: 20px; }
  .btn {
    display: inline-block; padding: 10px 28px; font-size: 1rem;
    font-weight: 600; color: #fff; background: #2563eb; border: none;
    border-radius: 8px; cursor: pointer; text-decoration: none;
  }
  .btn:hover { background: #1d4ed8; }
  .btn-secondary {
    background: #f1f5f9; color: #475569; margin-left: 12px;
  }
  .btn-secondary:hover { background: #e2e8f0; }
  .msg {
    margin-top: 16px; padding: 10px 16px; border-radius: 8px;
    font-size: 0.9rem; display: none;
  }
  .msg.success { display: block; background: #d1fae5; color: #059669; }
  .msg.error { display: block; background: #fee2e2; color: #dc2626; }
</style>
</head>
<body>
<div class="container">
  <h1>Airport Not Available</h1>
  <div class="icao" id="airport-id"></div>
  <p>Analysis data for this airport is not currently available.</p>

  <!-- Netlify detects this form at deploy time (data-netlify="true") -->
  <form name="airport-request" method="POST" data-netlify="true"
        style="display:inline" id="request-form">
    <input type="hidden" name="airport" id="airport-field">
    <button type="submit" class="btn" id="request-btn">
      Request Analysis
    </button>
  </form>
  <a class="btn btn-secondary" href="index.html">Back</a>
  <div class="msg" id="msg"></div>
</div>
<script>
var params = new URLSearchParams(window.location.search);
var airportId = (params.get('id') || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
document.getElementById('airport-id').textContent = airportId || '?';
document.getElementById('airport-field').value = airportId;
document.title = (airportId || 'Unknown') + ' - Not Available';

document.getElementById('request-form').addEventListener('submit', function(e) {
  e.preventDefault();
  if (!airportId) return;
  var btn = document.getElementById('request-btn');
  var msg = document.getElementById('msg');
  btn.disabled = true;
  btn.textContent = 'Submitting...';

  var body = 'form-name=airport-request&airport=' + encodeURIComponent(airportId);
  fetch('/', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: body
  }).then(function(resp) {
    if (resp.ok) {
      msg.className = 'msg success';
      msg.textContent = 'Analysis requested for ' + airportId + '.';
      btn.textContent = 'Requested';
    } else {
      throw new Error('Server returned ' + resp.status);
    }
  }).catch(function(err) {
    msg.className = 'msg error';
    msg.textContent = 'Could not submit request: ' + err.message;
    btn.disabled = false;
    btn.textContent = 'Request Analysis';
  });
});
</script>
</body>
</html>
"""
    Path(output_path).write_text(html)


def generate_batch_outputs(output_stats, base_dir, airport_list_file):
    """Main entry point: print console summary and generate HTML index.

    airport_list_file: path to the airports file (with section headers).
                      If it's not a file, just prints the console summary.
    """
    print_visualization_summary(output_stats)

    if os.path.isfile(airport_list_file):
        sections = parse_airport_sections(airport_list_file)
    else:
        # Single airport code — make one section
        sections = [("Airports", list(output_stats.keys()))]

    generate_index_html(sections, output_stats, Path(base_dir) / "index.html")

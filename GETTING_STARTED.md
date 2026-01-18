# Getting Started with adsb-actions

This guide walks you through setting up adsb-actions from scratch. By the end, you'll have a working system that monitors ADS-B aircraft data.

## Prerequisites

- **Python 3.8 - 3.12** (check with `python3 --version`; 3.13 lacks aarch64 wheels for numpy/scipy)

## Quick Start (No Hardware Required)

You can try adsb_actions using the airplanes.live API:

```bash
# 1. Clone and set up
git clone https://github.com/eastham/adsb_actions.git
cd adsb_actions
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 2. Run with live data from the API (monitors aircraft near SFO)
python3 src/applications/tcp_api_monitor/monitor.py examples/hello_world_api.yaml
```

You should see aircraft printed to the console. Edit `hello_world_api.yaml` to change the location or radius.

## Using a local ADS-B receiver

#### Step 1: Configure readsb to expose JSON data

If you have readsb running, add `--net-json-port 30006` to its configuration:

```bash
# On most systems, edit /etc/default/readsb and add to RECEIVER_OPTIONS:
--net-json-port 30006
```

Then restart readsb: `sudo systemctl restart readsb`

#### Step 2: Run the simple monitor

```bash
python3 src/tools/examples/simple_monitor.py --ipaddr localhost --port 30006 examples/hello_world_rules.yaml
```

You should see aircraft printed to the console, no more than once per
minute per aircraft.

## Understanding the YAML Config

Rules have **conditions** (when to match) -- which are an ANDed expression -- and **actions** (what to do):

```yaml
rules:
  low_flying_alert:
    conditions:
      max_alt: 1000      # Only aircraft below 1000 feet
      cooldown: 5        # Don't repeat for same aircraft within 5 minutes
    actions:
      print: True        # Print to console
```

See [RULE_SCHEMA.yaml](RULE_SCHEMA.yaml) for all available conditions and actions.

## What's Optional

The core library works without any external integrations. These features are **optional**:

| Feature | Requires | Purpose |
|---------|----------|---------|
| Slack alerts | Slack webhook URL in `private.yaml` | Send notifications to Slack channels |
| Paging | Paging service credentials in `private.yaml` | Send pages/alerts to recipients |
| Database integration | DB credentials in `private.yaml` | Log events to a database |
| GUI (Stripview) | `pip install -e ".[all]"` | Visual flight strip display |

If you don't need these, you don't need to configure them.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `ADSB_PRIVATE_PATH` | `private.yaml` (project root) | Path to credentials file |

Example:
```bash
export ADSB_PRIVATE_PATH=/etc/adsb-actions/private.yaml
python3 main.py ...
```

## Troubleshooting

### "Connection refused" when connecting to readsb
- Verify readsb is running: `systemctl status readsb`
- Verify the JSON port is open: `nc -zv localhost 30006`
- Check readsb config includes `--net-json-port 30006`

### "No private.yaml found" message
This is normal if you haven't configured optional integrations. The library works fine without it.

### Import errors
Make sure you've activated the virtual environment:
```bash
source .venv/bin/activate
```

## Project Structure

| Directory | Description |
|-----------|-------------|
| `src/adsb_actions/` | Core library - rule engine, flight tracking |
| `src/core/database/` | Database abstraction layer |
| `src/core/network/` | Network utilities (TCP client) |
| `src/tools/analysis/` | Analysis tools - replay, visualization, hotspot detection |
| `src/tools/examples/` | Example scripts - simple_monitor, generic_analyzer |
| `src/applications/airport_monitor/` | Headless airport monitoring service |
| `src/applications/flight_info_display/` | Kivy-based FIDS (Flight Info Display) |
| `src/applications/stripview/` | Kivy-based ATC flight strip GUI |
| `examples/88nv/` | Example configs for 88NV airport |
| `examples/` | Example configs (hello_world_rules.yaml, low_altitude_alert.yaml, etc.) |


<h3> More things to try: </h3>

1. Tests are available: `pytest -s tests/*.py`
2. Invoke a sample UI: `python3 src/applications/stripview/controller.py --testdata tests/20minutes.json --delay .2 --rules examples/88nv/stripview_ui.yaml examples/88nv/regions/brc_large_regions.kml`
3. Command lines for other sample applications can be found in launch.json.

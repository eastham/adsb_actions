# Getting Started with adsb-actions

This guide walks you through setting up adsb-actions from scratch. By the end, you'll have a working system that monitors ADS-B aircraft data.

## Prerequisites

- **Python 3.8 - 3.12** (check with `python3 --version`; 3.13 lacks aarch64 wheels for numpy/scipy)
- **ADS-B data source** - either:
  - A local [readsb](https://github.com/wiedehopf/readsb) instance, OR
  - The included test data files for offline testing

## Quick Start (No Hardware Required)

You can try adsb-actions immediately using pre-recorded test data:

```bash
# 1. Clone and set up
git clone https://github.com/eastham/adsb_actions.git
cd adsb_actions
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 2. Run with test data (analyzes a 1-hour recording)
pytest -s tests/test_1hr.py
```

You should see output showing aircraft detections and rule matches.

## Your First Live Setup

### Step 1: Configure readsb to expose JSON data

If you have readsb running, add `--net-json-port 30006` to its configuration:

```bash
# On most systems, edit /etc/default/readsb and add to RECEIVER_OPTIONS:
--net-json-port 30006
```

Then restart readsb: `sudo systemctl restart readsb`

### Step 2: Run the simple monitor

```bash
cd src/simple
python3 main.py --ipaddr localhost --port 30006 basic_rules.yaml
```

You should see aircraft printed to the console every minute (based on the `cooldown: 1` in basic_rules.yaml).

## Understanding the YAML Config

Rules have **conditions** (when to match) and **actions** (what to do):

```yaml
rules:
  low_flying_alert:
    conditions:
      max_alt: 1000      # Only aircraft below 1000 feet
      cooldown: 5        # Don't repeat for same aircraft within 5 minutes
    actions:
      print: True        # Print to console
```

See [CONFIG_INSTRUCTIONS.yaml](CONFIG_INSTRUCTIONS.yaml) for all available conditions and actions.

## What's Optional

The core library works without any external integrations. These features are **optional**:

| Feature | Requires | Purpose |
|---------|----------|---------|
| Slack alerts | Slack webhook URL in `private.yaml` | Send notifications to Slack channels |
| Paging | Paging service credentials in `private.yaml` | Send pages/alerts to recipients |
| Database logging | AppSheet credentials in `private.yaml` | Log operations to a database |
| GUI (Stripview) | `pip install -e ".[all]"` | Visual flight strip display |

If you don't need these, you don't need to configure them. The library will log a note about missing `private.yaml` and continue working.

## Next Steps

1. **Customize rules**: Edit the YAML to match your use case
2. **Add KML regions**: Define geographic areas of interest
3. **Write callbacks**: Create Python functions triggered by rules
4. **Try the GUI**: `cd src/stripview && python3 controller.py --help`

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

## Example Projects

| Directory | Description |
|-----------|-------------|
| `src/simple/` | Minimal example - just prints aircraft |
| `src/analyzer/` | Analysis tools, replay, visualization |
| `src/stripview/` | Kivy-based flight strip GUI |
| `src/airport_monitor*/` | Airport monitoring applications |


<h3> More things to try: </h3>

1. Tests are available: pytest -s tests/*.py
1. Invoke a sample UI: cd src/stripview ;  python3 controller.py -- --testdata ../../tests/20minutes.json --delay .2 --rules ui.yaml ../../tests/brc_large_regions.kml
1. Command lines for other sample applications can be found in launch.json.

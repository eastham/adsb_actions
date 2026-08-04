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

# 3. Show a UI and live map of aircraft at your airport of choice 
pip install -e '.[gui]'
python3 src/tools/airport_quickstart.py KSJC
```

You should see aircraft printed to the console. Edit `hello_world_api.yaml` to change the location or radius.

## Using a local ADS-B receiver

If you want to use local ADS-B hardware, here's how to get that going:

#### Step 1: Configure readsb to expose JSON data

If you have readsb running, add `--net-json-port 30006` to its configuration:

```bash
# On most systems, edit /etc/default/readsb and add to RECEIVER_OPTIONS:
--net-json-port 30006
```

Then restart readsb: `sudo systemctl restart readsb`

#### Step 2: Run the simple monitor

```bash
python3 src/analyzers/simple_monitor.py --ipaddr localhost --port 30006 examples/hello_world_rules.yaml
```

You should see local aircraft printed to the console, no more than once per
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
      webhook: ['slack', 'slack_channel']  # send a message to a slack 
      shell: "echo 'Aircraft {flight_id}'" # run a shell command
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


<h3> Other notes: </h3>

1. Check out other applications in the examples directory
2. Command lines for more complex applications can be found in launch.json.
3. Some tools are in src/tools
4. Tests are available: `pytest -s tests/`

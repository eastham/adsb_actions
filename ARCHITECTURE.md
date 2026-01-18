# Architecture

## Directory Structure

```
src/
├── adsb_actions/           # Core library - the rule engine
├── core/
│   └── database/           # Database abstraction (AppSheet, NullDatabase)
├── lib/                    # Core libraries: replay, readsb_parse, map_elements
├── analyzers/              # Analysis scripts: simple_monitor, generic_analyzer
├── postprocessing/         # Post-processing: visualizer, hotspot_analyzer
├── tools/                  # ADS-B injection utilities
└── applications/
    ├── airport_monitor/    # Headless service: detect takeoffs/landings/LOS, push to DB
    ├── flight_info_display/# Kivy FIDS (arrival/departure board)
    ├── stripview/          # Kivy ATC-style flight strip display
    └── tcp_api_monitor/    # Monitor aircraft via internet API (airplanes.live)

examples/
├── 88nv/                   # Black Rock City airport monitoring
├── sf_bay_area/            # San Francisco Bay Area monitoring (SJC, OAK)
├── hello_world_rules.yaml  # Simplest example - print all aircraft
├── hello_world_api.yaml    # API-based example (no hardware required)
├── low_altitude_alert.yaml # Example with callback
└── legacy/                 # Old airport display examples (reference only)
```

## Core Concepts

**Rules** (YAML) define conditions and actions:
- **Conditions**: altitude range, KML regions, aircraft lists, proximity, etc.
- **Actions**: print, callback, webhook (Slack/page), shell command

**Callbacks** connect rules to your code:
```python
adsb_actions.register_callback("landing", my_landing_function)
```

## Data Flow

```
ADS-B Source (readsb :30006 or API or file)
       ↓
  AdsbActions.loop()
       ↓
  Rules.process_flight() → check conditions
       ↓
  Actions: callbacks, webhooks, print, shell
```

## Quick Start

```bash
# Print all aircraft in a 20nm ring around a point
python3 src/analyzers/simple_monitor.py \
  --ipaddr localhost --port 30006 \
  examples/hello_world_rules.yaml

# Run the stripview GUI
python3 src/applications/stripview/controller.py -- \
  --testdata tests/20minutes.json --delay .2 \
  --rules examples/88nv/stripview_ui.yaml
```

## Adding Your Own Config

1. Copy `examples/low_altitude_alert.yaml` as a starting point
2. Define KML regions if needed (use Google Earth to create)
3. Add rules with conditions and actions
4. Run with `simple_monitor.py` or build your own using the core library

See [RULE_SCHEMA.yaml](RULE_SCHEMA.yaml) for all available options.

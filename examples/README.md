# Examples

Configuration files and examples demonstrating how to use the ADSB Actions monitoring system.

## Getting Started Examples

- **hello_world_api.yaml** - Basic API-based monitoring using airplanes.live data with geographic KML zone filtering. Prints aircraft around SFO.
  ```bash
  python src/applications/tcp_api_monitor/monitor.py examples/hello_world_api.yaml
  ```

- **hello_world_rules.yaml** - Simple rule-based example using local readsb hardware data. Prints all aircraft seen.
  ```bash
  # From sample pre-saved data
  python src/analyzers/simple_monitor.py --directory tests/sample_readsb_data examples/hello_world_rules.yaml

  # From local readsb feed
  python src/analyzers/simple_monitor.py --ipaddr 127.0.0.1 --port 30006 examples/hello_world_rules.yaml
  ```

- **low_altitude_alert.yaml** / **low_altitude_callbacks.py** - Detects aircraft at 100-4500 feet with custom Python callback implementation.
  ```bash
  python src/analyzers/callback_runner.py --data tests/sample_readsb_data --callback_definitions examples/low_altitude_callbacks.py examples/low_altitude_alert.yaml
  ```

- **emergency_squawk_alert.yaml** - Detects emergency transponder codes (7500/7600/7700) with Slack notifications. Note: the `latlongring` is commented out by default; uncomment it for API use, or use with a local readsb feed.
  ```bash
  # From local readsb feed 
  python src/analyzers/simple_monitor.py --ipaddr 127.0.0.1 --port 30006 examples/emergency_squawk_alert.yaml
  ```

- **military_alert.yaml** - Detects military aircraft in a geographic area using API mode.
  ```bash
  python src/applications/tcp_api_monitor/monitor.py examples/military_alert.yaml
  ```

## Subdirectories

- **88nv/** - Black Rock City Municipal Airport monitoring examples with Slack/pager alerts for operations and watched aircraft.
  ```bash
  # Stripview UI demo from saved data
  python src/applications/stripview/controller.py -- --testdata tests/20minutes.json --delay .2 --rules examples/88nv/stripview_ui.yaml
  ```

- **sf_bay_area/** - San Francisco Bay Area configurations including Oakland airport rules and various StripView UI configs.
  ```bash
  # Stripview UI live from API (no hardware needed)
  python src/applications/stripview/controller.py -- --api --rules examples/sf_bay_area/stripview_ui_sjc_api.yaml
  ```

- **generated/** - Auto-generated airport-specific configurations for airports like EGLL, KOAK, KSFO, KSJC, etc., including KML region definitions.

- **legacy/** - Older airport_monitor configurations.

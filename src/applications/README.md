# Applications

Complete, runnable applications for real-world airport monitoring and flight tracking with both CLI and GUI interfaces.

## Modules

- **flight_info_display/** - Kivy desktop application displaying real-time flight status on a text board with aircraft categorization, pilot name lookups, and live altitude tracking.
  ```bash
  python src/applications/flight_info_display/monitor.py -- --testdata tests/20minutes.json --delay .2 --rules examples/88nv/flight_info_display_config.yaml
  ```

- **airport_monitor/** - Detects and logs airport operations (takeoffs/landings) to a database. Includes callbacks for landing, takeoff, and loss-of-separation events.
  ```bash
  # From saved data
  python src/applications/airport_monitor/main.py --testdata tests/20minutes.json --delay .01 --rules examples/88nv/airport_monitor_rules.yaml

  # From local readsb feed
  python src/applications/airport_monitor/main.py --ipaddr 127.0.0.1 --port 30006 --rules examples/88nv/airport_monitor_rules.yaml
  ```

- **stripview/** - Advanced Kivy MD GUI providing interactive flight strips with aircraft details, admin functions, FlightAware links, and database integration for enriched displays.
  ```bash
  # From API (no hardware needed)
  python src/applications/stripview/controller.py -- --api --rules examples/sf_bay_area/stripview_ui_sjc_api.yaml

  # From saved data
  python src/applications/stripview/controller.py -- --testdata tests/20minutes.json --delay .2 --rules examples/88nv/stripview_ui.yaml

  # From local readsb feed
  python src/applications/stripview/controller.py -- --ipaddr 127.0.0.1 --port 30006 --rules examples/sf_bay_area/stripview_ui_sjc.yaml
  ```

- **tcp_api_monitor/** - Monitors ADS-B data from public internet APIs (airplanes.live, adsb.one) without requiring local hardware, applying adsb_actions rules to API-sourced data.
  ```bash
  python src/applications/tcp_api_monitor/monitor.py examples/hello_world_api.yaml
  ```

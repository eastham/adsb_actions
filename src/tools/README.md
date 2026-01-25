# Tools

Utility scripts for ADS-B message encoding/injection, data replay, and airport configuration automation.

## Modules

- **ADSB_Encoder.py** - Comprehensive ADS-B message encoding library supporting Mode-S altitude encoding, CPR position encoding, CRC checksums, and DF17 message generation for synthetic test data.

- **inject_adsb.py** - Socket-based client that injects ADS-B messages into readsb with retry logic and Prometheus metrics.

- **replay_to_adsb.py** - Replays recorded JSON flight data to readsb at adjustable speeds (1-3000x) for testing and demo scenarios.
  ```bash
  python src/tools/replay_to_adsb.py --inject_addr localhost:30001 tests/20minutes.json --speed_x 1
  ```

- **airport_quickstart.py** - User-friendly quick-start tool that fetches live METAR data, calculates wind-favored runways, generates airport configs, and launches the stripview GUI.
  ```bash
  python src/tools/airport_quickstart.py KSJC
  ```

- **generate_airport_config.py** - Generates YAML configuration files for specific airports with runway geometries.

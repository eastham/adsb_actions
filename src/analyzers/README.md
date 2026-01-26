# Analyzers

Core analysis engines that process ADS-B flight data from various sources (live feeds, APIs, or recorded files) and evaluate them against YAML-defined rules and custom Python callbacks to detect events like takeoffs, landings, and proximity alerts.

## Modules

- **callback_runner.py** - Main analyzer that runs rules on flight data from live ADS-B feeds or recorded data dumps. Dynamically loads custom callback functions from Python files.
  ```bash
  python src/analyzers/callback_runner.py --data tests/sample_readsb_data --callback_definitions examples/low_altitude_callbacks.py examples/low_altitude_alert.yaml
  ```

- **analyze_from_files.py** - Processes historical flight data from nested directories of readsb data dumps to detect flight events in batch mode.
  ```bash
  python src/analyzers/analyze_from_files.py --yaml examples/88nv/detect_ops_from_files.yaml tests/sample_readsb_data
  ```

- **simple_monitor.py** - Basic sample implementation demonstrating core library capabilities with both network and file-based replay support.
  ```bash
  # From sample pre-saved data
  python src/analyzers/simple_monitor.py --directory tests/sample_readsb_data examples/hello_world_rules.yaml

  # From local readsb feed
  python src/analyzers/simple_monitor.py --ipaddr 127.0.0.1 --port 30006 examples/hello_world_rules.yaml
  ```

- **prox_analyze_from_files.py** - Proximity analysis tool for detecting nearby aircraft interactions from file-based historical data.  Data comes from a readsb data directory.
  ```bash
  python src/analyzers/prox_analyze_from_files.py --yaml examples/88nv/prox_analyze_from_files.yaml tests/sample_readsb_data --resample
  ```

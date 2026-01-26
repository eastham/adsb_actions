# Postprocessing

Post-analysis tools for data quality assessment and geospatial visualization of flight events and safety-critical interactions.

## Modules


- **hotspot_analyzer.py** - Library for spatial analysis of Loss-of-Separation events using Kernel Density Estimation (KDE) to identify geographic concentrations of safety-critical events. Used internally by visualizer.py.

- **visualizer.py** - Interactive map visualization tool that plots events on Folium-based maps with support for VFR sectional charts, satellite imagery, KDE heatmaps, color-coded event types, and GeoJSON overlays. Exports to standalone HTML.

  **Input**: Reads stdout from any analyzer with the `print_csv` action enabled. The `print_csv` action outputs CSV lines that visualizer.py parses for lat/lon coordinates and event metadata.

  ```bash
  # Run analyzer with print_csv action, pipe to visualizer (outputs airport_map.html)
  python src/analyzers/simple_monitor.py --directory tests/sample_readsb_data examples/hello_world_csv.yaml 2>&1 | python src/postprocessing/visualizer.py

  # Or from a saved log file
  cat analyzer_output.txt | python src/postprocessing/visualizer.py
  ```

  See [examples/hello_world_csv.yaml](../../examples/hello_world_csv.yaml) for a complete example with `print_csv`.

- **data_verifier.py** - Scans directories of readsb data dumps and reports data availability metrics (position data points per day/hour) for validating recorded data completeness.
  ```bash
  python src/postprocessing/data_verifier.py /path/to/readsb/data
  ```
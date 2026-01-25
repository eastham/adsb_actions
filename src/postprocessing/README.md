# Postprocessing

Post-analysis tools for data quality assessment and geospatial visualization of flight events and safety-critical interactions.

## Modules


- **hotspot_analyzer.py** - Library for spatial analysis of Loss-of-Separation events using Kernel Density Estimation (KDE) to identify geographic concentrations of safety-critical events.

- **visualizer.py** - Interactive map visualization tool that plots events on Folium-based maps with support for VFR sectional charts, satellite imagery, KDE heatmaps, color-coded event types, and GeoJSON overlays. Exports to standalone HTML.
  ```bash
  cat analyzer_output.txt | python src/postprocessing/visualizer.py --output map.html
  ```

- **data_verifier.py** - Scans directories of readsb data dumps and reports data availability metrics (position data points per day/hour) for validating recorded data completeness.
  ```bash
  python src/postprocessing/data_verifier.py /path/to/readsb/data
  ```
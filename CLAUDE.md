# Project context:
adsb-actions turns aircraft tracking data into automated actions.  It all

This module allows you to apply conditions and actions to JSON flight
data coming from [readsb](https://github.com/wiedehopf/readsb), API provider, or saved historical data. The conditions and actions are
specified in a simple human-readable YAML format.

A big downstream focus is detecting and mapping Loss Of Separation (LOS) events, which has
been used to improve safety in the real world at actual airports.

# Project rules
Use similar style as surrounding code unless it's really bad (and flag if so), 
except in tests where you should do whatever is cleanest.

Prioritize clean, understandable code.  anything subtle should be commented.
Docstrings per function are not required but if written should be brief.

# where to find example data

## /tests/1hr.json simple time-sorted JSONL exemplar.

## //tests/fixtures time-sorted JSONL focused on one airport KWVI

This file has a LOS event: tests/fixtures/KWVI/060825_KWVI_traffic.csv

## /data: source data stored here: time-sorted into JSONL in  global_*

## /examples/generated: partial pipeline results stored here

# common command lines

## LOS research high-level pipeline run:
 python src/tools/batch_los_pipeline.py \
        --start-date 08/01/25 --end-date 08/31/25 \
        --airports ./examples/busiest_nontowered_and_local.txt --max-airports 10

## run the LOS visualizer which generates static HTML with a high-level map and individual html's of events:
cat examples/generated/KWVI/KWVI_combined.csv.out | python3 src/postprocessing/visualizer.py --sw 36.76903279623333,-121.99851398823964 --ne 37.10236612956666,-121.58148784376036 --native-heatmap --heatmap-opacity 0.5 --heatmap-radius 50 --busyness-data examples/generated/KWVI/KWVI_busyness.json --data-quality examples/generated/KWVI/KWVI_quality.json --output examples/generated/KWVI/KWVI_map.html  --traffic-tiles tiles/traffic/ --no-browser

# Project conventions

## Running Python/tests
Always activate the venv before running Python commands:
```
source .venv/bin/activate && <command>
```

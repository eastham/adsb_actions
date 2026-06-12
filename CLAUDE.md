# Project context:
adsb-actions turns aircraft tracking data from ADS-B into automated actions.  It allows you to apply conditions and actions to JSON flight
data coming from [readsb](https://github.com/wiedehopf/readsb), API provider, or saved historical data. The conditions and actions are
specified in a simple human-readable YAML format.

A big focus of this work is detecting and mapping Loss Of Separation (LOS) events, which has been used to improve safety in the real world at actual airports.  

The user is actively conducting research with this code and generaling the work so this it can improve safety at airports globally.

# Project rules
Use similar style as surrounding code unless it's really bad (and flag if so), except in tests where you should do whatever is cleanest.

Prioritize clean, understandable code.  anything subtle should be commented.
Docstrings per function are not required but if written should be brief.

# where data lives, including convenient example data

## /tests/1hr.json simple time-sorted JSONL exemplar.

## //tests/fixtures time-sorted JSONL focused on one airport KWVI

This file has a LOS event: tests/fixtures/KWVI/060825_KWVI_traffic.csv

## /data: source data stored here: time-sorted into JSONL in  global_*

## /examples/generated: partial pipeline results stored here

# analysis pipleines

## Analysis pipelines have two versions:
### v1: per-airport analysis, data is in examples/generated and data/. code is mostly in src/tools

### v2: one-global-map analysis, data is in data/v2.  code is mostly in src/hotspots 

Architecture of v2 is described in src/hotspots/PIPELINE_PLAN.md
***assume all requests are about v2 unless specified otherwise.***

traffic tile generation for v2 is still done using the v1 era src/tools/traffic_tiles.py 


# common command lines

see src/tools/COMMANDS

# Project conventions

## Running Python/tests
Always activate the venv before running Python commands:
```
source .venv/bin/activate && <command>
```

## Code locations
core code in src/adsb_actions including resampler
analysis code in src/analyzers

There are several large data directories linked from the project directory, so don't use find in the root.  all relevant code is in src/

Note that the "data" directory in the project root is a network
drive, if things appear to be missing it probably got unmounted
and the user needs to correct.

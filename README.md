<h2>adsb-actions: A package for taking actions based on live or recorded ADS-B data.</h2>

This module allows you to apply conditions and actions to JSON flight
data coming from [readsb](https://github.com/wiedehopf/readsb), or
other ADS-B data provider.  The conditions and actions are
specified in a simple human-readable YAML format.

These actions can then:
- Do real-time analysis, for example detect loss-of-separation events:

<p align="center">
  <img src="misc/los.png?raw=true" alt="Screenshot of LOS">
</p>

- Drive a UI:

<p align="center">
  <img src="misc/stripview.png?raw=true" alt="Screenshot of Stripview">
</p>

- Visualize events and find hotspots:

<p align="center">
  <img src="misc/heatmap.png?raw=true" alt="Screenshot of hotspots">
</p>

- Do offline analysis, for example to generate operational counts and statistics:

<p align="center">
  <img src="misc/landing.png?raw=true" alt="Screenshot of landings">
</p>
- Trigger Slack alerts based on arbitrary conditions
- Push operations to a database
- Whatever else you can imagine!

<h3>Overview</h3>
Each YAML rule contains **conditions** and **actions**. 

**conditions** are an ANDed set, and can include altitude ranges, lat/long proximity, location within a region specified in a KML file, etc.

**actions** include Slack, paging / JSON webhook, and python callback.  See CONFIG_INSTRUCTIONS.yaml for more info.

<h3>Example YAML config:</h3>

```
  config:
    kmls:  # optional KML files that specify geographic regions.
      - tests/test3.kml 

  aircraft_lists:  # optional lists of tail numbers of interest.
    alert_aircraft: [ "N12345" ]

  rules:
    nearby:
      conditions: 
        min_alt: 4000        # feet MSL, must be >= to match
        max_alt: 10000       # feel MSL, must be <= to match
        aircraft_list: alert_aircraft  # use aircraft_list above
        latlongring: [20, 40.763537, -119.2122323]
        regions: [ "23 upwind" ]  # region defined in KML
      actions:
        callback: nearby_cb  # call a function registered under this name
        print: True          # print info about this match to console
```

<h3>Example execution</h3>

1. (re)start your readsb to expose its raw output on a local port: --net-json-port=30006
2. cd src/analyzer
2. python3 generic_analyzer.py --data ../../tests/sample_readsb_data --callback_definitions=example_callbacks.py example_rules.yaml 

<h3>Installation from github:</h3>

1. (download or clone code from github)
1. python3 -m venv .venv
1. source .venv/bin/activate
1. pip3 install -e .  # Core only (no GUI or analysis tools)
1. pip3 install -e ".[all]"  # All features
1. pytest -s tests/test_1hr.py

<h3>Quick initial testing, assuming you have a radio with readsb running:</h3>

1. Add "--net-json-port 30006" to readsb startup args, as adsb_actions reads the json output
1. Run "python3 src/adsb_actions/adsbactions.py --ipaddr localhost --port 30006 src/adsb_actions/basic_rules.yaml"
1. You should see output for the aircraft readsb is seeing.

```
    Successful Connection
    INFO:adsbactions:Setup done
    01/21/24 14:38: Rule print_all_aircraft matched for N57111: 5350 MSL 141 deg 166.6 kts 37.8715, -122.2719
    01/21/24 14:38: Rule print_all_aircraft matched for N449WN: 7150 MSL 322 deg 194.8 kts 37.8703, -122.1147
```

<h3> More things to try: </h3>

1. Tests are available: pytest -s tests/*.py
1. Invoke a sample UI: cd src/stripview ;  python3 controller.py -- --testdata ../../tests/1hr.json --rules ui.yaml --delay .01 ../../tests/test2.kml
1. Command lines for other sample applications can be found in launch.json.

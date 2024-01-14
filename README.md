<h2>adsb-actions: A package for taking actions based on ADS-B data.</h2>

This module allows you to connect to a readsb or other ADS-B
data provider, and have actions taken according to rules defined in a simple YAML format.  

Each YAML rule contains **conditions** and **actions**. 

**conditions** are an ANDed set, and can include altitude ranges, lat/long proximity, location within a region specified in a KML file, etc.

**actions** include Slack, paging / JSON webhook, and python callback.  See CONFIG_INSTRUCTIONS.yaml for more.

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

<h3>API Usage:</h3>

```
    adsb_actions = AdsbActions(yaml_config, ip=args.ipaddr, port=args.port)
    adsb_actions.register_callback("nearby_cb", nearby_cb)
    adsb_actions.loop()
```

<h3>Installation from github:</h3>

1. (download or clone code from github)
1. python3 -m venv .venv
1. source .venv/bin/activate
1. pip3 install .
1. (install geos native library: https://libgeos.org/usage/install/)
1. pytest -s tests/test_1hr.py

<h3> Running and testing: </h3>

1. Tests are available: pytest -s tests/*.py
1. Invoke a sample UI: cd src/stripview ;  python3 controller.py -- --testdata ../../tests/1hr.json --rules ui.yaml --delay .01 ../../tests/test2.kml
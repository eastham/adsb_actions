<h2>adsb-actions: A package for taking actions based on live or recorded ADS-B data.</h2>

This module allows you to apply conditions and actions to JSON flight
data coming from [readsb](https://github.com/wiedehopf/readsb), or
other ADS-B data provider.  The conditions and actions are
specified in a simple human-readable YAML format.

These actions can then:

- Drive a UI:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/stripview.png?raw=true" alt="Screenshot of Stripview"><br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/monitor.png?raw=true" alt="Screenshot of monitor">

- Do real-time analysis, for example detect loss-of-separation events:

&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/los.png?raw=true" alt="Screenshot of LOS">

- Visualize events and find hotspots:

&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/heatmap.png?raw=true" alt="Screenshot of hotspots">

- Do offline analysis, for example to generate operational counts and statistics:

&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/landing.png?raw=true" alt="Screenshot of landings">
- Trigger Slack alerts based on arbitrary conditions
- Push operations to a database
- Whatever else you can imagine!

<h3>Overview</h3>
Each YAML rule contains **conditions** and **actions**. 

**conditions** are an ANDed set, and can include altitude ranges, lat/long proximity, location within a region specified in a KML file, etc.

**actions** include Slack, paging / JSON webhook, and python callback.  See CONFIG_INSTRUCTIONS.yaml for more info.

<h3>Example YAML config:</h3>
This will trigger a callback and save information to stdout when aircraft N12345 matches certain location criteria:

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

<h3>Installation from github:</h3>

1. (download or clone code from github)
1. python3 -m venv .venv
1. source .venv/bin/activate
1. pip3 install -e .  # Core only (no GUI or analysis tools)
1. pip3 install -e ".[all]"  # All features
1. pytest -s tests/test_1hr.py


<h3>Quick initial testing, assuming you have a radio with readsb running:</h3>

1. Add "--net-json-port 30006" to readsb startup args, as adsb_actions reads the json output
1. Run "cd src/analyzer; python3 src/adsb_actions/adsbactions.py --ipaddr localhost --port 30006 --callback_definitions=example_callbacks.py example_rules.yaml
1. You should see output for the aircraft readsb is seeing.


<h3> More things to try: </h3>

1. Tests are available: pytest -s tests/*.py
1. XXXXX Invoke a sample UI: cd src/stripview ;  python3 controller.py -- --testdata ../../tests/1hr.json --rules ui.yaml --delay .01 ../../tests/test2.kml
1. Command lines for other sample applications can be found in launch.json.

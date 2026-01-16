<h2>adsb-actions: A package for taking actions based on live or recorded ADS-B data.</h2>

This module allows you to apply conditions and actions to JSON flight
data coming from [readsb](https://github.com/wiedehopf/readsb), or
other ADS-B data provider.  The conditions and actions are
specified in a simple human-readable YAML format.

These actions can then:

- Drive a UI:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/stripview.png?raw=true" alt="Screenshot of Stripview"><br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/monitor.png?raw=true" alt="Screenshot of monitor">

- Do real-time analysis, for example detect loss-of-separation events:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/los.png?raw=true" alt="Screenshot of LOS">

- Visualize events and find hotspots:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/heatmap.png?raw=true" alt="Screenshot of hotspots">

- Do offline analysis, for example to generate operational counts and statistics:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="misc/landing.png?raw=true" alt="Screenshot of landings">
- Trigger Slack alerts based on arbitrary conditions
- Push operations to a database
- Whatever else you can imagine!

<h3>Overview</h3>
Each YAML rule contains ✅ <strong>conditions</strong> and ⚡ <strong>actions</strong>. 
<p>

✅ <strong>Conditions</strong> are an ANDed set, and can include altitude ranges, location within a region specified in a KML file, movement between regions, proximity to other aircraft, and more.
<p>
⚡ <strong>Actions</strong> include logging, Slack, paging / JSON webhook, shell execution, and python callback.  See CONFIG_INSTRUCTIONS.yaml for more info.

<h3>Example YAML config:</h3>
This will trigger a callback and log information to stdout when aircraft N12345 matches certain location criteria:<p>

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



**Ready to try it?** See [GETTING_STARTED.md](GETTING_STARTED.md) for a step-by-step setup guide.

<h3>Prerequisites</h3>

- Python 3.8 or higher
- An ADS-B data source (readsb, dump1090, etc.) OR use included test data

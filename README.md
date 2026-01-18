<h2>adsb-actions: Turn aircraft tracking data into automated actions.</h2>

This module allows you to apply conditions and actions to JSON flight
data coming from [readsb](https://github.com/wiedehopf/readsb), saved historical data, or
internet ADS-B API provider.  The conditions and actions are
specified in a simple human-readable YAML format.

These actions can then:

⭒ Drive a UI:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="screenshots/stripview.png?raw=true" alt="Screenshot of Stripview"><br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="screenshots/monitor.png?raw=true" alt="Screenshot of monitor">

⭒ Do real-time analysis, for example detect loss-of-separation events:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="screenshots/los.png?raw=true" alt="Screenshot of LOS">

⭒ Visualize events and find hotspots:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="screenshots/heatmap.png?raw=true" alt="Screenshot of hotspots">

⭒ Do offline analysis, for example to generate operational counts and statistics:<br>
&nbsp;&nbsp;&nbsp;&nbsp;<img src="screenshots/landing.png?raw=true" alt="Screenshot of landings">

⭒ Trigger Slack alerts based on arbitrary conditions

⭒ Push operations (takeoffs/landings/etc) to a database

⭒ Whatever else you can imagine!

<h2>Overview</h2>
Each YAML rule contains ✅ <strong>conditions</strong> and ⚡ <strong>actions</strong>. 
<p>

✅ <strong>Conditions</strong> are an ANDed set, and can include altitude ranges, location within a region specified in a KML file, movement between regions, proximity to other aircraft, and more.
<p>
⚡ <strong>Actions</strong> include logging, Slack, paging / JSON webhook, shell execution, and python callback.  See RULE_SCHEMA.yaml for more info.

<h2>Example rules:</h2>
This will trigger a callback and send a slack message when an aircraft
is seen below 2000 feet in a certain geograpic area:<p>

```
  rules:
    low_alt:
      conditions:
        max_alt: 2000
        latlongring: [10, 40.763537, -119.2122323]
      actions:
        callback: print_aircraft_data   # call this when matched
        webhook: ['slack', 'emergency_aircraft_channel']
```

<h2>Ready to try it?</h2> See [GETTING_STARTED.md](GETTING_STARTED.md) for a step-by-step setup guide.

<h2>Prerequisites</h2>

- Python 3.8 or higher
- An ADS-B data source (readsb, dump1090, etc.) OR use included test data

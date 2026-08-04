AdsbActions.py is the main API for the library.

The following will instantiate the library, attempt to connect to a network
socket, and process the ADS-B data coming in:

    yaml_file = "examples/hello_world_rules.yaml" # simple test config
    adsb_actions = AdsbActions(yaml_file=yaml_file, ip=[ipaddr], port=[port])
    adsb_actions.register_callback("nearby_cb", nearby_cb)
    adsb_actions.loop()

Alternatively, you can process data from a directory of readsb trace files:

    from lib import replay
    yaml_file = "examples/hello_world_rules.yaml"
    allpoints = replay.read_data("/path/to/data/directory")
    allpoints_iterator = replay.yield_json_data(allpoints)
    adsb_actions = AdsbActions(yaml_file=yaml_file)
    adsb_actions.loop(iterator_data=allpoints_iterator)

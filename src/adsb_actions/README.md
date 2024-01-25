AdsbActions.py is the main API For the library.

The following will instantiate the library, attempt to connect to a network
socket, and process the ADS-B data coming in:

    yaml_file = "src/adsb_actions/basic_rules.yaml" # simple test config
    adsb_actions = AdsbActions(yaml_file=yaml_file, ip=[ipaddr], port=[port])
    adsb_actions.register_callback("nearby_cb", nearby_cb)
    adsb_actions.loop()

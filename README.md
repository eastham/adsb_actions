adsb-actions: A package for taking actions based on ADS-B data.

This module allows you to connect to a readsb or other ADS-B
data provider, and have actions taken as a result using a 
flexible set of rule definition in YAML.  

Each YAML rule contains *conditions* and *actions*. 

*conditions* are an ANDed set, and can include altitude ranges, lat/long proximity, location within a region specified in a KML file, etc.

*actions* include Slack, paging / JSON webhook, and native callback.  See CONFIG_INSTRUCTIONS.yaml for more.

Usage:

    adsb_actions = AdsbActions(yaml_data, ip=args.ipaddr, port=args.port)
    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.loop()

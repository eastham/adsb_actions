from io import StringIO
import main

def process_adsb(json_string, flights, rules_instance):
    adsb_test_buf = StringIO(json_string)
    listen = main.TCPConnection()
    listen.f = adsb_test_buf
    main.flight_read_loop(listen, flights, rules_instance)

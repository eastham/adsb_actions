#!/usr/bin/python3
import socket
import json
import logging
import signal
import datetime
import sys
import time
from pathlib import Path
path_root = Path(__file__).parents[0]
sys.path.append(str(path_root))

import yaml
from rules import Rules
from flights import Flights
from stats import Stats
from bboxes import Bboxes
from location import Location

logger = None

class TCPConnection:
    def __init__(self=None, host=None, port=None, retry=False, exit_cb=None):
        self.host = host
        self.port = port
        self.sock = None
        self.retry = retry
        self.exit_cb = exit_cb
        self.f = None

    def connect(self):
        try:
            if self.sock: self.sock.close()     # reconnect case
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            print('Successful Connection')
        except Exception as e:
            print('Connection Failed: '+str(e))

        self.f = self.sock.makefile()

    def readline(self):
        return self.f.readline()

def sigint_handler(signum, frame):
    sys.exit(1)

def setup_network(ipaddr, port, retry_conn=True, exit_cb=None):
    print("Connecting to %s:%d" % (ipaddr, int(port)))

    signal.signal(signal.SIGINT, sigint_handler)
    conn = TCPConnection(ipaddr, int(port), retry_conn, exit_cb)
    conn.connect()

    logging.info("Setup done")
    return conn

def flight_update_read(listen, flights: Flights, rules: Rules):
    try:
        line = listen.readline()
        logger.debug("Read line: %s ", line)

        jsondict = json.loads(line)
    except json.JSONDecodeError:
        if not listen.sock:
            return -1  # test enviro
        else:
            logger.error("JSON Parse fail: %s", line)
    except Exception:
        return -1
        print(f"Socket input/parse error, reconnect plan = {listen.retry}")
        if listen.retry:
            time.sleep(2)
            listen.connect()
        else:
            if listen.exit_cb:
                listen.exit_cb()
            return -1
            # sys.exit(0) # XXX adsb_pusher used this
        return 0

    Stats.json_readlines += 1
    loc_update = Location.from_dict(jsondict)
    last_ts = flights.add_location(loc_update, rules)
    return last_ts

def flight_read_loop(listen: TCPConnection, flights: Flights, rules: Rules):
    CHECKPOINT_INTERVAL = 10 # seconds

    while True:
        last_read_time = flight_update_read(listen, flights, rules)
        if last_read_time == 0: continue
        if last_read_time < 0: break
        if not flights.last_checkpoint:
            flights.last_checkpoint = last_read_time

        # XXX this skips during gaps when no aircraft are seen
        if last_read_time and last_read_time - flights.last_checkpoint >= CHECKPOINT_INTERVAL:
            datestr = datetime.datetime.utcfromtimestamp(
                last_read_time).strftime('%Y-%m-%d %H:%M:%S')
            logging.debug("%ds Checkpoint: %d %s", CHECKPOINT_INTERVAL, last_read_time, datestr)

            flights.expire_old(rules, last_read_time)
            flights.check_distance(rules, last_read_time)
            flights.last_checkpoint = last_read_time

def setup_logger():
    global logger

    logging.basicConfig(level=logging.INFO)
    logging.info('System started.')
    logger = logging.getLogger(__name__)
    logger.level = logging.DEBUG

def setup_flights(yaml_data) -> Flights:
    setup_logger()

    bboxes_list = []
    try:
        for f in yaml_data['config']['kmls']:
            bboxes_list.append(Bboxes(f))
    except FileNotFoundError:
        logging.critical("File not found: %s", f)
    except KeyError:
        pass

    return Flights(bboxes_list)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('--ipaddr', help="IP address to connect to", required=True)
    parser.add_argument('--port', help="port to connect to", required=True)
    parser.add_argument('yaml', help='Path to the YAML file')
    args = parser.parse_args()

    listen = setup_network(args.ipaddr, args.port)

    with open(args.yaml, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    flights = setup_flights(yaml_data)
    rules = Rules(yaml_data)

    flight_read_loop(listen, flights, rules)
 
import logging
import json
import datetime
import time
import signal
import socket
import select
import sys
from io import StringIO

from rules import Rules
from flights import Flights
from bboxes import Bboxes
from stats import Stats
from location import Location

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

class AdsbActions:
    def __init__(self, yaml,ip=None, port=None, exit_cb=None):
        self.flights = Flights(self.load_bboxes(yaml))
        self.rules = Rules(yaml)
        self.exit_cb = exit_cb
        if ip and port:
            self.listen = self.setup_network(ip, port)

    def load_bboxes(self, yaml):
        bboxes_list = []
        try:
            for f in yaml['config']['kmls']:
                bboxes_list.append(Bboxes(f))
        except FileNotFoundError:
            logging.critical("File not found: %s", f)
        except KeyError:
            pass
        return bboxes_list

    def setup_network(self, ipaddr, port, retry_conn=True):
        print("Connecting to %s:%d" % (ipaddr, int(port)))

        signal.signal(signal.SIGINT, sigint_handler)
        conn = TCPConnection(ipaddr, int(port), retry_conn)
        conn.connect()

        logging.info("Setup done")
        return conn

    def flight_update_read(self):
        jsondict = None
        try:
            line = self.listen.readline()
            if not line:
                return -1
            logger.debug("Read line: %s ", line)
            jsondict = json.loads(line)
        except json.JSONDecodeError:
            if not self.listen.sock:
                return -1  # test environment
            else:
                logger.error("JSON Parse fail: %s", line)
        except Exception:
            print(f"Socket input error, reconnect plan = {self.listen.retry}")
            if self.listen.retry:
                time.sleep(2)
                self.listen.connect()
            else:
                if self.exit_cb:
                    self.exit_cb()
                return -1
                # sys.exit(0) # XXX adsb_pusher used this
            return 0

        Stats.json_readlines += 1
        if jsondict:
            loc_update = Location.from_dict(jsondict)
            return self.flights.add_location(loc_update, self.rules)
        else:
            return 0

    def loop(self, data=None):
        CHECKPOINT_INTERVAL = 5 # seconds

        if data:  # inject string data for testing
            self.listen = TCPConnection()
            self.listen.f = StringIO(data)

        while True:
            last_read_time = self.flight_update_read()
            if last_read_time == 0: continue
            if last_read_time < 0: break
            if not self.flights.last_checkpoint:
                self.flights.last_checkpoint = last_read_time

            # Here we do periodic maintenance tasks, and expensive operations.
            # Note: this skips during gaps when no aircraft are seen.
            # If timely expiration/maintenance is needed, dummy events can be
            # injected.
            if (last_read_time and
                last_read_time - self.flights.last_checkpoint >= CHECKPOINT_INTERVAL):
                datestr = datetime.datetime.utcfromtimestamp(
                    last_read_time).strftime('%Y-%m-%d %H:%M:%S')
                logging.debug("%ds Checkpoint: %d %s", CHECKPOINT_INTERVAL, last_read_time, datestr)

                self.flights.expire_old(self.rules, last_read_time)
                self.flights.check_distance(self.rules, last_read_time)
                self.flights.last_checkpoint = last_read_time

    def register_callback(self, name: str, fn):
        self.rules.callbacks[name] = fn

class TCPConnection:
    def __init__(self=None, host=None, port=None, retry=False):
        self.host = host
        self.port = port
        self.sock = None
        self.retry = retry
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

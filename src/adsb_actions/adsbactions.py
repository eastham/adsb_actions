"""This is the main API For the library.

The following code will instantiate the library, attempt to connect to a network
socket, and process the ADS-B data coming in:
    adsb_actions = AdsbActions(yaml_config, ip=args.ipaddr, port=args.port)
    adsb_actions.register_callback("nearby_cb", nearby_cb)
    adsb_actions.loop()

See CONFIGURATION_INSTRUCTIONS.yaml for yaml_config specs.
"""
import logging
import json
import datetime
import time
import signal
import socket
import sys
from io import StringIO
from typing import Callable

from rules import Rules
from flights import Flights
from bboxes import Bboxes
from stats import Stats
from location import Location

logger = logging.getLogger(__name__)

class AdsbActions:
    """Main API for the library."""

    def __init__(self, yaml, ip=None, port=None, bboxes=None):
        """optional arg bboxes forces what bboxes the system uses,
        overriding anything specified in the yaml."""
        self.flights = Flights(bboxes or self._load_bboxes(yaml))
        self.rules = Rules(yaml)
        if ip and port:
            self.listen = self._setup_network(ip, port)

    def loop(self, data: str = None, delay: float = 0.) -> None:
        """Run forever, processing ADS-B json data on the previously-opened socket.
        Will terminate when socket is closed.

        Args:
            data: instead of using the socket, process this data instead. 
                Useful for testing.
            delay: pause for this many seconds between input lines, for testing.
                .01-.05 is reasonable.
        """
        # TODO this probably should be a configurable instance variable:
        CHECKPOINT_INTERVAL = 5 # seconds.  How often to do mainentance tasks.

        if data:  # inject string data for testing
            self.listen = TCPConnection()
            self.listen.f = StringIO(data)

        while True:
            last_read_time = self._flight_update_read()

            if last_read_time == 0: continue
            if last_read_time < 0: break
            if not self.flights.last_checkpoint:
                self.flights.last_checkpoint = last_read_time

            # Run a "Checkpoint".
            # Here we do periodic maintenance tasks, and expensive operations.
            # Note: this will fail during gaps when no aircraft are seen.
            # If timely expiration/maintenance is needed, dummy events can be
            # injected.
            if (last_read_time and
                last_read_time - self.flights.last_checkpoint >= CHECKPOINT_INTERVAL):
                datestr = datetime.datetime.utcfromtimestamp(
                    last_read_time).strftime('%Y-%m-%d %H:%M:%S')
                logger.debug("%ds Checkpoint: %d %s", CHECKPOINT_INTERVAL, last_read_time, datestr)

                self.flights.expire_old(self.rules, last_read_time)
                self.rules.handle_proximity_conditions(self.flights, last_read_time)
                self.flights.last_checkpoint = last_read_time

            if delay:
                time.sleep(delay)

    def register_callback(self, name: str, fn: Callable) -> None:
        """Associate the given name string with a function to call."""
        self.rules.callbacks[name] = fn

    def register_webhook(self, url: str) -> None:
        """Call the given url when a webhook action is needed.  
        TODO not clear this is the right way to do this, should it be
        in the yaml instead?"""
        self.rules.webhook = url

    def _load_bboxes(self, yaml: str) -> list[Bboxes]:
        """Load the kml files found in the yaml, and parse those kmls."""
        bboxes_list = []
        try:
            for f in yaml['config']['kmls']:
                bboxes_list.append(Bboxes(f))
        except FileNotFoundError:
            logger.critical("File not found: %s", f)
            sys.exit(-1)
        except KeyError:
            pass
        return bboxes_list

    def _setup_network(self, ipaddr : str, port : int,
                       retry_conn : bool = True):
        """Open network connection."""
        print("Connecting to %s:%d" % (ipaddr, int(port)))

        signal.signal(signal.SIGINT, sigint_handler)
        conn = TCPConnection(ipaddr, int(port), retry_conn)
        conn.connect()

        logger.info("Setup done")
        return conn

    def _flight_update_read(self) -> float:
        """Attempt to read a line from the socket, and process it.
        Returns:
            a timestamp of the parsed location update if successful, 
            otherwise zero"""

        jsondict = None
        try:
            line = self.listen.readline()
            if not line:
                return -1
            logger.info("Read line: %s ", line.strip())
            jsondict = json.loads(line)
        except json.JSONDecodeError:
            if not self.listen.sock:
                return -1  # test environment
            else:
                logger.error("JSON Parse fail: %s", line)
        except Exception:
            print(f"Socket input error, reconnect plan = {self.listen.retry}")
            if self.listen.retry:
                # TODO needs testing/improvement.  This didn't always work in the past...
                time.sleep(2)
                self.listen.connect()
            else:
                return -1
                # sys.exit(0) # XXX adsb_pusher used this
            return 0

        Stats.json_readlines += 1

        if jsondict:
            # We got some data, process it.
            loc_update = Location.from_dict(jsondict)
            return self.flights.add_location(loc_update, self.rules)
        else:
            return 0


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

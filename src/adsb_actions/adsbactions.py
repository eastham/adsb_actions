"""This is the main API For the  library.

The following code will instantiate the library, attempt to connect to a network
socket, and process the ADS-B data coming in:
    adsb_actions = AdsbActions(yaml_config, ip=args.ipaddr, port=args.port)
    adsb_actions.loop()

Also useful, to support rules that want to call code:
    adsb_actions.register_callback("nearby_cb", nearby_cb)

See CONFIGURATION_INSTRUCTIONS.yaml for yaml_config specs.
"""
import logging
import json
import datetime
import time
import signal
import socket
import sys
import yaml
from io import StringIO
from typing import Callable

from .rules import Rules
from .flights import Flights
from .bboxes import Bboxes
from .stats import Stats
from .location import Location

logger = logging.getLogger(__name__)

class AdsbActions:
    """Main API for the library."""

    def __init__(self, yaml_data=None, yaml_file=None, ip=None, port=None,
                 mport=None, bboxes=None):
        """Main API for the library.  You can provide network port info in the
        constructor here, or specify local data sources in the subsequent call to
        loop().  Either yaml_data or yaml_file must be specified.

        Args:
            yaml_data: optional yaml data to use instead of loading from a file
            yaml_file: optional path to a yaml file to load
            ip: optional ip address to connect to
            port: optional port to conect to
            mport: optional metrics port
            exit_cb: optional callback to fire when socket closes or EOF is
                reached.
            bboxes: optional - forces what bounding boxes the system uses,
                overriding anything specified in the yaml."""

        assert yaml_data or yaml_file, "Must provide yaml or yaml_file"
        if yaml_file:
            with open(yaml_file, 'r', encoding='utf-8') as file:
                yaml_data = yaml.safe_load(file)

        self.flights = Flights(bboxes or self._load_bboxes(yaml_data))
        self.rules = Rules(yaml_data)
        self.listen = None
        self.data_iterator = None

        if ip and port:
            self.listen = self._setup_network(ip, port)

    def loop(self, string_data = None, iterator_data = None, delay: float = 0.) -> None:
        """Process ADS-B json data in a loop on the previously-opened socket.
        Will terminate when socket is closed.

        Args:
            string_data: optional: process this strinfigied JSON data instead
                of going to network
            iterator_data: optional: process data from this iterator that yields
                JSON instead of going to network.  Used for streaming large 
                amounts of data.
            delay: pause for this many seconds between input lines, for testing.
                .01-.05 is reasonable to be able to see what's going on.
        """
        # TODO this probably should be a configurable instance variable:
        CHECKPOINT_INTERVAL = 5 # seconds.  How often to do mainentance tasks.

        # Two ways to inject data for non-network cases:
        if string_data:
            self.listen = TCPConnection()
            self.listen.f = StringIO(string_data)
        else:
            self.data_iterator = iterator_data

        while True:
            last_read_time = self._flight_update_read()

            if last_read_time == 0: continue
            if last_read_time < 0: break
            if not self.flights.last_checkpoint:
                self.flights.last_checkpoint = last_read_time

            # Run a "Checkpoint".
            # Here we do periodic maintenance tasks, and expensive operations.
            # Note: this will of course not happen during gaps when no aircraft
            # are seen.
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

        logger.info("Parsed %s points.", Stats.json_readlines)
        self.rules.print_final_report()

    def register_callback(self, name: str, fn: Callable) -> None:
        """Associate the given name string, used in the configuration YAML,
        with a function to call.
        """
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
        """Attempt to read a line from the socket or other data source,
        and process it.

        Returns:
            a timestamp of the parsed location update if successful, 
            zero if not successful, -1 on EOF (in non-network cases)"""

        # TODO this function is a bit of a mess with all the
        # returns...needs cleanup and retest when/if we improve
        # the network resiliency...
        jsondict = None
        try:
            if self.listen:
                line = self.listen.readline()
                if not line:
                    return -1   # file EOF
                jsondict = json.loads(line)
            else:
                jsondict = next(self.data_iterator)

        except json.JSONDecodeError:
            logger.error("JSON Parse fail: %s", line)
        except StopIteration:
            return -1       # iterator EOF
        except Exception:
            logger.error("Socket input error")
            if self.listen.retry:
                # TODO needs testing/improvement.  This didn't always work in the past...
                logger.error("Attempting reconnect...")
                time.sleep(2)
                self.listen.connect()
                return 0
            else:
                return -1

        logger.debug("Read json: %s ", str(jsondict))
        Stats.json_readlines += 1

        if jsondict and 'alt_baro' in jsondict:
            # We got some valid data, process it. (points with no altitude
            # are ignored, they are likely to be dummy entries anyway)
            loc_update = Location.from_dict(jsondict)
            return self.flights.add_location(loc_update, self.rules)

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

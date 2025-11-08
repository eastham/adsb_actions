"""This is the main API For the  library.

The following code will instantiate the library, attempt to connect to a network
socket, and process the ADS-B data coming in:
    adsb_actions = AdsbActions(yaml_config, ip=args.ipaddr, port=args.port, mport=args.mport)
    adsb_actions.loop()

Also useful, to support rules that want to call code:
    adsb_actions.register_callback("nearby_cb", nearby_cb)

See CONFIGURATION_INSTRUCTIONS.yaml for yaml_config specs.
"""
import json
import datetime
import logging
import time
import traceback
import signal
import socket
import sys
from io import StringIO
from typing import Callable
import yaml

from .rules import Rules
from .flights import Flights
from .bboxes import Bboxes
from .stats import Stats
from .location import Location
from .resampler import Resampler

from prometheus_client import start_http_server, Gauge

from .adsb_logger import Logger

logger = logging.getLogger(__name__)
logger.level = logging.INFO
LOGGER = Logger()

class AdsbActions:
    """Main API for the library."""

    def __init__(self, yaml_data=None, yaml_file=None, ip=None, port=None,
                 mport=None, bboxes=None, expire_secs=180, pedantic=False,
                 resample=False):
        """Main API for the library.  You can provide network port info in the
        constructor here, or specify local data sources in the subsequent call to
        loop().  Either yaml_data or yaml_file must be specified.

        Args:
            yaml_data: optional yaml data to use instead of loading from a file
            yaml_file: optional path to a yaml file to load
            ip: optional ip address to connect to
            port: optional port to conect to
            mport: optional metrics port
            bboxes: optional - forces what bounding boxes the system uses,
                overriding anything specified in the yaml.
            expire_secs: how long to keep flights around after last observed
            pedantic: if True, enable strict behavior: checkpoints after each
                observation, and all rule checks apply even if aircraft are 
                not in known bounding boxes.
            resample: if True, enable resampling to detect proximity events
                between position updates.
        """

        assert yaml_data or yaml_file, "Must provide yaml or yaml_file"
        if yaml_file:
            with open(yaml_file, 'r', encoding='utf-8') as file:
                yaml_data = yaml.safe_load(file)

        self.flights = Flights(bboxes or self._load_bboxes(yaml_data),
                               ignore_unboxed_flights=not pedantic)
        self.rules = Rules(yaml_data)
        self.tcp_conn = None
        self.data_iterator = None
        self.exit_loop_flag = False      # set externally if we need to exit the main loop
        self.expire_secs = expire_secs
        self.pedantic = pedantic
        self.enable_resample = resample

        # Initialize location history for resampling
        if resample:
            self.resampler = Resampler()

        if ip and port:
            self.tcp_conn = self._setup_network(ip, port)

        if mport:
            start_http_server(mport)

    def do_resampled_prox_checks(self, gc_callback) -> list:
        """Complete resampling to detect proximity events between position
        updates."""
        if not self.enable_resample:
            logger.debug("Resampling is disabled")
            return None

        return self.resampler.do_prox_checks(self.rules, self.flights.bboxes,
                                             gc_callback=gc_callback)

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
            self.tcp_conn = TCPConnection()
            self.tcp_conn.test_fh = StringIO(string_data)
        else:
            self.data_iterator = iterator_data

        last_read_time = 0
        loop_ctr = 0

        while True:
            loop_ctr += 1
            last_read_return = self._flight_update_read()
            if last_read_return > 0:
                last_read_time = last_read_return
                logger.debug("Main loop last_read_time: %s", last_read_time)
                if loop_ctr % 1000 == 0:
                    logger.info("Main loop last_read_time: %s", last_read_time)

            if not self.flights.last_checkpoint:
                self.flights.last_checkpoint = last_read_time

            # Run a "Checkpoint".
            # Here we do periodic maintenance tasks, and expensive operations,
            # by default every 5 seconds.
            # Note: this will not fire during gaps when no aircraft are seen.
            # If timely expiration/maintenance is needed, dummy events can be
            # injected.
            time_for_forced_checkpoint = self.pedantic
            time_for_checkpoint = not self.pedantic and last_read_return > 0 and \
                last_read_time - self.flights.last_checkpoint >= CHECKPOINT_INTERVAL

            if (time_for_forced_checkpoint or time_for_checkpoint):
                datestr = datetime.datetime.utcfromtimestamp(
                    last_read_time).strftime('%Y-%m-%d %H:%M:%S')
                logger.debug("%ds Checkpoint: %d ops, %d callbacks, last_read_time %d %s",
                             CHECKPOINT_INTERVAL, Stats.json_readlines,
                             Stats.callbacks_fired, last_read_time, datestr)

                self.flights.expire_old(self.rules, last_read_time,
                                        self.expire_secs)
                self.rules.handle_proximity_conditions(self.flights, last_read_time)
                self.flights.last_checkpoint = last_read_time

            if last_read_return == 0:
                continue
            if last_read_return < 0:
                break

            if self.exit_loop_flag:
                logger.warning("Exiting AdsbActions loop")
                break

            if delay:
                time.sleep(delay)

        logger.warning("Exiting main loop, parsed %s points.", Stats.json_readlines)
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
        # Return None if there's no config section
        if 'config' not in yaml or 'kmls' not in yaml.get('config', {}):
            return None

        bboxes_list = []
        try:
            for f in yaml['config']['kmls']:
                bboxes_list.append(Bboxes(f))
        except FileNotFoundError:
            logger.critical("File mentioned in yaml not found: %s", f)
            sys.exit(-1)
        except Exception as e:
            logger.critical("Other error in yaml: " + str(e))
            sys.exit(-1)
        return bboxes_list

    def _setup_network(self, ipaddr : str, port : int,
                       retry_conn : bool = True):
        """Open network connection."""
        print("Connecting to %s:%d" % (ipaddr, int(port)))

        signal.signal(signal.SIGINT, sigint_handler)
        conn = TCPConnection(ipaddr, int(port), retry_conn)
        conn.connect()

        logger.info("Network setup done")
        return conn

    def _flight_update_read(self) -> float:
        """Attempt to read a line from the socket or other data source,
        and process it.

        Returns:
            a timestamp of the parsed location update if successful, 
            zero if not successful, -1 on EOF"""

        jsondict = None
        try:
            if self.tcp_conn:
                line = self.tcp_conn.readline()
                if not line:
                    raise IOError  # File EOF or socket closed
                jsondict = json.loads(line)
            else:
                jsondict = next(self.data_iterator)
        except json.JSONDecodeError:
            logger.error("_flight_update_read JSON Parse fail: %s", line)
        except StopIteration:
            logger.info("_flight_update_read: Data iterator exhausted (EOF)")
            return -1       # iterator EOF
        except Exception as e:   # pylint: disable=broad-except
            logger.debug("_flight_update_read: Exception occurred: %s", e)
            logger.debug("_flight_update_read: Traceback:\n%s",
                         traceback.format_exc())
            if self.tcp_conn and self.tcp_conn.retry:
                time.sleep(.2)          # avoid tight loop on error
                logger.debug(
                    "_flight_update_read Attempting reconnect... (tcp_conn=%s, retry=%s)", 
                    self.tcp_conn, self.tcp_conn.retry)
                self.tcp_conn.connect()
                logger.info("_flight_update_read Reconnected after disconnect/inactivity timeout")
                return 0
            else:
                logger.warning("_flight_update_read Exception: %s", e)
                return -1

        Stats.json_readlines += 1

        if jsondict and 'alt_baro' in jsondict:
            # We got some valid data, process it. (points with no altitude
            # are ignored, they are likely to be dummy entries anyway)
            loc_update = Location.from_dict(jsondict)

            if self.enable_resample:
                self.resampler.add_location(loc_update)

            return self.flights.add_location(loc_update, self.rules)
        elif jsondict:
            logger.debug("_flight_update_read: No alt_baro in jsondict for %s", jsondict)
        return 0


class TCPConnection:
    def __init__(self=None, host=None, port=None, retry=False):
        self.host = host
        self.port = port
        self.sock = None
        self.retry = retry

        self.test_fh = None     # file handle, set directly in test cases
        self.buffer = b""       # unprocessed socket data

    def connect(self):
        try:
            if self.sock: self.sock.close()     # reconnect case
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(60)
            print('Successful Connection')
        except Exception as e:
            print('Connection Failed: '+str(e))

    def readline(self):
        """Blocking read of a line from the socket or file handle."""
        if self.test_fh:
            return self.test_fh.readline()
        else:
            while True:
                line = self._readline_from_buffer()
                if line:
                    return line
                
                data = self.sock.recv(4096)
                if not data:
                    raise IOError  # File EOF or socket closed
                self.buffer += data

    def _readline_from_buffer(self):
        # Process all complete lines in the buffer
        if b'\n' in self.buffer:
            # Split at the first newline
            line, self.buffer = self.buffer.split(b'\n', 1)
            line = line.strip()
            # logger.info(f"Received line: {line.decode()}")
            return line
        return None

def sigint_handler(signum, frame):
    sys.exit(1)

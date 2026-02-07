"""This is the main API For the adsb_actions library.  It provides the entry 
point to process ADS-B data from a network socket or other source, apply rules, 
and execute actions.

The following code will instantiate the library, attempt to connect to a network
socket, and process the ADS-B data coming in:
    adsb_actions = AdsbActions(yaml_config, ip=args.ipaddr, port=args.port)
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

from prometheus_client import start_http_server

from .rules import Rules
from .flights import Flights
from .bboxes import Bboxes
from .stats import Stats
from .location import Location
from .resampler import Resampler
from .webhooks import register_webhook_handler
from .page import send_slack, send_page

from .adsb_logger import Logger

logger = logging.getLogger(__name__)
logger.level = logging.INFO
LOGGER = Logger()

def _log_loop_stats(last_read_time: float, flights: int,
                    rules, resampler, loop_ctr: int):
    """Log periodic loop statistics at INFO level, with extra detail at DEBUG."""
    # Track state for performance calculation
    if not hasattr(_log_loop_stats, 'last_wall'):
        _log_loop_stats.last_wall = time.monotonic()
        _log_loop_stats.last_ctr = 0

    now_wall = time.monotonic()
    elapsed = now_wall - _log_loop_stats.last_wall
    pts_per_sec = (loop_ctr - _log_loop_stats.last_ctr) / elapsed if elapsed > 0 else 0
    _log_loop_stats.last_wall = now_wall
    _log_loop_stats.last_ctr = loop_ctr

    time_str = datetime.datetime.utcfromtimestamp(last_read_time).strftime('%H:%MZ')
    logger.info("Main loop at: %s ts: %s flights=%d pts/s=%.0f",
                time_str, int(last_read_time), flights, pts_per_sec)
    if logger.isEnabledFor(logging.DEBUG):
        import gc
        gc_counts = gc.get_count()
        gc2_collections = gc.get_stats()[2]['collections']
        resampler_pts = (sum(len(v) for v in resampler.locations_by_flight_id.values())
                         if resampler else 0)
        logger.debug("  rules_log=%d emit_files=%d resampler_pts=%d loop_ctr=%d gc=%s gc2=%d",
                     len(rules.rule_execution_log.last_execution_time),
                     len(rules._emit_files),
                     resampler_pts, loop_ctr, gc_counts, gc2_collections)


class AdsbActions:
    """Main API for the library."""

    CHECKPOINT_INTERVAL = 5  # seconds. How often to do maintenance tasks.

    def __init__(self, yaml_data=None, yaml_file=None, ip=None, port=None,
                 mport=None, bboxes=None, expire_secs=180, pedantic=False,
                 resample=False, resample_bbox_filter=False,
                 use_optimizations=False):
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
            resample_bbox_filter: if True and resample is True, filter resampled
                data to only include points within configured bboxes. This is a
                memory optimization for large global datasets.
            use_optimizations: if True, enable performance optimizations for batch
                processing (bbox pre-computation + spatial grid indexing).
                Recommended for batch processing with many airport rules (e.g., shard pass).
        """

        assert yaml_data or yaml_file, "Must provide yaml or yaml_file"
        if yaml_file:
            with open(yaml_file, 'r', encoding='utf-8') as file:
                yaml_data = yaml.safe_load(file)

        self.flights = Flights(bboxes or self._load_bboxes(yaml_data),
                               ignore_unboxed_flights=not pedantic)
        self.rules = Rules(yaml_data, use_optimizations=use_optimizations)
        self.tcp_conn = None
        self.data_iterator = None
        self.exit_loop_flag = False      # set externally if we need to exit the main loop
        self.expire_secs = expire_secs
        self.pedantic = pedantic
        self.enable_resample = resample

        # Initialize location history for resampling
        # Pass bboxes/latlongrings to resampler for spatial filtering (memory optimization)
        # only if explicitly requested via resample_bbox_filter
        if resample:
            resampler_bboxes = self.flights.bboxes if resample_bbox_filter else None
            resampler_latlongrings = self._extract_latlongrings(yaml_data) if resample_bbox_filter else None
            self.resampler = Resampler(bboxes=resampler_bboxes,
                                       latlongrings=resampler_latlongrings)

        if ip and port:
            self.tcp_conn = self._setup_network(ip, port)

        if mport:
            start_http_server(mport)    # start prometheus metrics server

        # Register default webhook handlers
        register_webhook_handler('slack', send_slack)
        register_webhook_handler('page', send_page)

    def do_resampled_prox_checks(self, gc_callback) -> list:
        """Complete resampling to detect proximity events between position
        updates."""
        if not self.enable_resample:
            logger.debug("Resampling is disabled")
            return None

        return self.resampler.do_prox_checks(self.rules, self.flights.bboxes,
                                             ignore_unboxed_flights=self.flights.ignore_unboxed_flights,
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
        # Two ways to inject data for non-network cases:
        if string_data:
            self.tcp_conn = TCPConnection()
            self.tcp_conn.test_fh = StringIO(string_data)
        else:
            self.data_iterator = iterator_data

        last_read_time = 0
        last_expire_time = 0
        loop_ctr = 0

        while True:
            loop_ctr += 1
            last_read_return = self._flight_update_read()
            if last_read_return > 0:
                last_read_time = last_read_return
                logger.debug("Main loop periodic timestamp: %s", int(last_read_time))
                if loop_ctr % 25000 == 0:
                    _log_loop_stats(last_read_time, len(self.flights.flight_dict),
                                    self.rules,
                                    self.resampler if self.enable_resample else None,
                                    loop_ctr)

            if not self.flights.last_checkpoint:
                self.flights.last_checkpoint = last_read_time
                last_expire_time = last_read_time

            # Run a "Checkpoint".
            # Here we do periodic maintenance tasks, and expensive operations,
            # by default every 5 seconds.
            # Note: this will not fire during gaps when no aircraft are seen.
            # If timely expiration/maintenance is needed, dummy events can be
            # injected.
            time_for_forced_checkpoint = self.pedantic
            time_for_checkpoint = not self.pedantic and last_read_return > 0 and \
                last_read_time - self.flights.last_checkpoint >= self.CHECKPOINT_INTERVAL

            if (time_for_forced_checkpoint or time_for_checkpoint):
                datestr = datetime.datetime.utcfromtimestamp(
                    last_read_time).strftime('%Y-%m-%d %H:%M:%S')
                logger.debug("%ds Checkpoint: %d ops, %d callbacks, last_read_time %d %s",
                             self.CHECKPOINT_INTERVAL, Stats.json_readlines,
                             Stats.callbacks_fired, last_read_time, datestr)

                # expire_old is O(n) over all flights -- run at most once
                # per second of data-time since expire_secs is 180s.
                if last_read_time - last_expire_time >= 1:
                    self.flights.expire_old(self.rules, last_read_time,
                                            self.expire_secs)
                    last_expire_time = last_read_time
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

        logger.info("Exiting main loop, parsed %s points.", Stats.json_readlines)
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

    def _load_bboxes(self, yaml_arg: str) -> list[Bboxes]:
        """Load the kml files found in the yaml, and parse those kmls."""
        # Return None if there's no config section
        if 'config' not in yaml_arg or 'kmls' not in yaml_arg.get('config', {}):
            return None

        bboxes_list = []
        try:
            for f in yaml_arg['config']['kmls']:
                bboxes_list.append(Bboxes(f))
        except FileNotFoundError:
            logger.critical("File mentioned in yaml not found: %s", f)
            sys.exit(-1)
        except Exception as e:  # pylint: disable=broad-except
            logger.critical("Other error in yaml: %s", str(e))
            sys.exit(-1)
        return bboxes_list

    def _extract_latlongrings(self, yaml_data: dict) -> list:
        """Extract all latlongring conditions from rules for spatial filtering.

        Args:
            yaml_data: The parsed YAML configuration

        Returns:
            List of [radius_nm, lat, lon] tuples, or None if none found
        """
        if 'rules' not in yaml_data:
            return None

        latlongrings = []
        for rule_name, rule_body in yaml_data['rules'].items():
            if not isinstance(rule_body, dict):
                continue
            conditions = rule_body.get('conditions', {})
            if not isinstance(conditions, dict):
                continue
            if 'latlongring' in conditions:
                ring = conditions['latlongring']
                if isinstance(ring, list) and len(ring) == 3:
                    latlongrings.append(ring)
                    logger.debug("Extracted latlongring from rule %s: %s",
                                rule_name, ring)

        return latlongrings if latlongrings else None

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
                    raise IOError("File EOF or socket closed")
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
                logger.info("_flight_update_read Exception: %s", str(e))
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
        except Exception as e:    # pylint: disable=broad-except
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

def sigint_handler(_, __):
    sys.exit(1)

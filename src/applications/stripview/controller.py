"""Main module to start and build the UI.

Supports three data sources:
  1. Network socket (--ipaddr/--port): Live readsb data
  2. Test file (--testdata): Pre-recorded JSON for testing
  3. Public API (--api): Polls airplanes.live/adsb.one API (no hardware needed)
"""

import os
import signal
import threading
import argparse
import sys
import logging
import time
import json
import requests
import yaml
from adsb_actions.adsb_logger import Logger
from adsb_actions.bboxes import Bboxes
from adsb_actions.adsbactions import AdsbActions

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG
LOGGER = Logger()

# API configuration
#API_ENDPOINT = "https://api.adsb.one/v2/point/"
API_ENDPOINT = "https://api.airplanes.live/v2/point/"
API_RATE_LIMIT = 1/2  # requests per second (max 0.5 Hz)

os.environ['KIVY_LOG_MODE'] = 'PYTHON'  # inhibit Kivy's custom log format
import kivy
kivy.require('1.0.5')
from kivy.config import Config
Config.set('graphics', 'width', '540')
Config.set('graphics', 'height', '500')
from kivy.core.window import Window
from kivy.clock import Clock, mainthread
from kivy.uix.floatlayout import FloatLayout

from kivymd.app import MDApp
from flightstrip import FlightStrip

controllerapp = None


class APIPoller:
    """Polls the public ADS-B API and feeds data to adsb_actions."""

    def __init__(self, adsb_actions, lat, lon, radius_nm):
        self.adsb_actions = adsb_actions
        self.lat = lat
        self.lon = lon
        self.radius_nm = radius_nm
        self.running = True
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)

    def _poll_loop(self):
        """Main polling loop - fetches from API and feeds to adsb_actions."""
        while self.running:
            start_time = time.time()

            try:
                self._fetch_and_process()
            except Exception as e:
                logger.error(f"Error in API poll: {e}")

            # Rate limit
            elapsed = time.time() - start_time
            sleep_time = (1 / API_RATE_LIMIT) - elapsed
            if sleep_time > 0 and self.running:
                time.sleep(sleep_time)

    def _fetch_and_process(self):
        """Fetch data from API and process through adsb_actions."""
        url = f"{API_ENDPOINT}{self.lat}/{self.lon}/{self.radius_nm}"

        response = requests.get(url, timeout=10)
        json_data = response.json()

        ac_count = len(json_data.get('ac', []))
        logger.info(f"API returned {ac_count} aircraft")

        if ac_count == 0:
            return

        # Convert API format to newline-delimited JSON
        json_list = ""
        for line in json_data['ac']:
            line['now'] = json_data['now'] / 1000
            json_list += json.dumps(line) + "\n"

        # Feed to adsb_actions
        if json_list:
            self.adsb_actions.loop(string_data=json_list)


class ControllerApp(MDApp):
    def __init__(self, bboxes, focus_q, admin_q):
        logger.debug("controller init")

        self.strips = {}    # dict of FlightStrips by id
        self.MAX_SCROLLVIEWS = 4
        self.bboxes = bboxes
        self.focus_q = focus_q
        self.admin_q = admin_q

        super().__init__()

    def build(self):
        logger.debug("controller build")

        self.controller = Controller()
        self.theme_cls.theme_style="Dark"
        self.setup_titles()
        logger.debug("controller build done")
        return self.controller

    def register_close_callback(self, close_callback):
        """Callback for when the user tries to close the window."""
        Window.bind(on_request_close=close_callback)

    def get_title_button_by_index(self, index):
        title_id = "title_%d" % index
        return self.controller.ids[title_id]

    def setup_titles(self):
        """Set GUI title bars according to bbox/KML titles"""
        for i, bbox in enumerate(self.bboxes.boxes):
            title_button = self.get_title_button_by_index(i)
            title_button.text = bbox.name
            if i >= self.MAX_SCROLLVIEWS - 1:
                return

    @mainthread
    def update_strip(self, flight):
        """ Called on bbox change. """

        new_scrollview_index = flight.inside_bboxes_indices[0]
        # maybe have a redundant indside_bboxes_indexes?
        id = flight.flight_id

        if id in self.strips:
            # updating existing strip
            strip = self.strips[id]
            strip.update(flight, flight.lastloc, flight.inside_bboxes)
            if new_scrollview_index is None and strip.scrollview_index >= 0:
                # No longer in a tracked region.
                # Don't move strip but continue to update indefinitely
                # XXX probably not right behavior for everyone
                return

            if strip.scrollview_index != new_scrollview_index:
                # move strip to new scrollview
                logger.debug(f"UI index CHANGE to {new_scrollview_index}")
                strip.unrender()
                strip.scrollview_index = new_scrollview_index
                strip.render()
        else:
            if new_scrollview_index is None:
                return # not in a tracked region now, don't add it

            # location is inside one of our tracked regions, add new strip
            strip = FlightStrip(new_scrollview_index, self, flight,
                self.focus_q, self.admin_q)
            strip.update(flight, flight.lastloc, flight.all_bboxes_list)
            strip.render()
            strip.set_highlight()

            self.strips[id] = strip

    @mainthread
    def remove_strip(self, flight):
        try:
            strip = self.strips[flight.flight_id]
        except KeyError:
            return
        logger.info("Removing strip for %s" % flight.flight_id)
        strip.unrender()
        strip.stop_server_loop()
        del self.strips[flight.flight_id]

    @mainthread
    def annotate_strip(self, flight, flight2):
        """Change the color and text of the strip for extra attention"""

        logger.debug("annotate strip %s", flight.flight_id)
        id = flight.flight_id
        try:
            strip = self.strips[id]
        except KeyError:
            logger.debug("annotate not found")
            return
        note = "TRAFFIC ALERT"
        strip.annotate(note)
        strip.update_strip_text()

    @mainthread
    def set_strip_color(self, strip_id, color):
        """Actually write the strip color to the screen."""
        try:
            strip = self.strips[strip_id]
        except KeyError:
            return
        strip.background_color = color

class Controller(FloatLayout):
    """Placeholder for controller.kv to be loaded into."""
    def do_add_click(self, n):
        logger.debug("add click %d" % n)

def sigint_handler(signum, frame):
    exit(1)

def shutdown_adsb_actions(_, adsb_actions, data_thread, api_poller=None):
    logger.warning("Shutting down adsb_actions")

    if api_poller:
        api_poller.stop()
    adsb_actions.exit_loop_flag = True
    if data_thread:
        data_thread.join()

    logger.warning("adsb_actions shutdown complete")
    sys.exit(0)


def setup(focus_q, admin_q):
    logger.info('System started.')

    parser = argparse.ArgumentParser(
        description="Render a rack of aircraft status strips.",
        epilog="Data source: specify ONE of --ipaddr/--port, --testdata, or --api")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument('--ipaddr', help="IP address to connect to (readsb)")
    parser.add_argument('--port', help="port to connect to (readsb)")
    parser.add_argument('-m', '--mport', type=int, help="metrics port to listen on", default='9108')
    parser.add_argument('--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")
    parser.add_argument('--delay', help="Seconds of delay between reads, for testing", default=0)
    parser.add_argument('--api', action='store_true',
                        help="Use public API - requires latlongring in rules")
    args = parser.parse_args()

    # Validate mutually exclusive data source options
    sources = sum([
        bool(args.ipaddr and args.port),
        bool(args.testdata),
        bool(args.api)
    ])
    if sources != 1:
        logger.fatal("Exactly one data source required: --ipaddr/--port, --testdata, or --api")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    # Load YAML
    with open(args.rules, 'r') as f:
        yaml_data = yaml.safe_load(f)

    api_poller = None
    read_thread = None
    json_data = None

    # Setup flight data handling based on source
    if args.api:
        # API mode - find latlongring condition in rules (consistent with tcp_api_monitor)
        latlongring = None
        for rulename, rulebody in yaml_data.get('rules', {}).items():
            conditions = rulebody.get('conditions', {})
            if 'latlongring' in conditions:
                latlongring = conditions['latlongring']
                logger.info(f"Using latlongring from rule '{rulename}'")
                break

        if not latlongring:
            logger.fatal("--api requires a rule with 'latlongring' condition: [radius_nm, lat, lon]")
            sys.exit(1)

        radius_nm, lat, lon = latlongring
        logger.info(f"API mode: querying {lat}, {lon} radius {radius_nm}nm")
        adsb_actions = AdsbActions(yaml_data=yaml_data, expire_secs=120)
        api_poller = APIPoller(adsb_actions, lat, lon, radius_nm)

    elif args.ipaddr and args.port:
        # Network mode
        adsb_actions = AdsbActions(
            yaml_file=args.rules, ip=args.ipaddr, port=args.port,
            expire_secs=120, mport=args.mport)

    else:
        # Test data mode
        adsb_actions = AdsbActions(yaml_file=args.rules)
        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()

    # Load KML files from YAML config to define and label the strip racks.
    # First KML specifies which window to show each strip in,
    # second KML provides more detailed location strings for the UI.
    kml_files = adsb_actions.rules.yaml_data['config']['kmls']
    assert len(kml_files) == 2, \
        "2 kmls expected in yaml: first specifies window placement, second provides location strings"
    bboxes_list = [Bboxes(f) for f in kml_files]

    # UI setup
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGQUIT, sigint_handler)

    global controllerapp
    controllerapp = ControllerApp(bboxes_list[0], focus_q, admin_q)
    assert len(bboxes_list[0].boxes) == 4, \
        "4 racks expected in first kml"  # TODO: Obviously could be generalized

    adsb_actions.register_callback(
        "aircraft_update_cb", controllerapp.update_strip)
    adsb_actions.register_callback(
        "aircraft_remove_cb", controllerapp.remove_strip)
    adsb_actions.register_callback(
        "los_update_cb", controllerapp.annotate_strip)

    # Setup data thread (not used for API mode)
    if not args.api:
        read_thread = threading.Thread(target=adsb_actions.loop,
            kwargs={'string_data': json_data, 'delay': float(args.delay)})

    # Handling for orderly exit when the user closes the window manually.
    close_callback = lambda controller, actions=adsb_actions, thread=read_thread, poller=api_poller: \
        shutdown_adsb_actions(controller, actions, thread, poller)
    controllerapp.register_close_callback(close_callback)

    # Don't update the UI before it's drawn...
    if args.api:
        Clock.schedule_once(lambda x: api_poller.start(), 2)
    else:
        Clock.schedule_once(lambda x: read_thread.start(), 2)

    # TODO probably cleaner to put this method+state in a class.
    # we need to return both, derivative UIs will want to play
    # with adsb_actions, and you also need to return controllerapp
    # to start it running.
    return (adsb_actions, controllerapp)

if __name__ == '__main__':
    _, app = setup(None, None)
    app.run()

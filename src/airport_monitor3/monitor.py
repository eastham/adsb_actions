"""Simple flight status board.  Supports a fixed-width font"""

import argparse
import threading
import sys
import logging
import os
import signal
import yaml

os.environ['KIVY_LOG_MODE'] = 'PYTHON'  # inhibit Kivy's custom log format
import kivy
from kivy.app import App
from kivy.uix.textinput import TextInput
from kivy.clock import Clock, mainthread
from kivy.utils import escape_markup
from kivy.core.window import Window
from kivy.metrics import dp

from adsb_actions.bboxes import Bboxes
from adsb_actions.flight import Flight
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger

from db import appsheet_api

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

from kivy.uix.image import Image
from kivy.uix.boxlayout import BoxLayout

from kivy.uix.floatlayout import FloatLayout
import time

class Monitor(App):
    def __init__(self, text_position, adsb_actions, **kwargs):
        super().__init__(**kwargs)
        self.text_position = (dp(text_position[0]), dp(text_position[1]))
        self.adsb_actions = adsb_actions
        self.text_input = None
        self.image = None
        self.flight_name_cache = {}
        self.appsheet_api = appsheet_api.Appsheet()
        self.last_read = 0

    def build(self):
        self.text_input = TextInput(multiline=True, font_size=dp(20), 
                                    pos=self.text_position)

        layout = FloatLayout()
        layout.add_widget(self.text_input)

        # setup sizes and positions once the window is rendered
        layout.bind(size=self.on_size, pos=self.on_pos)

        return layout

    def on_size(self, instance, value):
        self.text_input.size = value
        if self.image:
            self.image.size = value

    def on_pos(self, instance, value):
        self.text_input.pos = value
        if self.image:
            self.image.pos = value

    @mainthread
    def update_text(self, text):
        self.text_input.text = text

    def format_row(self, flt, tail, loc, alt):
        return f'{flt: <15} {tail: <10} {loc: <15} {alt: <7}\n'

    def flight_db_lookup(self, flight_id):
        logger.debug("Looking up pilot for flight %s", flight_id)
        try:
            aircraft_obj = self.appsheet_api.aircraft_lookup(flight_id, True)
            pilot = self.appsheet_api.pilot_lookup(aircraft_obj['lead pilot'])
            name = pilot.get('Playa name')
            if not name or name == "":
                return False
            name = name[:12]

            if pilot.get('Scenics'):
                flightnum = int(pilot.get('Scenics')) + 1
            else:
                flightnum = 1
            name +=  " " + str(flightnum)
        except Exception as e:      #   pylint: disable=broad-except
            logger.error("Error looking up pilot for flight %s: %s", flight_id, e)
            return False
        return name

    def search_for_pilot(self, flight):
        """Search for the pilot name associated with a flight, specified in
        the YAML file.  Cache to avoid repeated lookups."""

        flight_id = flight.flight_id
        name = None

        # check for cached db info
        pilot_flight = flight.flags.get('pilot_flight')
        if pilot_flight:
            return pilot_flight

        if pilot_flight is None:
            # haven't yet attempted db lookup -- False means prior lookup failed
            name = self.flight_db_lookup(flight_id)
            flight.flags['pilot_flight'] = name

        # pretty ugly...N/A is coming from the flight constructor I think
        if not name or name.startswith("N/A"):
            name = flight.other_id
            if not name or name == "N/A":
                name = flight.flight_id

        logger.debug("Using name %s for %s", name, flight_id)
        return name

    def get_text_for_flight(self, flight):
        pilot_name = self.search_for_pilot(flight)
        try:
            location = flight.inside_bboxes[1].strip()
        except:         # pylint: disable=bare-except
            location = "--"

        alt_str = (str(flight.lastloc.alt_baro) + " " +
            flight.get_alt_change_str(flight.lastloc.alt_baro))

        return self.format_row(pilot_name, flight.flight_id, location,
                               alt_str)

    def get_text_for_index(self, title, index):
        text = '            ' + title + '\n\n\n'
        text += self.format_row("FLIGHT", "TAIL #", "LOCATION", "ALT")
        text += '\n\n'

        for flight in self.adsb_actions.flights:
            if flight.inside_bboxes_indices[0] == index:
                text += self.get_text_for_flight(flight)

        #text += '\n\n'
        return text

    def update_display(self, flight):
        """ Called on bbox change.  Not very smart, it just regenerates the whole
        display.  Could be optimized."""

        if flight:
            self.last_read = flight.lastloc.now
        if self.last_read:
            timestr = time.strftime('%a %H:%M', time.gmtime(self.last_read))
        else:
            timestr = "..."
        text = f"       88NV active flights as of {timestr}\n\n\n"
        text += self.get_text_for_index("  === Departing ===", 1) + '\n\n'
        text += self.get_text_for_index("=== Scenic Pattern ===", 0) + '\n\n'
        text += self.get_text_for_index("  === Arriving ===", 2) + '\n\n'

        self.update_text(text)

    def update_cb(self, flight):
        """This is the callback fired on change"""

        logger.info("Update callback for flight %s",
                    self.get_text_for_flight(flight).strip() if flight else "None")
        self.update_display(flight)

    def expire(self, flight):
        """Callback fired when an aircraft is removed from the system."""
        logger.info("Expire callback for flight %s",
                    self.get_text_for_flight(flight).strip() if flight else "None")

        self.update_display(None)

def sigint_handler(signum, frame):
    exit(1)

def parseargs():
    parser = argparse.ArgumentParser(
        description="render a simple flight status board.")
    parser.add_argument('--ipaddr', help="IP address to connect to")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument(
        '--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")
    parser.add_argument('--delay', help="Seconds of delay between reads, for testing", 
                        default=0)
    args = parser.parse_args()

    if not bool(args.testdata) != bool(args.ipaddr and args.port):
        logger.fatal("Either ipaddr/port OR testdata must be provided.")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    return args

def setup():
    logger.info('System started.')

    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGQUIT, sigint_handler)

    args = parseargs()

    with open(args.rules, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    # Setup flight data handling.
    json_data = None

    if not args.testdata:
        adsb_actions = AdsbActions(yaml_data, ip=args.ipaddr, port=args.port)
    else:
        adsb_actions = AdsbActions(yaml_data)

        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()

    # Actually build and start the app
    dp_window_size = [dp(i) for i in yaml_data['monitor_config']['window_size']]
    Window.size = dp_window_size
    # Window.top = dp(yaml_data['monitor_config']['window_top'])
    # Window.left = dp(yaml_data['monitor_config']['window_left'])
    Window.clearcolor = (0, 0, 1, 1)
    monitorapp = Monitor(yaml_data['monitor_config']['text_position'],
                         adsb_actions)

    adsb_actions.register_callback(
        "aircraft_update_cb", monitorapp.update_cb)
    adsb_actions.register_callback(
        "aircraft_expire_cb", monitorapp.expire)

    read_thread = threading.Thread(target=adsb_actions.loop,
        kwargs={'string_data': json_data, 'delay': float(args.delay)})

    # Don't update the UI before it's drawn...
    Clock.schedule_once(lambda x: read_thread.start(), 2)
    Clock.schedule_once(lambda x: monitorapp.update_cb(None), 2)

    monitorapp.run()

if __name__ == '__main__':
    setup()

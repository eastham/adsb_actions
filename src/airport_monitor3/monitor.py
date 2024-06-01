"""Simple flight status board.  Supports a fixed-width font"""

import argparse
import logging
import threading
import sys
import yaml
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
from db import appsheet_api

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG
adsb_actions = None

from kivy.uix.image import Image
from kivy.uix.boxlayout import BoxLayout

from kivy.uix.floatlayout import FloatLayout
import time

class Monitor(App):
    def __init__(self, text_position, **kwargs):
        super().__init__(**kwargs)
        self.text_input = None
        self.image = None
        self.text_position = (dp(text_position[0]), dp(text_position[1]))
        self.flight_name_cache = {}
        self.appsheet_api = appsheet_api.Appsheet()

    def build(self):
        self.text_input = TextInput(multiline=True, pos=self.text_position)

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

    def update_text(self, text):
        self.text_input.text = text

    def format_row(self, arg1, arg2, arg3):
        return f'{arg1: <15} {arg2: <12} {arg3: <15}\n'

    def search_for_pilot(self, flight):
        """Search for the pilot name associated with a flight, specified in
        the YAML file.  Cache to avoid repeated lookups."""

        flight_id = flight.flight_id
        name = None
        pilot_name = self.flight_name_cache.get(flight_id)
        if pilot_name:
            return pilot_name

        if pilot_name is False:
            # not yet attempted db lookup -- None means no record found
            try:
                logging.debug("Looking up pilot for %s", flight_id)
                aircraft_obj = self.appsheet_api.aircraft_lookup(flight_id, True)
                pilot = self.appsheet_api.pilot_lookup(aircraft_obj['lead pilot'])
                name = pilot.get('Public name')
                self.flight_name_cache[flight_id] = name
            except Exception:      #   pylint: disable=broad-except
                logging.debug("No lead pilot for %s", flight_id)
                self.flight_name_cache[flight_id] = None

        # pretty ugly...N/A is coming from the flight constructor I think
        if not name or name == "N/A":
            name = flight.other_id
            if not name or name == "N/A":
                name = flight.flight_id

        logging.debug("Using name %s for %s", name, flight_id)
        return name
        
    def get_text_for_flight(self, flight):
        pilot_name = self.search_for_pilot(flight)
        try:
            location = flight.inside_bboxes[1].strip()
        except:
            location = "--"
        return self.format_row(pilot_name, flight.flight_id, location)

    def get_text_for_index(self, title, index):
        text = '            ' + title + '\n\n\n'
        text += self.format_row("PILOT/FLIGHT", "TAIL #", "LOCATION")
        text += '\n\n'

        for flight in adsb_actions.flights:
            if flight.inside_bboxes_indices[0] == index:
                text += self.get_text_for_flight(flight)

        text += '\n\n'
        return text

    @mainthread
    def update_display(self, flight):
        """ Called on bbox change.  Not very smart, it just regenerates the whole
        display.  Could be optimized."""

        timestr = "..."
        if flight:
            time_secs = flight.lastloc.now
            timestr = time.strftime('%a %H:%M', time.gmtime(time_secs))
        text = f"       88NV active flights as of {timestr}\n\n\n"
        text += self.get_text_for_index("=== Scenic Flights ===", 0) + '\n\n'
        text += self.get_text_for_index("=== Arrivals ===", 2) + '\n\n'
        text += self.get_text_for_index("=== Departures ===", 1) + '\n\n'

        self.update_text(text)

    def inside_bbox(self, flight):
        logger.debug("inside_bbox for flight %s", flight.flight_id if flight else "None")
        """This is the callback fired on change"""
        self.update_display(flight)

    def expire(self, flight):
        logger.debug("expire for flight %s", flight.flight_id)
        self.update_display(None)

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
    logging.basicConfig(
        format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('System started.')

    args = parseargs()

    with open(args.rules, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    # Setup flight data handling.
    json_data = None
    global adsb_actions

    if not args.testdata:
        adsb_actions = AdsbActions(yaml_data, ip=args.ipaddr, port=args.port)
    else:
        adsb_actions = AdsbActions(yaml_data)

        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()

    # Actually build and start the app
    dp_window_size = [dp(i) for i in yaml_data['monitor_config']['window_size']]
    Window.size = dp_window_size
    Window.top = dp(yaml_data['monitor_config']['window_top'])
    Window.left = dp(yaml_data['monitor_config']['window_left'])

    monitorapp = Monitor(yaml_data['monitor_config']['text_position'])

    adsb_actions.register_callback(
        "aircraft_update_cb", monitorapp.inside_bbox)
    adsb_actions.register_callback(
        "aircraft_expire_cb", monitorapp.expire)

    read_thread = threading.Thread(target=adsb_actions.loop,
        kwargs={'string_data': json_data, 'delay': float(args.delay)})

    # Don't update the UI before it's drawn...
    Clock.schedule_once(lambda x: read_thread.start(), 2)
    Clock.schedule_once(lambda x: monitorapp.inside_bbox(None), 2)

    monitorapp.run()

if __name__ == '__main__':
    setup()

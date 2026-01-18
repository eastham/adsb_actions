"""Legacy airport monitor 1 - Simple flight status board using Kivy.

A basic flight information display with fixed-width font support and optional
TV-frame surround image. Shows aircraft grouped by Scenic/Arrivals/Departures
sections with pilot name lookups from YAML configuration.

This is a legacy example; see tools/stripview for the current implementation.

Usage: python monitor.py --rules monitor.yaml [--ipaddr IP --port PORT | --testdata FILE]
"""

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
from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

adsb_actions = None

from kivy.uix.image import Image
from kivy.uix.boxlayout import BoxLayout

from kivy.uix.floatlayout import FloatLayout

class Monitor(App):
    def __init__(self, surround_image_fn, text_position, default_pilot_name,
                 pilot_names, **kwargs):
        super().__init__(**kwargs)
        self.text_input = None
        self.image = None
        self.surround_image_fn = surround_image_fn
        self.text_position = (dp(text_position[0]), dp(text_position[1]))
        self.default_pilot_name = default_pilot_name
        self.pilot_names = pilot_names

    def build(self):
        self.text_input = TextInput(multiline=True, pos=self.text_position)

        layout = FloatLayout()
        layout.add_widget(self.text_input)

        if self.surround_image_fn:
            self.image = Image(source=self.surround_image_fn, fit_mode='fill')
            layout.add_widget(self.image)

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

    def search_for_pilot(self, flight_id):
        """Search for the pilot name associated with a flight, specified in
        the YAML file."""
        for pilot in self.pilot_names:
            if flight_id == pilot.get('tail'):
                return pilot['pilot']
        return self.default_pilot_name

    def get_text_for_flight(self, flight):
        pilot_name = self.default_pilot_name
        if self.pilot_names:
            pilot_name = self.search_for_pilot(flight.flight_id)
        return self.format_row(pilot_name, flight.flight_id, flight.inside_bboxes[1].strip())

    def get_text_for_index(self, title, index):
        text = '            ' + title + '\n\n'
        text += self.format_row("PILOT", "TAIL #", "LOCATION")
        text += '\n'

        for flight in adsb_actions.flights:
            if flight.inside_bboxes_indices[0] == index:
                text += self.get_text_for_flight(flight)
        text += '\n\n'
        return text

    @mainthread
    def update_display(self):
        """ Called on bbox change. """

        text = 'Welcome to 88NV -- "serving ample delays since 2000" \n\n'
        text += self.get_text_for_index("=== Scenic Flights ===", 0) + '\n\n'
        text += self.get_text_for_index("=== Arrivals ===", 2) + '\n\n'
        text += self.get_text_for_index("=== Departures ===", 1) + '\n\n'

        self.update_text(text)

    def handle_change(self, _):
        """This is the callback fired on change"""
        self.update_display()

def parseargs():
    parser = argparse.ArgumentParser(
        description="render a simple flight status board.")
    parser.add_argument('--ipaddr', help="IP address to connect to")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument(
        '--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")
    parser.add_argument('--delay', help="Seconds of delay between reads, for testing", default=0)
    args = parser.parse_args()

    if not bool(args.testdata) != bool(args.ipaddr and args.port):
        logger.fatal("Either ipaddr/port OR testdata must be provided.")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    return args

def setup():
    logger.info('System started.')

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

    monitorapp = Monitor(yaml_data['monitor_config']['surround_image'],
                         yaml_data['monitor_config']['text_position'],
                         yaml_data['default_pilot_name'],
                         yaml_data['pilot_names'])

    adsb_actions.register_callback(
        "aircraft_update_cb", monitorapp.handle_change)

    read_thread = threading.Thread(target=adsb_actions.loop,
        kwargs={'string_data': json_data, 'delay': float(args.delay)})

    # Don't update the UI before it's drawn...
    Clock.schedule_once(lambda x: read_thread.start(), 2)
    monitorapp.run()

if __name__ == '__main__':
    setup()

"""Legacy airport monitor 2 - Fancier airport-terminal-style flight display.

An improved flight information display with variable-width font support and
per-line colors. Shows Scenic/Arrivals/Departures in a grid layout with
alternating row colors for better readability.

This is a legacy example; see tools/stripview for the current implementation.

Usage: python monitorapp.py <kml_files> --rules monitor.yaml [--ipaddr IP --port PORT | --testdata FILE]
"""

import argparse
import logging
import threading
import yaml
import sys
from kivy.app import App
from kivy.uix.textinput import TextInput
from kivy.clock import Clock, mainthread
from kivy.utils import escape_markup
from kivy.core.window import Window
Window.size = (800, 600)

from adsb_actions.bboxes import Bboxes
from adsb_actions.flight import Flight
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger

from flightmonitor import FlightMonitor

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

adsb_actions = None
flightmonitor = None

def handle_change(_):
    """Redraw all data rows on the page."""
    cursor_arr = [0] * flightmonitor.NUM_SECTIONS
    rows_psec = flightmonitor.ROWS_PER_SECTION

    for flight in adsb_actions.flights:
        # print(f'{flight.flight_id}: {flight.inside_bboxes_indices}')

        # rack number = subsection of the display for each group of flights
        rack = flight.inside_bboxes_indices[0]
        if rack == None:
            continue

        # swap arrivals and departures to align with kml
        if rack == 1: rack = 2
        elif rack == 2: rack = 1

        if 0 <= rack < flightmonitor.NUM_SECTIONS:
            if cursor_arr[rack] >= rows_psec:
                # rack is full
                continue
            flightmonitor.change_button_text(
                rack * rows_psec + cursor_arr[rack],
                ('BRC1', flight.flight_id, flight.inside_bboxes[1].strip()))
            cursor_arr[rack] += 1

    # clear out any remaining rows we didn't update yet
    for i in range(flightmonitor.NUM_SECTIONS):
        for j in range(cursor_arr[i], rows_psec):
            flightmonitor.change_button_text(i * rows_psec + j,
                                             ('', '', ''))

class MonitorApp(App):
    def build(self):
        global flightmonitor
        flightmonitor = FlightMonitor()
        return flightmonitor

def parseargs():
    parser = argparse.ArgumentParser(
        description="render a simple flight status board.")
    parser.add_argument('file', nargs='+', help="kml files to use")
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
    monitorapp = MonitorApp()

    adsb_actions.register_callback(
        "aircraft_update_cb", handle_change)

    read_thread = threading.Thread(target=adsb_actions.loop,
        kwargs={'string_data': json_data, 'delay': float(args.delay)})

    # Don't update the UI before it's drawn...
    Clock.schedule_once(lambda x: read_thread.start(), 2)
    monitorapp.run()

if __name__ == '__main__':
    setup()

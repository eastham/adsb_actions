"""Main module to start and build the UI."""

from kivy.clock import Clock, mainthread
from kivy.uix.floatlayout import FloatLayout
import signal
import threading
import argparse
import sys
import logging
import yaml

sys.path.insert(0, '../adsb_actions')
from bboxes import Bboxes
from flight import Flight
from adsbactions import AdsbActions
from flightstrip import FlightStrip

import kivy
kivy.require('1.0.5')
from kivy.config import Config
Config.set('graphics', 'width', '600')
Config.set('graphics', 'height', '800')

from kivymd.app import MDApp
from dialog import Dialog

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

controllerapp = None

class ControllerApp(MDApp):
    def __init__(self, bboxes, focus_q, admin_q):
        logging.debug("controller init")

        self.strips = {}    # dict of FlightStrips by id
        self.dialog = None
        self.MAX_SCROLLVIEWS = 4
        self.bboxes = bboxes
        self.focus_q = focus_q
        self.admin_q = admin_q

        super().__init__()

    def build(self):
        logging.debug("controller build")

        self.controller = Controller()
        self.dialog = Dialog()
        self.theme_cls.theme_style="Dark"
        self.setup_titles()
        logging.debug("controller build done")
        return self.controller

    def get_title_button_by_index(self, index):
        title_id = "title_%d" % index
        return self.controller.ids[title_id]

    def setup_titles(self):
        """Set GUI title bars according to bbox/KML titles"""
        for i, bbox in enumerate(self.bboxes.boxes):
            title_button = self.get_title_button_by_index(i)
            title_button.text = bbox.name
            if i >= self.MAX_SCROLLVIEWS-1: return

    @mainthread
    def update_strip(self, flight):
        """ Called on bbox change. """
        new_scrollview_index = flight.inside_bboxes_indices[0]
        # maybe have a redundant indside_bboxes_indexes?
        id = flight.flight_id

        if id in self.strips:
            # updating exsiting strip
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
        logging.debug("removing flight %s" % flight.flight_id)
        strip.unrender()
        strip.stop_server_loop()
        del self.strips[flight.flight_id]

    @mainthread
    def annotate_strip(self, flight, flight2):
        """Change the color and text of the strip for extra attention"""
        logging.debug("annotate strip %s", flight.flight_id)
        id = flight.flight_id
        try:
            strip = self.strips[id]
        except KeyError:
            logging.debug("annotate not found")
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
        logging.debug("add click %d" % n)

def sigint_handler(signum, frame):
    exit(1)

def setup(focus_q, admin_q):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('System started.')

    parser = argparse.ArgumentParser(description="render a rack of aircraft status strips.")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('file', nargs='+', help="kml files to use")
    parser.add_argument('--ipaddr', help="IP address to connect to")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument('--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")
    parser.add_argument('--delay', help="Seconds of delay between reads, for testing", default=0)
    args = parser.parse_args()

    if not bool(args.testdata) != bool(args.ipaddr and args.port):
        logger.fatal("Either ipaddr/port OR testdata must be provided.")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    # Load state needed to define and label the 4 strip racks.
    bboxes_list = [] 
    for f in args.file:
        bboxes_list.append(Bboxes(f)) # describes the 4 racks

    # UI setup
    signal.signal(signal.SIGINT, sigint_handler)
    global controllerapp
    controllerapp = ControllerApp(bboxes_list[0], focus_q, admin_q)

    # Setup flight data handling.
    json_data = None
    if not args.testdata:
        adsb_actions = AdsbActions(
            yaml_file=args.rules, ip=args.ipaddr, port=args.port)
    else:
        adsb_actions = AdsbActions(yaml_file=args.rules)

        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()

    adsb_actions.register_callback(
        "aircraft_update_cb", controllerapp.update_strip)
    adsb_actions.register_callback(
        "aircraft_remove_cb", controllerapp.remove_strip)
    adsb_actions.register_callback(
        "abe_update_cb", controllerapp.annotate_strip)

    read_thread = threading.Thread(target=adsb_actions.loop,
        kwargs={'string_data': json_data, 'delay': float(args.delay)})

    # Don't update the UI before it's drawn...
    Clock.schedule_once(lambda x: read_thread.start(), 2)

    # TODO probably cleaner to put this method+state in a class.
    # we need to return both, derivative UIs will want to play
    # with adsb_actions, and you also need to return controllerapp
    # to start it running.
    return (adsb_actions, controllerapp)

if __name__ == '__main__':
    _, app = setup(None, None)
    app.run()

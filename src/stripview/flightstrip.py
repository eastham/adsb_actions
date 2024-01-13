"""Representation of a single flight strip in the UI, handling
colors, text, and mouse actions."""

import kivy
kivy.require('1.0.5')
from kivy.config import Config
Config.set('graphics', 'width', '600')
Config.set('graphics', 'height', '800')
from kivy.clock import Clock
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.button import Button

import threading
import time
import webbrowser

import sys
sys.path.insert(0, '../adsb_actions')

import logging

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

SERVER_REFRESH_RATE = 60 # seconds

USE_APPSHEET = False
if USE_APPSHEET:
    import appsheet_api # TODO
    appsheet = appsheet_api.Appsheet()
else:
    appsheet = None

class Controller(FloatLayout):
    def do_add_click(self, n):
        logging.debug("add click %d" % n)

class FlightStrip:
    def __init__(self, index, app, flight, id, tail, focus_q, admin_q):
        self.scrollview_index = index
        self.app = app
        self.flight = flight
        self.id = id # redundant to flight?
        self.tail = tail# redundant to flight?
        self.focus_q = focus_q
        self.admin_q = admin_q
        self.bg_color_warn = False
        self.update_thread = None
        self.stop_event = threading.Event()

        self.top_string = None
        self.note_string = ""
        self.alt_string = ""
        self.loc_string = ""
        self.deanno_event = None

        self.layout = GridLayout(cols=2, row_default_height=150, height=150, size_hint_y=None)
        self.main_button = Button(size_hint_x=None, padding=(10,10),
            text_size=(500,150), width=500, height=225, halign="left",
            valign="top", markup=True, on_release=self.main_button_click)

        self.right_layout = GridLayout(rows=3, row_default_height=50)

        self.admin_button = Button(text='Open', size_hint_x=None, width=100,
            on_release=self.admin_click)
        self.focus_button = Button(text='Focus', size_hint_x=None, width=100,
            on_release=self.focus_click)
        self.web_button = Button(text='Web', size_hint_x=None, width=100,
            on_release=self.web_click)

        self.layout.add_widget(self.main_button)
        self.layout.add_widget(self.right_layout)
        self.right_layout.add_widget(self.admin_button)
        self.right_layout.add_widget(self.focus_button)
        self.right_layout.add_widget(self.web_button)
        self.main_button.background_normal = ''  # colors don't render right without this
        self.update_thread = threading.Thread(target=self.server_refresh_thread, args=[flight])
        self.update_thread.start()

    def __del__(self):
        logging.debug(f"Deleting strip {self.id}")

    def render(self):
        """put the strip on the screen according to its current state"""
        self.get_scrollview().add_widget(self.layout, index=100)

    def unrender(self):
        """Hide the strip"""
        self.get_scrollview().remove_widget(self.layout)

    def update_strip_text(self):
        self.main_button.text = (self.top_string + " " + self.loc_string +
            "\n" + self.alt_string + "\n" + self.note_string)

    def get_scrollview(self):
        """Return the name for the scrollview in which this strip should live"""
        scrollbox_name = "scroll_%d" % self.scrollview_index
        return self.app.controller.ids[scrollbox_name].children[0]

    def main_button_click(self, arg):
        pass

    def admin_click(self, arg):
        if 'Row ID' not in self.flight.flags:
            self.do_server_update(self.flight) # hopefully sets row id

        if 'Row ID' in self.flight.flags:
            if self.admin_q: self.admin_q.put(self.flight.flags['Row ID'])
        return

    def web_click(self, arg):
        webbrowser.open("https://flightaware.com/live/flight/" + self.id)

    def focus_click(self, arg):
        logging.debug("focus " + self.id)
        if self.focus_q: self.focus_q.put(self.id)

    def stop_server_loop(self):
        logging.debug("stop_server_loop, thread " + str(self.update_thread))
        self.stop_event.set()

    def do_server_update(self, flight):
        """Attempt to download details about this flight from the external server."""
        tail = flight.tail if flight.tail else flight.flight_id.strip()

        logging.debug("do_server_update: " + tail)
        try:
            # TODO could optimize: only if unregistered?
            # TODO move appsheet code to another module for cleanliness
            obj = appsheet.aircraft_lookup(tail, wholeobj=True)
            self.note_string = ""

            if obj:
                flight.flags['Row ID'] = obj['Row ID']
                self.note_string += "Arrivals=%s " % obj['Arrivals']

            self.bg_color_warn = False

            if not obj:
                self.note_string += "*Unreg "
                self.bg_color_warn = True
            else:
                if test_dict(obj, 'Ban'):
                    self.note_string += "*BANNED "
                    self.bg_color_warn = True

                if not test_dict(obj, 'IsBxA'):
                    arr = obj['Arrivals']
                    try:
                        if int(arr) > 2:
                            self.note_string += "* >2 arrivals "
                    except Exception:
                        pass

                if not test_dict(obj, 'Registered online'):
                    if not test_dict(obj, 'IsBxA'):
                        self.note_string += "*Manual reg "
                        self.bg_color_warn = True

                if test_dict(obj, 'Related Notes'):
                    self.note_string += "*Notes "

            if test_dict(obj, 'IsBxA'):
                self.note_string += "BxA"

        except Exception:
            logging.debug("do_server_update parse failed")
            pass

        self.set_normal()
        self.update(flight, None, None)
        logging.debug("done running update_from_server " + tail)

    def server_refresh_thread(self, flight):
        """This thread periodically refreshes aircraft details with the server."""
        if not appsheet: return
        while not self.stop_event.is_set():
            self.do_server_update(flight)
            time.sleep(SERVER_REFRESH_RATE)
        logging.debug("Exited refresh thread")

    def update(self, flight, location, bboxes_list):
        """ Re-build strip strings, changes show up on-screen automatically """
        logging.debug(f"strip.update for {flight.tail}, {bboxes_list}")
        if (flight.flight_id.strip() != flight.tail and flight.tail):
            extratail = flight.tail
        else:
            extratail = ""
        self.top_string = "[b][size=34]%s %s[/size][/b]" % (flight.flight_id.strip(),
            extratail)

        if location and bboxes_list:
            bbox_2nd_level = flight.get_bbox_at_level(1)

            # XXX hack to keep string from wrapping...not sure how to get kivy
            # to do this
            cliplen = 23 - len(flight.flight_id.strip()) - len(extratail)
            if cliplen < 0: cliplen = 0
            self.loc_string = bbox_2nd_level[0:cliplen] if bbox_2nd_level else ""

            altchangestr = flight.get_alt_change_str(location.alt_baro)
            self.alt_string = altchangestr + " " + str(location.alt_baro) + " " + str(int(location.gs))

        self.update_strip_text()

    def set_highlight(self):
        """Use a stronger color to draw attention to newly added strips"""
        self.main_button.background_color = (.5,.5,.5)
        Clock.schedule_once(lambda dt: self.set_normal(), 5)

    def set_normal(self):
        """Set strip to its steady-state color based on its state"""
        if self.bg_color_warn:
            self.main_button.background_color = (1,0,0)
        else:
            self.main_button.background_color = (0,.7,0)

    def annotate(self, note):
        """Highlight this strip and add a warning note to it."""
        logging.debug("**** annotate " + note)

        self.note_string = note
        if self.deanno_event:
            Clock.unschedule(self.deanno_event)
        self.deanno_event = Clock.schedule_once(lambda dt: self.deannotate(), 10)
        self.main_button.background_color = (.7,.7,0)

        self.update_strip_text()

    def deannotate(self):
        """Remove warning condition."""
        self.note_string = ""
        self.set_normal()
        self.update_strip_text()

def test_dict(d, key):
    """Returns true if key is in d and that they key's entry is not empty/N"""
    if not d: return False
    if not key in d: return False
    if d[key] == '' or d[key] == 'N': return False
    return True

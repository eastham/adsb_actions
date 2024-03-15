#!/usr/bin/python3
"""Make calls to appsheet to get and set database data.
Also can be called from the command line to make certain db calls."""

import copy
import json
import datetime
import argparse
import time
import random
import sys
import logging
import pprint
import requests

from adsb_actions.config import Config
logger = logging.getLogger(__name__)
logger.level = logging.INFO

REQUEST_BODY = {
"Properties": {
   "Locale": "en-US",
   "Timezone": "Pacific Standard Time",
},
"Rows": [
]
}

# Testing flags/constants
USE_FAKE_CALLS = False       # don't actually send anything to server
DELAYTEST = False            # add random delay for threading testing
FAKE_KEY = "XXXfake keyXXX"  # fake db key to use if real db calls disabled
DUMMY_AIRCRAFT = "N1911"     # aircraft to use for dummy ops

class Appsheet:
    def __init__(self):
        self.config = Config()
        self.headers = {"ApplicationAccessKey":
            self.config.private_vars["appsheet"]["accesskey"]}
        self.use_fake_calls = USE_FAKE_CALLS
        if self.use_fake_calls:
            logger.warning("NOTE: using fake database calls, "
                           "no actions will be taken")
        self.pp = pprint.PrettyPrinter(indent=4)

    def pprint_format(self, arg):
        """return the complex arg as a pretty-printed string"""
        return self.pp.pformat(arg)

    def enter_fake_mode(self):
        self.use_fake_calls = True

    def aircraft_lookup(self, tail, wholeobj=False):
        """return database internal ID for this tail number """

        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Find"
        body["Properties"]["Selector"] = "Select(Aircraft[Row ID], [Regno] = \"%s\")" % tail
        try:
            if not self.use_fake_calls:
                ret = self.sendop(self.config.private_vars["appsheet"]["aircraft_url"], body)
                if ret:
                    logger.debug("lookup for tail " + tail + " lookup returning "+ ret[0]["Row ID"])
                    if wholeobj:
                        return ret[0]
                    return ret[0]["Row ID"]

                logger.debug("lookup for tail " + tail + " failed")
                return ret
            return FAKE_KEY
        except Exception:
            logger.warning("aircraft_lookup op raised exception")

        return None

    def add_aircraft(self, regno, test=False, description=""):
        """Create aircraft in database"""

        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Add"
        body["Rows"] = [{
            "regno": regno,
            "test": test,
            "description": description
        }]
        try:
            if not self.use_fake_calls:
                ret = self.sendop(self.config.private_vars["appsheet"]["aircraft_url"], body)
                return ret["Rows"][0]["Row ID"]
            else:
                return FAKE_KEY
        except Exception as e:
            logger.warning("add_aircraft op raised exception: " + str(e))
        return None

    def get_all_entries(self, table):
        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Find"
        url = table + "_url"
        try:
            ret = self.sendop(self.config.private_vars["appsheet"][url], body)
            if ret:
                return ret
        except Exception as e:
            logger.warning("get_all_entries raised exception: " + str(e))
        return None

    def delete_all_entries(self, table):
        if self.use_fake_calls:
            return

        allentries = self.get_all_entries(table)
        deleterows = []
        for op in allentries:
            deleterows.append({"Row ID": op["Row ID"]})

        logger.warning("Deleting %d rows from table '%s', starting in 5 seconds.",
                    len(deleterows), table)
        time.sleep(5)

        logger.debug("delete rows are " + str(deleterows))

        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Delete"
        body["Rows"] = deleterows
        url = table + "_url"
        self.sendop(self.config.private_vars["appsheet"][url], body, timeout=None)

    def delete_aircraft(self, regno):
        if self.use_fake_calls:
            return

        allentries = self.get_all_entries("aircraft")
        deleterows = []
        for op in allentries:
            if op["Regno"].lower() == regno.lower():
                deleterows.append({"Row ID": op["Row ID"]})

        logger.info("delete rows are %s", str(deleterows))

        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Delete"
        body["Rows"] = deleterows
        url = "aircraft" + "_url"
        self.sendop(self.config.private_vars["appsheet"][url], body, timeout=None)

    def add_aircraft_from_file(self, fn):
        if self.use_fake_calls:
            return
        with open(fn, 'r') as file:
            for line in file:
                line = line.strip().upper()
                if line and (line[0] == 'N' or line[0] == 'C'):
                    print(f"adding {line}")
                    self.add_aircraft(line)

    def add_op(self, aircraft, time, scenic, optype, flight_name):
        if self.use_fake_calls:
            return
        optime = datetime.datetime.fromtimestamp(time)

        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Add"
        body["Rows"] = [{
            "Aircraft": aircraft,
            "Scenic": scenic,
            #"test": True,
            "manual": False,
            "optype": optype,
            "Time": optime.strftime("%m/%d/%Y %H:%M:%S"),
            "Flight Name": flight_name
        }]
        try:
            if not self.use_fake_calls:
                self.sendop(self.config.private_vars["appsheet"]["ops_url"], body)
            return True
        except Exception as e:
            logger.warning("add_op raised exception: " + str(e))

        return None

    def add_dummy_ops(self):
        if self.use_fake_calls:
            return
        # add a bunch of ops for testing
        ac_ref = self.aircraft_lookup(DUMMY_AIRCRAFT)
        assert ac_ref, "Could not find aircraft for dummy entry"
        for _ in range(100):
            self.add_op(ac_ref, time.time(), False,
                        "Arrival", "Test Flight")

    def add_cpe(self, flight1, flight2, latdist, altdist, time, lat, long):
        # XXX needs test w/ lat /long addition
        optime = datetime.datetime.fromtimestamp(time)

        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Add"
        body["Rows"] = [{
            "Aircraft1": flight1,
            "Aircraft2": flight2,
            "Time": optime.strftime("%m/%d/%Y %H:%M:%S"),
            "Min alt sep": altdist,
            "Min lat sep": latdist*6076,
            "lat": lat,
            "long": long
        }]

        try:
            if not self.use_fake_calls:
                ret = self.sendop(self.config.private_vars["appsheet"]["cpe_url"], body)
                return ret["Rows"][0]["Row ID"]
            return FAKE_KEY
        except Exception:
            logger.warning("add_cpe op raised exception")
        return None

    def update_cpe(self, flight1, flight2, latdist, altdist, time, rowid):
        optime = datetime.datetime.fromtimestamp(time)
        body = copy.deepcopy(REQUEST_BODY)
        body["Action"] = "Edit"
        body["Rows"] = [{
            "Row ID": rowid,
            "Aircraft1": flight1,
            "Aircraft2": flight2,
            "Time": optime.strftime("%m/%d/%Y %H:%M:%S"),
            "Min alt sep": altdist,
            "Min lat sep": latdist*6076,
            "Final": True
        }]

        try:
            if not self.use_fake_calls:
                ret = self.sendop(self.config.private_vars["appsheet"]["cpe_url"], body)
                return ret
            return FAKE_KEY
        except Exception:
            logger.warning("update_cpe op raised exception")
        return None

    def sendop(self, url, body, timeout=30):
        """send a request to the appsheet server and return the response dict.
        If the response is not 200, raise an HTTPError. If the response is
        empty, return None. If the response is not empty, return the response
        dict."""
        response_dict = None

        caller = sys._getframe(1).f_code.co_name

        if DELAYTEST:
            delay = random.uniform(1, 3)
            logger.info("debug delay of %ss", delay)
            time.sleep(delay)

        response = requests.post(
            url,
            headers=self.headers, json=body, timeout=timeout)
        if response.status_code != 200:
            logger.debug("non-200 return: %s", 
                         self.pprint_format(response))
            raise requests.HTTPError("op returned non-200 code: " +
                                     str(response))

        if not response.text:
            return None
        response_dict = json.loads(response.text)
        if not response_dict:
            return None
        logger.debug("sendop response_dict for op %s: %s",
                     caller, self.pprint_format(response_dict))

        return response_dict

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    as_instance = Appsheet()

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("--get_all_ops", action="store_true")

    parser.add_argument("--delete_all_ops", action="store_true")
    parser.add_argument("--delete_all_pilots", action="store_true")
    parser.add_argument("--delete_all_aircraft", action="store_true")
    parser.add_argument("--delete_all_abes", action="store_true")
    parser.add_argument("--delete_all_notes", action="store_true")
    parser.add_argument("--add_aircraft", help="add all aircraft listed in file, one per line")
    parser.add_argument("--delete_aircraft", help="delete all copies of argument aircraft")
    parser.add_argument("--add_dummy_ops", action="store_true")

    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    if args.get_all_ops:
        print(as_instance.get_all_entries("ops"))
    if args.delete_aircraft:
        confirm = input("Deleting all copies of aircraft. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            as_instance.delete_aircraft(args.delete_aircraft)
    if args.add_aircraft:
        as_instance.add_aircraft_from_file(args.add_aircraft)
    if args.delete_all_ops:
        confirm = input("Deleting all ops. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            as_instance.delete_all_entries("ops")
    if args.delete_all_pilots:
        confirm = input("Deleting all pilots. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            as_instance.delete_all_entries("pilot")
    if args.delete_all_aircraft:
        confirm = input("Deleting all aircraft. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            as_instance.delete_all_entries("aircraft")
    if args.delete_all_abes:
        confirm = input("Deleting all ABEs. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            as_instance.delete_all_entries("cpe")
    if args.delete_all_notes:
        confirm = input("Deleting all notes. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            as_instance.delete_all_entries("notes")
    if args.add_dummy_ops:
        as_instance.add_dummy_ops()

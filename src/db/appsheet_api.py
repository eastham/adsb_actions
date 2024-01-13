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
import requests
import pprint

sys.path.insert(0, '../adsb_actions')
from config import Config

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

BODY = {
"Properties": {
   "Locale": "en-US",
   "Timezone": "Pacific Standard Time",
},
"Rows": [
]
}

# NOTE: real-life server calls can be enabled/disabled here
SEND_AIRCRAFT = False
LOOKUP_AIRCRAFT = False
SEND_OPS = False
SEND_CPES = False

# Testing flags/constants
DELAYTEST = False            # add random delay for threading testing
FAKE_KEY = "XXXfake keyXXX"  # fake db key to use if real db calls disabled

class Appsheet:
    def __init__(self):
        self.config = Config()
        self.headers = {"ApplicationAccessKey":
            self.config.private_vars["appsheet"]["accesskey"]}

    def aircraft_lookup(self, tail, wholeobj=False):
        """return database internal ID for this tail number """
        logger.debug("aircraft_lookup %s" % (tail))

        body = copy.deepcopy(BODY)
        body["Action"] = "Find"
        body["Properties"]["Selector"] = "Select(Aircraft[Row ID], [Regno] = \"%s\")" % tail
        # ppd(body)
        try:
            if LOOKUP_AIRCRAFT:
                ret = self.sendop(self.config.private_vars["appsheet"]["aircraft_url"], body)
                if ret:
                    logger.debug("lookup for tail " + tail + " lookup returning "+ ret[0]["Row ID"])
                    if wholeobj: return ret[0]
                    else: return ret[0]["Row ID"]
                else:
                    logger.debug("lookup for tail " + tail + " failed")
                return ret
            else:
                return FAKE_KEY
        except Exception:
            logger.info("aircraft_lookup op raised exception")

        return None

    def add_aircraft(self, regno, test=False, description=""):
        """Create aircraft in database"""
        logger.debug("add_aircraft %s" % (regno))

        body = copy.deepcopy(BODY)
        body["Action"] = "Add"
        body["Rows"] = [{
            "regno": regno,
            "test": test,
            "description": description
        }]
        #ppd(self.headers)
        #ppd(body)
        try:
            if SEND_AIRCRAFT :
                ret = self.sendop(self.config.private_vars["appsheet"]["aircraft_url"], body)
                return ret["Rows"][0]["Row ID"]
            else:
                return FAKE_KEY
        except Exception as e:
            logger.info("add_aircraft op raised exception: " + str(e))

        return None

    def get_all_entries(self, table):
        logger.debug("get_all_entries " + table)

        body = copy.deepcopy(BODY)
        body["Action"] = "Find"
        #ppd(body)
        url = table + "_url"
        try:
            ret = self.sendop(self.config.private_vars["appsheet"][url], body)
            if ret:
                return ret
        except Exception:
            pass
        return None

    def delete_all_entries(self, table):
        allentries = self.get_all_entries(table)
        deleterows = []
        for op in allentries:
            deleterows.append({"Row ID": op["Row ID"]})

        logger.debug("delete rows are " + str(deleterows))

        body = copy.deepcopy(BODY)
        body["Action"] = "Delete"
        body["Rows"] = deleterows
        url = table + "_url"
        #ppd(body)
        ret = self.sendop(self.config.private_vars["appsheet"][url], body, timeout=None)

    def delete_aircraft(self, regno):
        allentries = self.get_all_entries("aircraft")
        deleterows = []
        for op in allentries:
            if op["Regno"].lower() == regno.lower():
                deleterows.append({"Row ID": op["Row ID"]})

        logger.debug("delete rows are " + str(deleterows))

        body = copy.deepcopy(BODY)
        body["Action"] = "Delete"
        body["Rows"] = deleterows
        url = "aircraft" + "_url"
        #ppd(body)
        ret = self.sendop(self.config.private_vars["appsheet"][url], body, timeout=None)

    def add_aircraft_from_file(self, fn):
        with open(fn, 'r') as file:
            for line in file:
                line = line.strip().upper()
                if line and (line[0] == 'N' or line[0] == 'C'):
                    print(f"adding {line}")
                    self.add_aircraft(line)

    def add_op(self, aircraft, time, scenic, optype, flight_name):
        logger.debug("add_op %s %s %s" % (aircraft, optype, scenic))
        optime = datetime.datetime.fromtimestamp(time)

        body = copy.deepcopy(BODY)
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
            if SEND_OPS:
                ret = self.sendop(self.config.private_vars["appsheet"]["ops_url"], body)
            return True
        except Exception:
            logger.info("add_op raised exception")

        return None

    def add_cpe(self, flight1, flight2, latdist, altdist, time, lat, long):
        # XXX needs test w/ lat /long addition
        logger.info("add_cpe %s %s" % (flight1, flight2))
        optime = datetime.datetime.fromtimestamp(time)

        body = copy.deepcopy(BODY)
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
        #ppd(body)
        try:
            if SEND_CPES:
                ret = self.sendop(self.config.private_vars["appsheet"]["cpe_url"], body)
                return ret["Rows"][0]["Row ID"]
            else:
                return FAKE_KEY
        except Exception:
            logger.info("add_cpe op raised exception")
        return None

    def update_cpe(self, flight1, flight2, latdist, altdist, time, rowid):
        logger.info("update_cpe %s %s" % (flight1, flight2))
        optime = datetime.datetime.fromtimestamp(time)
        body = copy.deepcopy(BODY)
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
        #ppd(body)

        try:
            if SEND_CPES:
                ret = self.sendop(self.config.private_vars["appsheet"]["cpe_url"], body)
                return ret
            else:
                return FAKE_KEY
        except Exception:
            logger.info("update_cpe op raised exception")
        return None

    def sendop(self, url, body, timeout=30):
        logger.info("sending to url "+url)
        response_dict = None

        if DELAYTEST:
            delay = random.uniform(1, 3)
            logger.info(f"delaying {delay}")
            time.sleep(delay)
        response = requests.post(
            url,
            headers=self.headers, json=body, timeout=timeout)
        if response.status_code != 200:
            #ppd(response)
            raise Exception("op returned non-200 code: "+str(response))
        # ppd(response)
        if not response.text: return None
        response_dict = json.loads(response.text)
        logger.debug(f"sendop response_dict for op ...{url[-20:]}: {response_dict}")

        if not len(response_dict): return None

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

    args = parser.parse_args()

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

pp = pprint.PrettyPrinter(indent=4)
def ppd(arg):
    pp.pprint(arg)

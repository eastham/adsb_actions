#!/usr/bin/python3
import copy
import json
import datetime
import argparse
import requests
import time
import random

from config import Config
from dbg import ppd, log, set_dbg_level, dbg

BODY = {
"Properties": {
   "Locale": "en-US",
   "Timezone": "Pacific Standard Time",
},
"Rows": [
]
}

DELAYTEST = False            # for threading testing
SEND_AIRCRAFT = False
LOOKUP_AIRCRAFT = False
SEND_OPS = False
SEND_CPES = False
FAKE_KEY = "XXXfake keyXXX"  # for testing purposes

class Appsheet:
    def __init__(self):
        self.config = Config()
        self.headers = {"ApplicationAccessKey":
            self.config.private_vars["appsheet"]["accesskey"]}

    def aircraft_lookup(self, tail, wholeobj=False):
        """return appsheet internal ID for this tail number """
        dbg("aircraft_lookup %s" % (tail))

        body = copy.deepcopy(BODY)
        body["Action"] = "Find"
        body["Properties"]["Selector"] = "Select(Aircraft[Row ID], [Regno] = \"%s\")" % tail
        # ppd(body)
        try:
            if LOOKUP_AIRCRAFT:
                ret = self.sendop(self.config.private_vars["appsheet"]["aircraft_url"], body)
                if ret:
                    dbg("lookup for tail " + tail + " lookup returning "+ ret[0]["Row ID"])
                    if wholeobj: return ret[0]
                    else: return ret[0]["Row ID"]
                else:
                    dbg("lookup for tail " + tail + " failed")
                return ret
            else:
                return FAKE_KEY
        except Exception:
            log("aircraft_lookup op raised exception")

        return None

    def add_aircraft(self, regno, test=False, description=""):
        """Create aircraft in appsheet"""
        dbg("add_aircraft %s" % (regno))

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
            log("add_aircraft op raised exception: " + str(e))

        return None

    def get_all_entries(self, table):
        dbg("get_all_entries " + table)

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

        dbg("delete rows are " + str(deleterows))

        body = copy.deepcopy(BODY)
        body["Action"] = "Delete"
        body["Rows"] = deleterows
        url = table + "_url"
        ppd(body)
        ret = self.sendop(self.config.private_vars["appsheet"][url], body, timeout=None)

    def delete_aircraft(self, regno):
        allentries = self.get_all_entries("aircraft")
        deleterows = []
        for op in allentries:
            if op["Regno"].lower() == regno.lower():
                deleterows.append({"Row ID": op["Row ID"]})

        dbg("delete rows are " + str(deleterows))

        body = copy.deepcopy(BODY)
        body["Action"] = "Delete"
        body["Rows"] = deleterows
        url = "aircraft" + "_url"
        ppd(body)
        ret = self.sendop(self.config.private_vars["appsheet"][url], body, timeout=None)

    def add_aircraft_from_file(self, fn):
        with open(fn, 'r') as file:
            for line in file:
                line = line.strip().upper()
                if line and (line[0] == 'N' or line[0] == 'C'):
                    print(f"adding {line}")
                    self.add_aircraft(line)

    def add_op(self, aircraft, time, scenic, optype, flight_name):
        dbg("add_op %s %s" % (aircraft, optype))
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
            log("add_op raised exception")

        return None

    def add_cpe(self, flight1, flight2, latdist, altdist, time, lat, long):
        # XXX needs test w/ lat /long addition
        log("add_cpe %s %s" % (flight1, flight2))
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
            log("add_cpe op raised exception")
        return None

    def update_cpe(self, flight1, flight2, latdist, altdist, time, rowid):
        log("update_cpe %s %s" % (flight1, flight2))
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
            log("update_cpe op raised exception")
        return None

    def sendop(self, url, body, timeout=30):
        log("sending to url "+url)
        response_dict = None

        if DELAYTEST:
            delay = random.uniform(1, 3)
            log(f"delaying {delay}")
            time.sleep(delay)

        response = requests.post(
            url,
            headers=self.headers, json=body, timeout=timeout)
        if response.status_code != 200:
            ppd(response)
            raise Exception("op returned non-200 code: "+str(response))
        # ppd(response)
        if not response.text: return None
        response_dict = json.loads(response.text)
        dbg(f"sendop response_dict for op ...{url[-20:]}: {response_dict}")

        if not len(response_dict): return None

        return response_dict

if __name__ == "__main__":
    set_dbg_level(2)
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

    if args.get_all_ops: print(as_instance.get_all_ops())
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

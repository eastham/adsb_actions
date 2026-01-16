"""Monitor ADS-B data from an internet API and apply adsb_actions 
rules/actions as usual.

API requests for aircraft data are made based on all "latlongring" conditions
found in the yaml file.

Usage:
    python3 tcp_client.py <yaml_path>
"""

import argparse
import signal
import time
import sys
import threading
import logging
import json
import yaml
import requests

from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG
LOGGER = Logger()

API_ENDPOINT = "https://api.airplanes.live/v2/point/"
API_RATE_LIMIT = 1/60      # requests per second
EXPIRE_SECS = 31  # expire aircraft from database not seen in this many seconds

class QueryState:
    """Information and methods associated with a single recurring API call.
    
    Args:
        name (str): The name of the query or location queried.
        latlongring (list): The lat/long coordinates of the query.
        adsb_actions (AdsbActions): The adsb_actions instance to use for processing.
        logfile (file): The file to write ALL results of the query (optional)"""
    def __init__(self, name, latlongring, adsb_actions, logfile):
        self.name = name
        self.latlongring = latlongring
        self.active = True
        self.last_checked = 0
        self.last_activated = 0
        self.adsb_actions = adsb_actions
        self.logfile = logfile

    def call_api_and_process(self):
        logger.info(f'Doing API query for rule "{self.name}"')

        url = (f"{API_ENDPOINT}{self.latlongring[1]}/"
               f"{self.latlongring[2]}/{self.latlongring[0]}")

        # Issue query
        try:
            response = requests.get(url, timeout=10)
            json_data = response.json()
        except Exception as e:      # pylint: disable=broad-except
            logger.error(f"Error in API query: {str(e)}")
            return

        logger.info(f"API call returned {len(json_data['ac'])} flights")

        # Process data from API call.
        # TODO should be optimized to not go to string then back to json:
        json_list = ""      # list of json objects, a weird format
        for line in json_data['ac']:
            line['now'] = json_data['now'] / 1000
            json_list += json.dumps(line) + "\n"

        if self.logfile:
            self.logfile.write(json_list)
        self.adsb_actions.loop(string_data=json_list)
        self.last_checked = time.time()


class MonitorThread:
    def __init__(self, adsb_actions):
        self.queries = {}
        self.monitor_thread = threading.Thread(target=self.monitor_thread_loop)
        self.adsb_actions = adsb_actions

    def run(self):
        self.monitor_thread.start()

    def handle_exit(self, *_):
        sys.exit(0)

    def add_query(self, name, latlongring, logfile=None):
        logger.info(f"Adding API query {name}")
        self.queries[name] = QueryState(name, latlongring,
                                        self.adsb_actions, logfile)

    def monitor_thread_loop(self):
        """Main thread loop."""
        while True:
            ret = self.do_all_queries()

    def do_all_queries(self):
        """Do all registered queries.  Returns number of API queries issued."""
        query_ctr = 0

        for query in self.queries.values():
            if not query.active:
                continue

            start_time = time.time()

            query.call_api_and_process()
            query_ctr += 1

            # sleep to maintain API rate limit
            sleep_time = 1/API_RATE_LIMIT - (time.time() - start_time)
            if sleep_time > 0:
                time.sleep(sleep_time)

        return query_ctr

    def activity_callback(self, flight):
        """Currently basically a no-op..."""
        logging.info(f"activity callback {flight.flight_id}")


if __name__ == "__main__":
    # get yaml path from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path", help="Path to yaml file")
    args = parser.parse_args()
    yaml_path = args.yaml_path

    # read in yaml file
    with open(yaml_path, 'r') as stream:
        try:
            yaml_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)
            sys.exit(-1)

    # set up processing environment
    adsb_actions = AdsbActions(yaml_data=yaml_data, expire_secs=EXPIRE_SECS)
    monitor_thread = MonitorThread(adsb_actions)

    signal.signal(signal.SIGINT, monitor_thread.handle_exit)

    # register callback
    adsb_actions.register_callback("activity_cb",
                                   monitor_thread.activity_callback)

    # read in API queries to run, only those with lat/long rings
    try:
        for rulename, rulebody in yaml_data['rules'].items():
            if 'latlongring' in rulebody['conditions']:
                monitor_thread.add_query(rulename,
                                         rulebody['conditions']['latlongring'],
                                         None)
    except Exception as ex:      # pylint: disable=broad-except
        logger.error("error in yaml file: " + str(ex))

    # start monitoring thread
    monitor_thread.run()

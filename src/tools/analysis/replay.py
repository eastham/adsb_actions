#!/usr/bin/python3
"""
Replay a directory of readsb trace files, and make the data available
in json form, on a socket, or stdout, using the same format as readsb
running in realtime (which is unfortunately different from the disk 
format).

Example Usage:
Socket output mode: replay.py --port 6666 --utc_convert=-7 --speed_x=10 [file]
String output mode: replay.py --utc_convert=-7 [file]
JSON API:    
    allpoints = read_data(directory)
    allpoints_iterable = yield_json_data(allpoints)

CAUTION: in socket mode, this silently drops data when run at high
speed (>1000x), not sure why.  If you need that kind of speed,
probably better to use the JSON API.
"""

import argparse
import datetime
import os
import gzip
import json
from fnmatch import fnmatch
import signal
import socket
import sys
import time

from tools.analysis import readsb_parse

# used to send placeholder timestamps to the client
EMPTY_MESSAGE = {'flight': 'N/A'}

def locate_files(directory, pattern):
    """Find all relevant files in this directory tree."""

    allfiles = []
    for path, _, files in os.walk(directory):
        for name in files:
            if fnmatch(name, pattern):
                allfiles.append(os.path.join(path, name))
    return allfiles

def parse_files(files: list) -> dict:
    """Uncompress and parse a list of files, return results
    in a dict indexed by timestamp."""

    allpoints = {}

    for file in files:
        fd = gzip.open(file, mode="r")
        try:
            jsondict = json.loads(fd.read())
        except gzip.BadGzipFile:
            print(f"Failed to un-gzip file {file}, skipping.")
            continue
        except Exception as e:
            print(f"JSON parse error in file {file}, skipping: {e}")
            continue
        readsb_parse.parse_readsb_json(jsondict, allpoints)

    return allpoints

def read_data(directory):
    files = locate_files(directory, "*.json")
    if not files:
        files = {directory}
    allpoints = parse_files(files)
    return allpoints

def yield_json_data(allpoints, insert_dummy_entries = True):
    first_ts = min(allpoints.keys())
    last_ts = max(allpoints.keys())
    first_time = datetime.datetime.fromtimestamp(
        first_ts).strftime("%m/%d/%y %H:%M")
    last_time = datetime.datetime.fromtimestamp(
        last_ts).strftime("%m/%d/%y %H:%M")

    print(f"First point seen at {first_ts} / {first_time}, last at ",
          f"{last_ts} / {last_time}")
    print(f"Parse of {len(allpoints)} points complete, beginning processing...")

    counter = 0
    for k in range(first_ts, last_ts):
        if not k in allpoints:
            if insert_dummy_entries and counter % 20 == 0:
                # Send an entry at least every 20 iterations to make it easy for the
                # client to account for the passage of time, do maintenance work, etc
                EMPTY_MESSAGE['now'] = k
                yield EMPTY_MESSAGE
        else:
            for point in allpoints[k]:
                yield point
        counter += 1

def main(directory : str, port: int,
         utc_convert : int, speed_x : int):
    """Read all files from given directory, send out on port (or stdout),
    applying a timezone conversion and speed multiplier"""

    signal.signal(signal.SIGINT, lambda *_: sys.exit(1))

    print("Parsing data...")
    allpoints = read_data(directory)
    if not allpoints:
        print("No data found, exiting.")
        sys.exit(1)
    allpoints_iterable = yield_json_data(allpoints)

    sock = Socket('0.0.0.0', port) if port else 0

    if sock:
        print("Waiting for first network connection...")
        while not sock.try_accept():
            pass
    send_ctr = 0

    # Iterate through the points in time order.  One second at a time,
    # each second may contain multiple points...
    for point in allpoints_iterable:
        # keep monitoring for new connections
        if sock:
            sock.try_accept()

        start_work_ts = time.time()

        point['now'] += utc_convert * 60 * 60  # convert to local time
        string = json.dumps(point) + "\n"
        buffer = bytes(string, 'ascii')
        if sock:
            sock.sendall(buffer)
            send_ctr += 1
        else:
            print(buffer.decode(), end='')

        # slow down if needed to hit speed multiplier.
        if speed_x:
            work_time = time.time() - start_work_ts
            sleeptime = (1./speed_x) - work_time
            if sleeptime > 0.:
                time.sleep(sleeptime)

    print(f"Sent {send_ctr} lines.")

class Socket:
    """Class representing a socket connection."""

    def __init__(self, ip, port):
        """Constructs a new Socket object with specified IP and port."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2097152)
        self.socket.setblocking(False)
        self.socket.bind((ip, port))
        self.socket.listen(5)
        self.connections = []

    def accept(self):
        """Accepts incoming connections."""
        conn, addr = self.socket.accept()
        conn.setblocking(False)
        self.connections.append(conn)
        print(f"Connected to {addr} on {self.socket.getsockname()}")

    def try_accept(self):
        """Accept connection, silently fail if none"""
        try:
            self.accept()
            return True
        except socket.error:
            return False

    def sendall(self, data):
        """Sends data to all connected clients."""
        for conn in self.connections:
            try:
                conn.sendall(data)
            except socket.error as e:
                if e.errno == 32:  # Broken pipe
                    conn.close()
                    self.connections.remove(conn)

    def close(self):
        """Close the socket connection."""
        self.socket.close()
        for conn in self.connections:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Argument Parser for Constants')

    parser.add_argument('--port', type=int,
                        help='Network port.  If not specified, prints to stdout.')
    parser.add_argument('--utc_convert', type=int, default=0,
                        help='Time conversion from UTC to add in hours, positive or negative')
    parser.add_argument('--speed_x', type=int,
                        help='Playback speed multiplier. 1-3000 or so, or omit for full speed.')
    parser.add_argument('directory', type=str, help='Directory to scan')

    args = parser.parse_args()

    main(args.directory, args.port, args.utc_convert, args.speed_x)

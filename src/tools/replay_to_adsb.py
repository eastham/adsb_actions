#!/usr/bin/python3
"""

"""

import argparse
import json
import signal
import sys
import time
from tools.inject_adsb import ReadsbConnection
from tools import ADSB_Encoder

def inject_position(readsb, icao, lat, lon, alt):
    """Inject a position into readsb."""

    sentence1, sentence2 = ADSB_Encoder.encode(icao, lat, lon, alt)
    ret1 = readsb.inject(sentence1, sentence2)
    # send twice to force tar1090 rendering
    ret2 = readsb.inject(sentence1, sentence2)
    if ret1 + ret2:
        print("Failed to send position to readsb")
    return ret1 + ret2

def main(file: str, readsb_connection: ReadsbConnection,
         speed_x : int):
    """XXXXXX TODO"""

    signal.signal(signal.SIGINT, lambda *_: sys.exit(1))

    print("Parsing data...")
    allpoints = []
    # load json file
    try:
        with open(file, 'r') as f:
            for line in f:
                print(line)
                allpoints.append(json.loads(line))
        if not allpoints:
            print("No data found")
            return
    except FileNotFoundError:
        print("File not found")
        return
    
    # Iterate through the points in time order.  One second at a time,
    # each second may contain multiple points...
    send_ctr = 0
    for point in allpoints:
        start_work_ts = time.time()

        try:
            icao = int(point['hex'], 16)
            inject_position(readsb_connection, icao,
                            point['lat'], point['lon'], point['alt_baro'])
        except KeyError:
            print("Skipping line: " + str(point))
            continue

        send_ctr += 1
        # slow down if needed to hit speed multiplier.
        if speed_x:
            work_time = time.time() - start_work_ts
            sleeptime = (1./speed_x) - work_time
            if sleeptime > 0.:
                time.sleep(sleeptime)

    print(f"Sent {send_ctr} lines.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Argument Parser for Constants')

    parser.add_argument('--inject_addr', type=str,
                        help='Inject data into this address:port')
    parser.add_argument('--speed_x', type=int, default=100,
                        help='Playback speed multiplier. 1-3000 or so, or omit for full speed.')
    parser.add_argument('file', type=str, help='JSON file to replay')

    args = parser.parse_args()

    try:
        inj_ip, inj_port = args.inject_addr.split(':')
        inj_port = int(inj_port)
        readsb = ReadsbConnection(inj_ip, inj_port)
    except:         # pylint: disable=bare-except
        print("Bad inject_addr format, must be ip:port")
        sys.exit(1)

    main(args.file, readsb, args.speed_x)

"""Support for translating readsb's disk files, from a difficult-to-
read format to JSON that looks like this:

{'now': 1725215956, 'alt_baro': '0', 'gscp': 1, 'lat': 40.763479, 
'lon': -119.212334, 'track': 101.2, 'hex': 'ab86de', 'flight': 'N8417A', 
'flightdict': None} 
"""
import pprint
from datetime import datetime

pp = pprint.PrettyPrinter(indent=4)

def parse_readsb_json(input_dict: dict, parsed_output: dict, tp_callback = None) -> dict:
    """Analyze a single readsb json file, which contains a handful of aircraft's
    traces for the day.  Unfortunately it is stored in a totally different
    format than the wire format, which we restore here.

    Args:
        input_dict: input in readsb format
        parsed_output: dict by timestamp, each entry is a list of parsed json dicts
            Results are added to parsed_output which is mutated in place.
        tp_callback: optional callback to fire with the data for each point."""

    # A few details are at the aircraft level:
    icao_num = input_dict['icao']
    flight_str = input_dict.get('r')  # tail number
    start_ts = int(input_dict['timestamp'])
    # pp.pprint(d)

    point_ctr = 0
    # iterate through trace points for this aircraft
    for tp in input_dict['trace']:
        # pp.pprint(tp)

        # Pull out relevant values
        time_offset, lat, long, alt, gs, track, _, _, flightdict, *_ = tp

        # Clean up data
        time_offset = int(time_offset)
        gs = int(gs) if gs else 0
        if not flight_str and flightdict:
            flight_str = flightdict.get('flight', '').strip()

        try:
            altint = int(alt)   # can be 'ground' etc
        except Exception:      # pylint: disable=broad-except
            alt = "0"

        # Per-tracepoint timestamp is seconds past the per-file timestamp
        this_ts = start_ts + time_offset

        # Add this location to allpoints
        newdict = {'now': this_ts, 'alt_baro': alt, 'gscp': gs, 'lat': lat,
            'lon':long, 'track': track, 'hex': icao_num, 'flight': flight_str,
            'flightdict': flightdict}

        # Extract additional fields from flightdict if present
        if flightdict:
            for field in ['squawk', 'emergency', 'category', 'baro_rate', 'gs']:
                if field in flightdict:
                    newdict[field] = flightdict[field]

        if this_ts in parsed_output:
            parsed_output[this_ts].append(newdict)
        else:
            parsed_output[this_ts] = [newdict]

        if tp_callback:
            tp_callback(icao_num, flight_str, lat, long, altint, 
                        get_timestr(this_ts))

        point_ctr += 1

    # print(f"Parsed {point_ctr} points.")

def get_timestr(ts):
    return (datetime.fromtimestamp(ts)).strftime('%H:%M:%S')

Support for offline playback and analysis, taking data from
readsb's disk format.

analyze_from_files.py: Offline flight operation analyzer,
tracking takeoffs, landings, etc.  See src/op_pusher for an
example of an online analyzer. 

Example output:

    INFO:adsbactions:Parsed 162543 points.
    Rule takeoff matched 203 times.
    Rule takeoff_popup matched 0 times.
    Rule landing matched 207 times.
        Including saw_takeoff 138 times.
    Rule proximity_alert matched 14 times.
        Including saw_takeoff 10 times.

replay.py: read saved tracks from disk and output on network socket
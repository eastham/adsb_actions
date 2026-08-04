

# download data from adsb.lol's github for the date mm/dd/yy and a given airport.
# all data should go in examples/generated instead of the paths named below

raw data filenames look like this for 12/06/25: v2025.12.06-planes-readsb-prod-0.tar.aa
first check for the presence of the tar.aa / tar.ab files before downloading.
if not found,
select the date (in 2025) from here: 
https://github.com/adsblol/globe_history_2025/blob/main/RELEASES.md
then download the two tar.a? files.

Once received, we need to untar them.  This creates 3 directories: traces, acas, heatmap. 
Any pre-existing data needs to be removed before starting:
rm -rf traces acas heatmap

then concatenate and extract the data:
cat [filenames] | tar --options read_concatenated_archives -xf -

# convert global data (do airport lookup to get latlong -- reuse load_airport() function) - and change output filename to match date
python src/tools/convert_traces.py ~/Downloads/adsb_lol_data/traces -o examples/KWVI/121425_kwvi.gz --lat 36.93 --lon -121.79 --radius 5 --progress 100

# do prox analysis, output lines with string "CSV" have the data (change dates as needed)
python3 src/analyzers/prox_analyze_from_files.py --yaml examples/KWVI/prox_analyze_from_files.yaml --resample --sorted-file examples/KWVI/121425_kwvi.gz --animate-los >& examples/KWVI/121425_kwvi.out
 
# grab csv lines
grep CSV examples/KWVI/121425_kwvi.out > examples/KWVI121425.csv.out

# (debug) print aircraft every 10 min:
python3 src/analyzers/simple_monitor.py examples/hello_world_rules.yaml --sorted-file examples/KWVI/1214_kwvi.gz

# (debug) visualize all points (XXX missing step to create CSV with all points)
python3 src/analyzers/simple_monitor.py examples/print_csv.yaml --sorted-file examples/KWVI/1220_kwvi.gz >& examples/KWVI/122025_kwvi.all.csv

cat examples/KWVI/1220.all.csv| python3 src/postprocessing/visualizer.py

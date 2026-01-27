
# convert global data
python src/tools/convert_traces.py ~/Downloads/adsb_lol_data/traces -o ~/Downloads/adsb_lol_data/1213.wvi.gz --lat 36.93 --lon -121.79 --radius 5 --progress
100

# do prox analysis
python3 src/analyzers/prox_analyze_from_files.py --yaml examples/KWVI/prox_analyze_from_files.yaml --resample --sorted-file ~/Downloads/adsb_lol_data/1213.wvi5nm.gz

# visualize:
cat examples/KWVI/wvi1213.5nm.out | python3 src/postprocessing/visualizer.py

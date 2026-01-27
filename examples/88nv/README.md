LOS seen on 8/28/2025  - 6 of them

python3 ../../src/analyzers/prox_analyze_from_files.py --yaml ./prox_analyze_from_files.yaml --resample ~/adsb_data/2025/08/28 >& 828.out

optional data compression: 
python src/tools/convert_traces.py ~/adsb_data/2025/08/28/traces -o examples/88nv/2025-08-28.jsonl.gz --lat 40.7618 --lon -119.210 --radius 10 --progress 100


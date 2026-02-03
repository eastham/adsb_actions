import os
import subprocess
import requests
import shutil
import argparse
from pathlib import Path
from datetime import datetime
import generate_airport_config

DATA_DIR = "data"
BASE_DIR = "examples/generated"
FT_ABOVE_AIRPORT = 4000
FT_BELOW_AIRPORT = -200 # negative to ignore ground events

def validate_date(date_text):
    try:
        return datetime.strptime(date_text, '%m/%d/%y')
    except ValueError:
        raise argparse.ArgumentTypeError(f"Incorrect date format '{date_text}', should be mm/dd/yy")

def run_command(command):
    print(f"🚀 Executing: {command}")
    result = subprocess.run(command, shell=True, text=True)
    if result.returncode != 0:
        print(f"❌ Command failed.")
    return result

def load_airport(airport_icao):
    airport = generate_airport_config.load_airport(airport_icao)
    try:
        field_elevation = float(airport.get('elevation_ft') or 0)
        center_lat = float(airport.get('latitude_deg') or 0)
        center_lon = float(airport.get('longitude_deg') or 0)
        field_alt = int(airport.get('elevation_ft'))
    except (ValueError, TypeError):
        print("Error: Could not parse airport coordinates.", file=sys.stderr)
        sys.exit(1)

    return center_lat, center_lon, field_alt

def setup_pipeline(args):
    date_obj = args.date
    airport_icao = args.airport.upper()
    
    # Format strings
    date_iso = date_obj.strftime('%Y.%m.%d')
    date_compact = date_obj.strftime('%m%d%y')
    full_year = date_obj.strftime('%Y')
    
    base_dir = Path(BASE_DIR)
    data_dir = Path(DATA_DIR)
    airport_dir = base_dir / airport_icao
    airport_dir.mkdir(parents=True, exist_ok=True)
    
    trace_gz = airport_dir / f"{date_compact}_{airport_icao}.gz"
    file_prefix = f"v{date_iso}-planes-readsb-prod-0"

    # --- STAGE 1: Generate Airport yaml if needed --- TODO TODO
    lat, lon, field_alt = load_airport(airport_icao)

    # Use command-line overrides for altitude bounds
    ft_above = args.ft_above
    ft_below = args.ft_below

    airport_yaml = base_dir / airport_icao / f"prox_analyze_from_files.yaml"
    if not airport_yaml.exists():
        print(f"⚙️ Generating airport YAML at {airport_yaml}...")
        yaml_text = generate_airport_config.generate_prox_yaml(airport_icao,
                                                    field_alt + ft_above,
                                                    field_alt + ft_below)
        os.makedirs (base_dir / airport_icao, exist_ok=True)
        with open(airport_yaml, 'w') as f:
            f.write(yaml_text)
    else:
        print(f"✅ {airport_yaml} exists. Skipping generation.")

    # --- STAGE 4: Download (.tar.aa / .tar.ab) ---
    for ext in ['aa', 'ab']:
        local_file = data_dir / f"{file_prefix}.tar.{ext}"
        if not local_file.exists() or args.force_download:
            print(f"📥 Downloading {local_file.name}...") # TODO progress indicator?
            url = f"https://github.com/adsblol/globe_history_{full_year}/releases/download/v{date_iso}-planes-readsb-prod-0/{file_prefix}.tar.{ext}"
            print(f"Downloading from {url}...")
            r = requests.get(url, stream=True)
            if r.status_code == 404:
                # https://github.com/adsblol/globe_history_2025/releases/download/v2025.06.01-planes-readsb-prod-0tmp/v2025.06.01-planes-readsb-prod-0tmp.tar.aa
                # global replace  prod-0 with prod-0tmp
                url = url.replace("prod-0", "prod-0tmp")
                print(f"Retrying download from {url}...")
                r = requests.get(url, stream=True)

            if r.status_code == 200:
                with open(local_file, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            else:
                print(f"⚠️ Could not download {local_file.name} (Status: {r.status_code})")
        else:
            print(f"✅ {local_file.name} exists. Skipping download.")

    # --- STAGE 5: Extraction (traces/ folder) ---
    # We check for the 'traces' directory as the indicator of extraction
    # force_download also forces re-extract
    if not trace_gz.exists() or args.force_extract:
        print("📦 Extracting tar archives...")
        # Clean old data before extract
        for folder in ['traces', 'acas', 'heatmap']:
            if Path(folder).exists(): shutil.rmtree(folder)
            
        archive_pattern = data_dir / f"{file_prefix}.tar.a*"
        run_command(f"cat {archive_pattern} | tar --options read_concatenated_archives -xf -")
    else:
        print(f"✅ {trace_gz.name} directory exists. Skipping extraction.")

    # --- STAGE 6: Convert Traces (.gz file) ---
    if not trace_gz.exists() or args.force_extract:
        print(f"⚙️ Converting traces to {trace_gz.name}...")
        run_command(f"python src/tools/convert_traces.py traces -o {trace_gz} "
                    f"--lat {lat} --lon {lon} --radius 5 --progress 100")
    else:
        print(f"✅ {trace_gz.name} exists. Skipping conversion.")

    # Clean temp data (unless --no-cleanup for batch processing)
    if not args.no_cleanup:
        for folder in ['traces', 'acas', 'heatmap']:
            if Path(folder).exists():
                shutil.rmtree(folder)

    # --- STAGE 7/8: Analysis (Always runs to reflect config changes) ---
    analysis_out = trace_gz.with_suffix('.out')
    csv_final = base_dir / f"{date_compact}_{airport_icao}.csv.out"
    print("📊 Running Analysis...")
    run_command(f"python3 src/analyzers/prox_analyze_from_files.py "
                f"--yaml {base_dir}/{airport_icao}/prox_analyze_from_files.yaml "
                f"--resample --sorted-file {trace_gz} --animate-los > {analysis_out} 2>&1")

    run_command(f"grep CSV {analysis_out} > {csv_final}")
    print("✅ CSV output written to:", csv_final)
    print("✅ Visualization of individual events:")
    run_command(f"grep 'LOS visualization' {analysis_out}")
    # Debug/Visualizer
    all_points_csv = airport_dir / f"{date_compact}_{airport_icao.lower()}.all.csv"
    #run_command(f"python3 src/analyzers/simple_monitor.py examples/print_csv.yaml "
    #            f"--sorted-file {trace_gz} > {all_points_csv} 2>&1")
    
    run_command(f"cat {all_points_csv} | python3 src/postprocessing/visualizer.py")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ADSB.lol Optimized Pipeline")
    parser.add_argument("date", type=validate_date, help="Date in mm/dd/yy format")
    parser.add_argument("airport", type=str, help="Airport ICAO code")

    parser.add_argument("--force-download", action="store_true",
                        help="Force download and re-extraction of raw tarballs")
    parser.add_argument("--force-extract", action="store_true",
                        help="Force re-conversion of traces (step 6)")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Skip cleanup of traces/ directory (for batch processing)")
    parser.add_argument("--ft-above", type=int, default=FT_ABOVE_AIRPORT,
                        help=f"Feet above airport elevation for analysis ceiling (default: {FT_ABOVE_AIRPORT})")
    parser.add_argument("--ft-below", type=int, default=FT_BELOW_AIRPORT,
                        help=f"Feet below airport elevation for analysis floor (default: {FT_BELOW_AIRPORT})")

    args = parser.parse_args()
    setup_pipeline(args)
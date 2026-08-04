# Manual Integration Test for Traffic Cloud Visualization

## Prerequisites
```bash
source .venv/bin/activate
```

## Test 1: Export Traffic Samples (Standalone)

If you have existing resampler data or sorted JSONL file:

```bash
python3 src/analyzers/prox_analyze_from_files.py \
  --sorted-file examples/generated/sorted_global.jsonl.gz \
  --yaml analyze_from_files.yaml \
  --resample \
  --export-traffic-samples /tmp/test_traffic.csv
```

**Expected output:**
- Message: "Exporting traffic point cloud samples..."
- Message: "Exported N traffic samples to /tmp/test_traffic.csv"
- File created at /tmp/test_traffic.csv

**Verify:**
```bash
wc -l /tmp/test_traffic.csv   # Should show point count
head /tmp/test_traffic.csv     # Should show lat,lon,alt format
```

## Test 2: Visualize Traffic Samples (Standalone)

Create a mock LOS CSV for testing:
```bash
cat > /tmp/test_los.csv << 'EOF'
CSV OUTPUT FOR POSTPROCESSING: 1693420800,2023-08-30 12:00:00,2023-08-30 12:00:00,40.7123,-119.2456,5000,N12345,N67890,0,http://test,http://test,0,0,overtake,approach,,0.5,500,
EOF
```

Test visualizer with traffic samples:
```bash
cat /tmp/test_los.csv | python3 src/postprocessing/visualizer.py \
  --sw 40.6,-119.4 --ne 40.8,-119.1 \
  --traffic-samples /tmp/test_traffic.csv \
  --output /tmp/test_map.html \
  --no-browser
```

**Expected output:**
- Message: "Loading traffic samples from /tmp/test_traffic.csv..."
- Message: "Loaded N traffic points"
- Message: "Added traffic cloud with N points"
- File created: /tmp/test_map.html

**Verify:**
```bash
open /tmp/test_map.html
```

Check map has:
- [ ] Layer control in top-right
- [ ] "Traffic Cloud" layer listed
- [ ] Light blue semi-transparent dots visible
- [ ] LOS event point(s) visible on top
- [ ] Can toggle traffic cloud on/off

## Test 3: Full Batch Pipeline (If Data Available)

```bash
python3 src/tools/batch_los_pipeline.py \
  --start-date 2023-08-20 \
  --end-date 2023-08-20 \
  --icao KRNO
```

**Expected in output:**
- Per-date: "Exporting traffic point cloud samples..."
- Aggregation: "Combined N traffic sample files"
- Aggregation: "Using traffic samples: KRNO_traffic_combined.csv"

**Verify files created:**
```bash
ls -lh examples/generated/KRNO/*traffic*
# Should see:
# - 082023_KRNO_traffic.csv (per-date)
# - KRNO_traffic_combined.csv (aggregated)

# Check visualization
open examples/generated/KRNO/KRNO_map.html
```

## Test 4: Performance Check

For a real dataset, verify processing time is reasonable:

```bash
time python3 src/analyzers/prox_analyze_from_files.py \
  --sorted-file <your-file> \
  --yaml analyze_from_files.yaml \
  --resample \
  --export-traffic-samples /tmp/test_traffic.csv
```

**Expected:**
- Export adds ~1-2 seconds overhead
- No significant memory increase

## Cleanup

```bash
rm -f /tmp/test_*.csv /tmp/test_map.html
```

## Success Criteria

✅ Traffic samples export without errors
✅ CSV format is valid (lat,lon,alt)
✅ Visualizer loads and renders traffic cloud
✅ Map layer control allows toggling
✅ Processing overhead is < 5 seconds
✅ Storage per day is < 2MB per airport

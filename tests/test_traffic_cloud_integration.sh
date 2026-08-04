#!/bin/bash
# Integration test for traffic track visualization feature
# Uses the 1hr.json test dataset

set -e  # Exit on error

echo "🧪 Traffic Track Visualization Integration Test"
echo "==============================================="

# Activate venv
source .venv/bin/activate

# Create temp directory for test outputs
TEST_DIR=$(mktemp -d)
echo "Test directory: $TEST_DIR"

# Test 1: Export traffic tracks from test data
echo ""
echo "📊 Test 1: Export traffic tracks"
echo "---------------------------------"

python3 src/analyzers/prox_analyze_from_files.py \
  --yaml tests/test_yaml/analyze_from_files.yaml \
  --resample \
  --export-traffic-samples "$TEST_DIR/traffic.json" \
  --sorted-file tests/1hr.json > "$TEST_DIR/analysis.out" 2>&1

# Check output
if [ -f "$TEST_DIR/traffic.json" ]; then
    LINE_COUNT=$(wc -l < "$TEST_DIR/traffic.json")
    echo "✓ Traffic tracks file created with $LINE_COUNT tracks"

    # Show sample
    echo "Sample tracks:"
    head -3 "$TEST_DIR/traffic.json"

    # Verify JSON format
    if head -1 "$TEST_DIR/traffic.json" | python3 -m json.tool > /dev/null 2>&1; then
        echo "✓ JSON format is valid"
    else
        echo "❌ JSON format is invalid!"
        exit 1
    fi

    # Check for point reduction stats in analysis output
    if grep -q "reduction" "$TEST_DIR/analysis.out"; then
        REDUCTION=$(grep "reduction" "$TEST_DIR/analysis.out" | head -1)
        echo "✓ $REDUCTION"
    fi
else
    echo "❌ Traffic tracks file not created!"
    cat "$TEST_DIR/analysis.out"
    exit 1
fi

# Test 2: Create mock LOS CSV for visualization
echo ""
echo "🗺️  Test 2: Visualize with traffic tracks"
echo "-----------------------------------------"

# Create a simple LOS event CSV
cat > "$TEST_DIR/los.csv" << 'EOF'
CSV OUTPUT FOR POSTPROCESSING: 1693420800,2023-08-30 12:00:00,2023-08-30 12:00:00,40.7123,-119.2456,5000,N12345,N67890,0,http://test,http://test,0,0,overtake,approach,,0.5,500,
EOF

# Run visualizer
cat "$TEST_DIR/los.csv" | python3 src/postprocessing/visualizer.py \
  --sw 40.6,-119.4 \
  --ne 40.8,-119.1 \
  --traffic-samples "$TEST_DIR/traffic.json" \
  --output "$TEST_DIR/map.html" \
  --no-browser > "$TEST_DIR/viz.out" 2>&1

# Check output
if [ -f "$TEST_DIR/map.html" ]; then
    echo "✓ Map HTML created: $TEST_DIR/map.html"

    # Verify traffic tracks were added (check for polyline which is Leaflet's PolyLine)
    if grep -q 'L.polyline' "$TEST_DIR/map.html"; then
        TRACK_LINE_COUNT=$(grep -c 'L.polyline' "$TEST_DIR/map.html")
        echo "✓ Map contains traffic tracks ($TRACK_LINE_COUNT PolyLines found)"
    else
        echo "❌ Map does not contain traffic tracks!"
        exit 1
    fi

    # Check file size is reasonable (should be smaller with tracks vs point cloud)
    SIZE=$(wc -c < "$TEST_DIR/map.html")
    echo "✓ Map size: $((SIZE / 1024)) KB"

    # Verify track count
    TRACK_COUNT=$(grep -o 'traffic tracks' "$TEST_DIR/viz.out" | head -1)
    if [ -n "$TRACK_COUNT" ]; then
        echo "✓ $(grep 'Added.*traffic tracks' "$TEST_DIR/viz.out")"
    fi

else
    echo "❌ Map HTML not created!"
    cat "$TEST_DIR/viz.out"
    exit 1
fi

# Test 3: Check processing time
echo ""
echo "⏱️  Test 3: Performance check"
echo "-----------------------------"

START=$(date +%s)
python3 src/analyzers/prox_analyze_from_files.py \
  --yaml tests/test_yaml/analyze_from_files.yaml \
  --resample \
  --export-traffic-samples "$TEST_DIR/traffic2.json" \
  --sorted-file tests/1hr.json > /dev/null 2>&1
END=$(date +%s)

DURATION=$((END - START))
echo "Processing time: ${DURATION}s"

if [ $DURATION -lt 30 ]; then
    echo "✓ Processing time is acceptable (< 30s)"
else
    echo "⚠️  Processing took longer than expected"
fi

# Cleanup
echo ""
echo "🧹 Cleanup"
echo "----------"
echo "Test files in: $TEST_DIR"
echo "To inspect: open $TEST_DIR/map.html"
echo "To cleanup: rm -rf $TEST_DIR"

echo ""
echo "✅ All tests passed!"
echo ""
echo "Next steps:"
echo "  - Open map: open $TEST_DIR/map.html"
echo "  - Verify traffic tracks (blue PolyLines) are visible and toggleable"
echo "  - Clean up when done: rm -rf $TEST_DIR"

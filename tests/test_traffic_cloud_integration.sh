#!/bin/bash
# Integration test for traffic cloud visualization feature
# Uses the 20minutes.json test dataset

set -e  # Exit on error

echo "🧪 Traffic Cloud Integration Test"
echo "=================================="

# Activate venv
source .venv/bin/activate

# Create temp directory for test outputs
TEST_DIR=$(mktemp -d)
echo "Test directory: $TEST_DIR"

# Test 1: Export traffic samples from test data
echo ""
echo "📊 Test 1: Export traffic samples"
echo "----------------------------------"

python3 src/analyzers/prox_analyze_from_files.py \
  --yaml tests/test_yaml/analyze_from_files.yaml \
  --resample \
  --export-traffic-samples "$TEST_DIR/traffic.csv" \
  --sorted-file tests/1hr.json > "$TEST_DIR/analysis.out" 2>&1

# Check output
if [ -f "$TEST_DIR/traffic.csv" ]; then
    LINE_COUNT=$(wc -l < "$TEST_DIR/traffic.csv")
    echo "✓ Traffic CSV created with $LINE_COUNT points"

    # Show sample
    echo "Sample points:"
    head -3 "$TEST_DIR/traffic.csv"

    # Verify format
    if head -1 "$TEST_DIR/traffic.csv" | grep -E '^-?[0-9]+\.[0-9]+,-?[0-9]+\.[0-9]+,[0-9]+$' > /dev/null; then
        echo "✓ CSV format is valid (lat,lon,alt)"
    else
        echo "❌ CSV format is invalid!"
        exit 1
    fi
else
    echo "❌ Traffic CSV not created!"
    cat "$TEST_DIR/analysis.out"
    exit 1
fi

# Test 2: Create mock LOS CSV for visualization
echo ""
echo "🗺️  Test 2: Visualize with traffic cloud"
echo "----------------------------------------"

# Create a simple LOS event CSV
cat > "$TEST_DIR/los.csv" << 'EOF'
CSV OUTPUT FOR POSTPROCESSING: 1693420800,2023-08-30 12:00:00,2023-08-30 12:00:00,40.7123,-119.2456,5000,N12345,N67890,0,http://test,http://test,0,0,overtake,approach,,0.5,500,
EOF

# Run visualizer
cat "$TEST_DIR/los.csv" | python3 src/postprocessing/visualizer.py \
  --sw 40.6,-119.4 \
  --ne 40.8,-119.1 \
  --traffic-samples "$TEST_DIR/traffic.csv" \
  --output "$TEST_DIR/map.html" \
  --no-browser > "$TEST_DIR/viz.out" 2>&1

# Check output
if [ -f "$TEST_DIR/map.html" ]; then
    echo "✓ Map HTML created: $TEST_DIR/map.html"

    # Verify traffic cloud was added (check for blue circles which are unique to traffic cloud)
    if grep -q '"blue"' "$TEST_DIR/map.html"; then
        echo "✓ Map contains traffic cloud points"
    else
        echo "❌ Map does not contain traffic cloud!"
        exit 1
    fi

    # Check file size is reasonable
    SIZE=$(wc -c < "$TEST_DIR/map.html")
    echo "✓ Map size: $((SIZE / 1024)) KB"

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
  --export-traffic-samples "$TEST_DIR/traffic2.csv" \
  --sorted-file tests/20minutes.json > /dev/null 2>&1
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
echo "  - Verify traffic cloud layer is visible and toggleable"
echo "  - Clean up when done: rm -rf $TEST_DIR"

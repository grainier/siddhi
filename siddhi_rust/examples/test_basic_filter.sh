#!/bin/bash
# Test basic filter with sample data

echo "Testing basic filter application..."

# Create test input file
cat << EOF > /tmp/test_input.txt
["Stock1", 50]
["Stock2", 150]
["Stock3", 200]
["Stock4", 75]
["Stock5", 300]
EOF

echo "Input data:"
cat /tmp/test_input.txt

echo -e "\nRunning Siddhi application with test data..."
timeout 5 /opt/siddhi/bin/siddhi-runner /opt/siddhi/apps/basic_filter_int.siddhi < /tmp/test_input.txt 2>&1 || true

echo -e "\nTest completed"
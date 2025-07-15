#!/bin/bash
# This script builds the tikvrpc package with debug flags and checks for specific heap allocation markers in the standard error output.
set -euo pipefail

DIR=$(dirname "$0")

# Construct environmental variables required for the build.
export GO111MODULE=on
export CGO_ENABLED=1

# Run the build command with retrieval of allocation diagnostics.
# Redirect the output to a temporary file for easier processing.
OUTPUT=$(mktemp)
echo "output: $OUTPUT"
if ! go build -tags=fusion -gcflags="-m -l" $DIR > "$OUTPUT" 2>&1; then
  echo "Build failed with the following error:"
  cat "$OUTPUT"
  exit 1
fi

# The expected heap markers.
markers=(
  "moved to heap: ch"
  "moved to heap: respOut"
  "moved to heap: respLen"
  "moved to heap: respCap"
  "moved to heap: ts"
)

# Flag to check if any marker is missing.
all_markers_found=true

# Check for each marker in the output.
for marker in "${markers[@]}"; do
  if ! grep -Fq "$marker" "$OUTPUT" ; then
    echo "Error: Expected marker not found: '$marker'"
    all_markers_found=false
  fi
done

if [ "$all_markers_found" = true ]; then
  echo "Success: All expected markers were found in the output."
  exit 0
else
  echo "One or more expected markers were not found."
  exit 1
fi

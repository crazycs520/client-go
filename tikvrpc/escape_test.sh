#!/bin/bash
# This script builds the tikvrpc package with debug flags and checks for specific heap allocation markers in the standard error output.
# TODO: The script produces unstable results on macOS, and the reason is currently unknown.
set -euo pipefail

# Determine the absolute path for the libtikv directory based on the test location.
# Assuming this script is placed in /root/workspace/tidbx-server/client-go/tikvrpc,
# the libtikv directory is two levels up ("../../libtikv").
LIBTIKV=$(realpath ../../libtikv)
if [ -z "$LIBTIKV" ]; then
  echo "Failed to resolve absolute path for libtikv directory."
  exit 1
fi

# Construct environmental variables required for the build.
export GO111MODULE=on
export CGO_ENABLED=1
export CGO_CFLAGS="-I/usr/local/include -I${LIBTIKV}"

# Run the build command with retrieval of allocation diagnostics.
# Capture the standard error output.
if ! output=$(go build -gcflags="-m -l" . 2>&1); then
  echo "Build failed with the following error:"
  echo "$output"
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
  if ! echo "$output" | grep -Fq "$marker"; then
    echo "Error: Expected marker not found: '$marker'"
    all_markers_found=false
  fi
done

if [ "$all_markers_found" = true ]; then
  echo "Success: All expected markers were found in the standard error output."
  exit 0
else
  echo "One or more expected markers were not found."
  exit 1
fi

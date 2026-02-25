#!/usr/bin/env bash
# -------------------------------------------------
# parallel_nccopy.sh
# Convert every *${include_pattern}*.nc in INPUT_DIR to NetCDF4‑Classic
# and write the result to OUTPUT_DIR
# -------------------------------------------------

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <input_dir> <output_dir> <include_pattern>"
    echo "include_pattern can be of the form clm2.h0a, cam.h0a, etc"
    exit 1
fi

INPUT_DIR=$(realpath "$1")
OUTPUT_DIR=$(realpath "$2")
include_pattern=$3

mkdir -p "$OUTPUT_DIR"

# How many jobs to run at once?  Use the number of CPU cores,
# or set it manually (e.g., -j 12).
NUM_JOBS=12

# Export variables for the subshell that parallel spawns
export OUTPUT_DIR

parallel -j "$NUM_JOBS" --bar \
    nccopy -k netCDF4_classic \
    "{}" "${OUTPUT_DIR}/{/.}.nc" \
    ::: "$INPUT_DIR"/*${include_pattern}*.nc

echo "All files converted → $OUTPUT_DIR"

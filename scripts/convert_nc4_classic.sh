#!/bin/bash

# Usage: ./convert_nc4_classic.sh <input_dir> <output_dir> <num_workers>
# Example: ./convert_nc4_classic.sh /path/to/input /path/to/output 8

INPUT_DIR=${1:?Usage: $0 input_dir output_dir num_workers}
OUTPUT_DIR=${2:?Usage: $0 input_dir output_dir num_workers}
NUM_WORKERS=${3:-4}

mkdir -p "$OUTPUT_DIR"

convert_file() {
    input_file="$1"
    output_dir="$2"
    filename=$(basename "$input_file")
    output_file="$output_dir/$filename"

    if [ -f "$output_file" ]; then
        echo "Skipping $filename - already exists"
        return
    fi

    echo "Converting $filename"
    ncks -7 "$input_file" "$output_file"
    if [ $? -eq 0 ]; then
        echo "Done: $filename"
    else
        echo "ERROR: failed to convert $filename"
    fi
}

export -f convert_file

find "$INPUT_DIR" -name "*h0a*.nc" | \
    parallel -j "$NUM_WORKERS" convert_file {} "$OUTPUT_DIR"

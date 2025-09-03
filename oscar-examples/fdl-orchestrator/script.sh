#!/bin/bash

# Debug: Show environment variables
echo "=== Environment Variables ==="
echo "INPUT_FILE_PATH: $INPUT_FILE_PATH"
echo "TMP_OUTPUT_DIR: $TMP_OUTPUT_DIR"
echo "MOUNT_PATH: $MOUNT_PATH"
echo "=============================="

# Sleep for 5 seconds
sleep 5

FILE_NAME=$(basename "$INPUT_FILE_PATH")

# Check if required environment variables are set
if [ -z "$INPUT_FILE_PATH" ]; then
    echo "ERROR: INPUT_FILE_PATH environment variable not set"
    exit 1
fi

if [ -z "$MOUNT_PATH" ]; then
    echo "ERROR: MOUNT_PATH environment variable not set"
    exit 1
fi

# Check if the mount path is available
echo "[script.sh] Checking if the mount path is available"
ls -lah /mnt


# Check if the input command script exists
if [ ! -f "$INPUT_FILE_PATH" ]; then
    echo "ERROR: Command script not found at $INPUT_FILE_PATH"
    exit 1
fi

echo "SCRIPT: Executing command script: $INPUT_FILE_PATH"


# Execute the command script with bash
# The command script will handle its own working directory and environment setup
# Redirect stdout to out.log and stderr to err.log
bash "$INPUT_FILE_PATH" 1> "$TMP_OUTPUT_DIR/$FILE_NAME.out.log" # 2> "$TMP_OUTPUT_DIR/$FILE_NAME.err.log"
exit_code=$?


echo "SCRIPT: Command completed with exit code: $exit_code"

# Create output file in TMP_OUTPUT_DIR for OSCAR to detect completion
if [ -n "$TMP_OUTPUT_DIR" ]; then
    OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME.exit_code"
    echo "$exit_code" > "$OUTPUT_FILE"
    echo "SCRIPT: Exit code written to: $OUTPUT_FILE"
fi

echo "Script completed."
exit $exit_code


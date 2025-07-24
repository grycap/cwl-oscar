#!/bin/bash

echo "Running run-command: File available in $INPUT_FILE_PATH"

# Sleeping for 10 seconds
echo "[script.sh] Sleeping for 10 seconds"
sleep 10

FILE_NAME=$(basename "$INPUT_FILE_PATH")
OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME.output"
ERROR_FILE="$TMP_OUTPUT_DIR/$FILE_NAME.error"

# Check if the mount path is available
echo "[script.sh] Checking if the mount path is available"
ls -lah /mnt

# Check if the output file is available
echo "[script.sh] Checking if the $MOUNT_PATH file is available"
ls -lah "$MOUNT_PATH"

# Execute the contents of the input file
`cat "$INPUT_FILE_PATH"` 1> "$MOUNT_PATH/$FILE_NAME.output" 2> "$ERROR_FILE"; echo $? > "$OUTPUT_FILE"

echo "Script completed."
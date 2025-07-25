#!/bin/bash

echo "Running run-script: File available in $INPUT_FILE_PATH"

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
ls -lah "$MOUNT_PATH" > "$MOUNT_PATH/file_check.log" 2>&1

# Execute the contents of the input file
chmod +x "$INPUT_FILE_PATH"
./"$INPUT_FILE_PATH" 1> "$OUTPUT_FILE" 2> "$ERROR_FILE"

echo "Script completed."
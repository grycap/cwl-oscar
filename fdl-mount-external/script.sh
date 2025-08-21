#!/bin/bash
echo "Running copy-input-mounted-vol-external: File available in $INPUT_FILE_PATH"
FILE_NAME=$(basename "$INPUT_FILE_PATH")
CURRENT_DATE=$(date +%Y%m%d-%H%M%S)
OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME-$CURRENT_DATE.txt"
echo "Copying input file $INPUT_FILE_PATH to mounted volume $MOUNT_PATH/$FILE_NAME"
cp "$INPUT_FILE_PATH" "$MOUNT_PATH/$FILE_NAME"
echo "List contents of $MOUNT_PATH:"
ls -l "$MOUNT_PATH" 
ls -l "$MOUNT_PATH" > $OUTPUT_FILE
echo "Script completed. Output file available at $MOUNT_PATH/$OUTPUT_FILE"

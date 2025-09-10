#!/usr/bin/env python3
# Copyright 2025 Universitat Politècnica de València and contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""OSCAR service script template for CWL execution."""

OSCAR_SERVICE_SCRIPT_TEMPLATE = '''#!/bin/bash

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
bash "$INPUT_FILE_PATH" > "$TMP_OUTPUT_DIR/$FILE_NAME.out.log" 2> "$TMP_OUTPUT_DIR/$FILE_NAME.err.log"
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
'''

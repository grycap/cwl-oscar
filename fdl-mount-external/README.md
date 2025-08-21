# Copy Input to Mounted Volume External Script

## Overview

This script (`script.sh`) performs a file copying operation with logging functionality. It's designed to copy an input file to a mounted external volume and create a timestamped log of the operation.

## Script Purpose

The `copy-input-mounted-vol-external` script receives a file in the input bucket, copies that file to the external shared bucket, and creates a new file (`filename-timestamp.txt`) in the output bucket.

The script relies on the following environment variables:

- `$INPUT_FILE_PATH`: Path to the source file to be copied
- `$MOUNT_PATH`: Path to the destination mounted volume
- `$TMP_OUTPUT_DIR`: Directory for temporary and output files

## Output Files

- **Copied file**: `$MOUNT_PATH/$FILE_NAME` (original filename preserved)
- **Log file**: `$TMP_OUTPUT_DIR/$FILE_NAME-YYYYMMDD-HHMMSS.txt` (timestamped directory listing)


## Output Example

```
-08-21 11:15:59,494 - supervisor - INFO - Storage event found.
2025-08-21 11:15:59,494 - supervisor - INFO - event_records = {'eventVersion': '2.0', 'eventSource': 'minio:s3', 'awsRegion': '', 'eventTime': '2025-08-21T11:15:56.918Z', 'eventName': 's3:ObjectCreated:Put', 'userIdentity': {'principalId': 'minio'}, 'requestParameters': {'principalId': 'minio', 'region': '', 'sourceIPAddress': '10.244.0.1'}, 'responseElements': {'x-amz-id-2': 'dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7…o-rpi.txt'
2025-08-21 11:16:00,812 - supervisor - INFO - Successful download of file 'in/hello-rpi.txt' from bucket 'copy-input-mounted-vol-external' in path '/tmp/tmpt0vm913s/hello-rpi.txt'
…-rw-rw---- 1 1000 users 24 Aug 21 11:16 hello-rpi.txt
-rw-rw---- 1 1000 users 14 Aug 19 11:14 hello.txt
-rw-rw---- 1 1000 users  0 Aug 21 10:42 test
-rw-rw---- 1 1000 users  0 Aug 21 11:04 test2
Script completed. Output file available at /mnt/shared-epitest//tmp/tmp5ziluzlb/hello-rpi.txt-20250821-111600.txt
```

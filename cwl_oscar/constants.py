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

"""Constants and configuration values for cwl-oscar."""

# Default values
DEFAULT_MEMORY = '1Gi'
DEFAULT_CPU = '1.0'
DEFAULT_DOCKER_IMAGE = 'opensourcefoundries/minideb:jessie'
DEFAULT_MOUNT_PATH = '/mnt/cwl2o-data/mount'
DEFAULT_CLUSTER_ID = 'oscar-cluster'

# Retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2  # seconds
DEFAULT_RETRY_MULTIPLIER = 2  # exponential backoff

# Timeout configuration
DEFAULT_UPLOAD_TIMEOUT = 300  # seconds
DEFAULT_CHECK_INTERVAL = 5  # seconds
DEFAULT_SERVICE_SETUP_WAIT = 3  # seconds

# Service naming
SERVICE_NAME_PREFIX = 'clt-'
SERVICE_HASH_LENGTH = 8

# Storage providers
DEFAULT_STORAGE_PROVIDER = 'minio.default'
SHARED_STORAGE_PROVIDER = 'minio.shared'
DEFAULT_REGION = 'us-east-1'

# File extensions
EXIT_CODE_EXTENSION = '.exit_code'
OUTPUT_EXTENSION = '.output'

# Logging prefixes
LOG_PREFIX_SERVICE_MANAGER = "OSCARServiceManager"
LOG_PREFIX_EXECUTOR = "OSCARExecutor"
LOG_PREFIX_JOB = "[job %s]"

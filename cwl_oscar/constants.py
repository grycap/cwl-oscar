#!/usr/bin/env python3


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

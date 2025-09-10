#!/bin/bash
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
# MinIO Bucket Cleanup Script Wrapper
# This script provides a convenient way to run the Python cleanup script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

MinIO Bucket Cleanup Script

OPTIONS:
    -e, --endpoint ENDPOINT    MinIO server endpoint (e.g., localhost:9000 or https://minio.example.com:9000)
    -a, --access-key KEY      MinIO access key
    -s, --secret-key KEY      MinIO secret key
    -b, --bucket NAME...      Name(s) of the bucket(s) to cleanup (can specify multiple)
    -r, --region REGION       AWS region (optional)
    -i, --insecure            Use HTTP instead of HTTPS
    -d, --dry-run             List objects without deleting (for testing)
    -w, --workers NUM         Maximum number of parallel workers (default: 10)
    -v, --verbose             Enable verbose logging
    -h, --help                Show this help message

EXAMPLES:
    # Basic usage with HTTPS (single bucket)
    $0 -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket

    # Multiple buckets
    $0 -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b bucket1 bucket2 bucket3

    # Basic usage with HTTP
    $0 -e http://localhost:9000 -a minioadmin -s minioadmin -b my-bucket

    # Use HTTP (insecure) - overrides protocol detection
    $0 -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket -i

    # Dry run to see what would be deleted (multiple buckets)
    $0 -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b bucket1 bucket2 -d

    # With custom region and workers
    $0 -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket -r us-east-1 -w 20

EOF
}

# Function to check if Python and required packages are available
check_dependencies() {
    print_info "Checking dependencies..."
    
    # Check if Python 3 is available
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed or not in PATH"
        exit 1
    fi
    
    # Check if minio package is available
    if ! python3 -c "import minio" &> /dev/null; then
        print_warning "MinIO Python package not found. Installing..."
        if [ -f "minio_requirements.txt" ]; then
            pip3 install -r minio_requirements.txt
        else
            pip3 install minio
        fi
    fi
    
    print_success "Dependencies check passed"
}

# Function to validate required arguments
validate_args() {
    if [ -z "$ENDPOINT" ]; then
        print_error "Endpoint is required"
        show_usage
        exit 1
    fi
    
    if [ -z "$ACCESS_KEY" ]; then
        print_error "Access key is required"
        show_usage
        exit 1
    fi
    
    if [ -z "$SECRET_KEY" ]; then
        print_error "Secret key is required"
        show_usage
        exit 1
    fi
    
    if [ ${#BUCKET[@]} -eq 0 ]; then
        print_error "At least one bucket name is required"
        show_usage
        exit 1
    fi
}

# Function to confirm deletion
confirm_deletion() {
    if [ "$DRY_RUN" = "true" ]; then
        print_info "DRY RUN MODE: No objects will be deleted"
        return 0
    fi
    
    # Handle multiple buckets in confirmation message
    if [ ${#BUCKET[@]} -eq 1 ]; then
        print_warning "WARNING: This will permanently delete ALL objects in bucket '$BUCKET' and then delete the bucket itself!"
    else
        print_warning "WARNING: This will permanently delete ALL objects in the following buckets and then delete the buckets themselves:"
        for bucket in "${BUCKET[@]}"; do
            print_warning "  - $bucket"
        done
    fi
    print_warning "This action cannot be undone!"
    
    read -p "Are you sure you want to continue? (yes/no): " -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi
    
    print_info "Proceeding with deletion..."
}

# Main script
main() {
    # Default values
    SECURE="true"
    DRY_RUN="false"
    WORKERS="10"
    VERBOSE="false"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--endpoint)
                ENDPOINT="$2"
                shift 2
                ;;
            -a|--access-key)
                ACCESS_KEY="$2"
                shift 2
                ;;
            -s|--secret-key)
                SECRET_KEY="$2"
                shift 2
                ;;
            -b|--bucket)
                # Collect all bucket names
                BUCKET=()
                shift
                while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do
                    BUCKET+=("$1")
                    shift
                done
                ;;
            -r|--region)
                REGION="$2"
                shift 2
                ;;
            -i|--insecure)
                SECURE="false"
                shift
                ;;
            -d|--dry-run)
                DRY_RUN="true"
                shift
                ;;
            -w|--workers)
                WORKERS="$2"
                shift 2
                ;;
            -v|--verbose)
                VERBOSE="true"
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Validate arguments
    validate_args
    
    # Check dependencies
    check_dependencies
    
    # Confirm deletion
    confirm_deletion
    
    # Build command
    CMD="python3 minio_bucket_cleanup.py"
    
    # Clean endpoint - remove protocol prefix if present
    if [[ "$ENDPOINT" == https://* ]]; then
        CLEAN_ENDPOINT="${ENDPOINT#https://}"
        SECURE="true"
    elif [[ "$ENDPOINT" == http://* ]]; then
        CLEAN_ENDPOINT="${ENDPOINT#http://}"
        SECURE="false"
    else
        CLEAN_ENDPOINT="$ENDPOINT"
    fi
    

    CMD="$CMD --endpoint '$CLEAN_ENDPOINT'"
    
    CMD="$CMD --access-key '$ACCESS_KEY'"
    CMD="$CMD --secret-key '$SECRET_KEY'"
    
    # Add all bucket names
    for bucket in "${BUCKET[@]}"; do
        CMD="$CMD --bucket '$bucket'"
    done
    
    CMD="$CMD --max-workers $WORKERS"
    

    
    if [ "$SECURE" = "false" ]; then
        CMD="$CMD --insecure"
    fi
    
    if [ -n "$REGION" ]; then
        CMD="$CMD --region '$REGION'"
    fi
    
    if [ "$DRY_RUN" = "true" ]; then
        CMD="$CMD --dry-run"
    fi
    
    if [ "$VERBOSE" = "true" ]; then
        CMD="$CMD --verbose"
    fi
    
    # Debug output
    print_info "Buckets to cleanup:"
    for bucket in "${BUCKET[@]}"; do
        print_info "  - $bucket"
    done
    echo
    
    # Execute the command
    print_info "Executing: $CMD"
    echo
    
    eval $CMD
    
    if [ $? -eq 0 ]; then
        print_success "MinIO bucket cleanup completed successfully!"
    else
        print_error "MinIO bucket cleanup failed!"
        exit 1
    fi
}

# Run main function with all arguments
main "$@"

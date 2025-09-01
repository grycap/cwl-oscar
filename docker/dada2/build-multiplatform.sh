#!/bin/bash

# Multi-platform Docker build script for DADA2
# Builds for both amd64 and arm64 architectures

set -e

# Default configuration
DEFAULT_REGISTRY="robertbio"
VERSION="1.30.0"
PLATFORMS="linux/amd64,linux/arm64"

# Parse command line arguments
REGISTRY="$DEFAULT_REGISTRY"
while [[ $# -gt 0 ]]; do
    case $1 in
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--registry REGISTRY]"
            echo ""
            echo "Options:"
            echo "  --registry REGISTRY    Docker registry to push to (default: $DEFAULT_REGISTRY)"
            echo "  -h, --help            Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                           # Push to $DEFAULT_REGISTRY/dada2"
            echo "  $0 --registry myregistry     # Push to myregistry/dada2"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Set image name based on registry
IMAGE_NAME="${REGISTRY}/dada2"

echo "Building multi-platform DADA2 Docker image"
echo "==========================================="
echo "Image: ${IMAGE_NAME}:${VERSION}"
echo "Platforms: ${PLATFORMS}"
echo ""

# Check if buildx is available
if ! docker buildx version > /dev/null 2>&1; then
    echo "❌ Docker buildx is not available. Please install Docker Desktop or enable buildx."
    exit 1
fi

# Create and use a new builder instance for multi-platform builds
echo "Setting up buildx builder..."
docker buildx create --name dada2-builder --use --bootstrap || true
docker buildx use dada2-builder

# Build and push multi-platform image
echo "Building and pushing multi-platform image..."
docker buildx build \
    --platform ${PLATFORMS} \
    --tag ${IMAGE_NAME}:${VERSION} \
    --tag ${IMAGE_NAME}:latest \
    --push \
    .

echo ""
echo "✅ Multi-platform build completed successfully!"
echo ""
echo "Image tags created:"
echo "  ${IMAGE_NAME}:${VERSION}"
echo "  ${IMAGE_NAME}:latest"
echo ""
echo "Supported platforms:"
echo "  linux/amd64"
echo "  linux/arm64"
echo ""
echo "To test the image:"
echo "  docker run --rm ${IMAGE_NAME}:latest R -e 'library(dada2); library(ShortRead)'"

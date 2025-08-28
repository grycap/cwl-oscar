#!/bin/bash

# * Build all custom Debian-based Docker images for malaria surveillance tools
# Usage: ./build_all.sh [--multiplatform]

set -e

# * Parse arguments
MULTIPLATFORM=false
PLATFORMS="linux/amd64"

if [[ "$1" == "--multiplatform" ]]; then
    MULTIPLATFORM=true
    PLATFORMS="linux/amd64,linux/arm64"
    echo "Building multi-platform images for: $PLATFORMS"
else
    echo "Building single-platform images for: $PLATFORMS"
    echo "Use --multiplatform flag to build for both amd64 and arm64"
fi

echo "Building custom Debian-based Docker images..."

# ! Navigate to dockerfiles directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# * Setup buildx for multi-platform builds if needed
if [[ "$MULTIPLATFORM" == true ]]; then
    echo "Setting up Docker buildx for multi-platform builds..."
    
    # * Create and use a new builder instance if it doesn't exist
    if ! docker buildx ls | grep -q "multiarch-builder"; then
        docker buildx create --name multiarch-builder --use --bootstrap
    else
        docker buildx use multiarch-builder
    fi
    
    # * Install QEMU for emulation
    docker run --privileged --rm tonistiigi/binfmt --install all
    
    BUILD_CMD="docker buildx build --platform $PLATFORMS"
else
    BUILD_CMD="docker build --platform $PLATFORMS"
fi

# * Build NanoFilt image
echo "Building NanoFilt image..."
cd nanofilt
$BUILD_CMD -t robertbio/nanofilt:2.8.0-debian .
cd ..

# * Build Minimap2 image
echo "Building Minimap2 image..."
cd minimap2
$BUILD_CMD -t robertbio/minimap2:2.24-debian .
cd ..

# * Build Samtools image
echo "Building Samtools image..."
cd samtools
$BUILD_CMD -t robertbio/samtools:1.16.1-debian .
cd ..

# * Build BCFtools image
echo "Building BCFtools image..."
cd bcftools
$BUILD_CMD -t robertbio/bcftools:1.16-debian .
cd ..

echo "All Docker images built successfully!"

# * List built images
echo "Built images:"
docker images | grep robertbio

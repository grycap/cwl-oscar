#!/bin/bash

# * Test individual tool builds for debugging
# Usage: ./test-single-tool.sh [tool_name] [--multiplatform]

set -e

TOOL_NAME="$1"
MULTIPLATFORM="$2"

if [ -z "$TOOL_NAME" ]; then
    echo "Usage: $0 [nanofilt|minimap2|samtools|bcftools] [--multiplatform]"
    echo "Available tools:"
    echo "  nanofilt   - NanoFilt quality filtering"  
    echo "  minimap2   - Minimap2 sequence alignment"
    echo "  samtools   - SAM/BAM processing"
    echo "  bcftools   - Variant calling"
    echo "  summary    - Summary analysis with pandas"
    exit 1
fi

if [[ "$MULTIPLATFORM" == "--multiplatform" ]]; then
    PLATFORMS="linux/amd64,linux/arm64"
    BUILD_CMD="docker buildx build --platform $PLATFORMS"
    echo "Building $TOOL_NAME for multi-platform: $PLATFORMS"
else
    PLATFORMS="linux/amd64"
    BUILD_CMD="docker build --platform $PLATFORMS"  
    echo "Building $TOOL_NAME for single platform: $PLATFORMS"
fi

case "$TOOL_NAME" in
    nanofilt)
        $BUILD_CMD -t malaria-tools/nanofilt:2.8.0-debian ./nanofilt/
        ;;
    minimap2)
        $BUILD_CMD -t malaria-tools/minimap2:2.24-debian ./minimap2/
        ;;  
    samtools)
        $BUILD_CMD -t malaria-tools/samtools:1.16.1-debian ./samtools/
        ;;
    bcftools)
        $BUILD_CMD -t malaria-tools/bcftools:1.16-debian ./bcftools/
        ;;
    *)
        echo "Unknown tool: $TOOL_NAME"
        exit 1
        ;;
esac

echo "$TOOL_NAME build completed!"

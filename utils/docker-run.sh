#!/bin/bash

# Docker helper script for cwl-oscar
# This script provides convenient commands to build, run, and test cwl-oscar in Docker

set -e

# Default values
DEFAULT_DOCKER_REGISTRY="robertbio"
DOCKER_REGISTRY="${DEFAULT_DOCKER_REGISTRY}"
DOCKER_IMAGE="cwl-oscar:latest"

# Parse registry parameter if provided
if [[ "$1" == "--registry" ]] && [[ -n "$2" ]]; then
    DOCKER_REGISTRY="$2"
    shift 2  # Remove --registry and its value from arguments
    echo "Using custom registry: $DOCKER_REGISTRY"
elif [[ "$1" == --registry=* ]]; then
    DOCKER_REGISTRY="${1#--registry=}"
    shift    # Remove --registry=value from arguments
    echo "Using custom registry: $DOCKER_REGISTRY"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# * Function to update build information
update_build_info() {
    local build_info_file="cwl_oscar/.build_info"
    local current_time=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
    local git_revision="unknown"
    
    # Try to get git revision
    if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
        git_revision=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    fi
    
    echo -e "${YELLOW}Updating build info: $current_time (git: $git_revision)${NC}"
    
    # Create build info file with key=value format
    cat > "$build_info_file" << EOF
BUILD_TIME=$current_time
GIT_REVISION=$git_revision
EOF
    
    echo -e "${GREEN}✓ Created build info file: $build_info_file${NC}"
}

print_usage() {
    echo "Usage: $0 [--registry REGISTRY] {build|build-linux|build-multi|build-push-multi|run|test|security-check|push|pull|examples|help}"
    echo ""
    echo "Options:"
    echo "  --registry REGISTRY   - Set Docker registry (default: $DEFAULT_DOCKER_REGISTRY)"
    echo "                         Can also use --registry=REGISTRY format"
    echo ""
    echo "Commands:"
    echo "  build         - Build the cwl-oscar Docker image (current platform)"
    echo "  build-linux   - Build specifically for linux/amd64"
    echo "  build-multi   - Build for multiple platforms (linux/amd64, linux/arm64)"
    echo "  build-push-multi - Build and push multi-platform image to registry"
    echo "  run           - Run cwl-oscar with your arguments"
    echo "  test          - Run basic tests to verify the image works"
    echo "  security-check- Check for sensitive files in Docker build context"
    echo "  push          - Push the image to a registry"
    echo "  pull          - Pull the image from a registry"
    echo "  examples      - Show example usage commands"
    echo "  shell         - Start an interactive shell in the container"
    echo "  help          - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build-linux                              # Build for Linux AMD64 (default registry)"
    echo "  $0 --registry myuser build-multi            # Build for multiple platforms with custom registry"
    echo "  $0 --registry=company/project push          # Push to custom registry"
    echo "  $0 build-push-multi                         # Build and push multi-platform image to registry"
    echo "  $0 security-check                           # Check for sensitive files"
    echo "  $0 run --help"
    echo "  $0 run --cluster-endpoint https://oscar.example.com --cluster-token TOKEN workflow.cwl input.json"
    echo "  $0 test"
}

build_image() {
    echo -e "${BLUE}Building cwl-oscar Docker image (current platform)...${NC}"
    echo -e "${YELLOW}Using registry: $DOCKER_REGISTRY${NC}"
    
    # * Update build info before building
    update_build_info
    
    docker build --no-cache -t "$DOCKER_IMAGE" .
    echo -e "${GREEN}✓ Image built successfully: $DOCKER_IMAGE${NC}"
}

build_linux_image() {
    echo -e "${BLUE}Building cwl-oscar Docker image for linux/amd64...${NC}"
    echo -e "${YELLOW}Using registry: $DOCKER_REGISTRY${NC}"
    
    # * Update build info before building
    update_build_info
    
    docker build --platform linux/amd64 --no-cache -t "cwl-oscar:linux-amd64" .
    
    # Also tag as latest for convenience
    docker tag "cwl-oscar:linux-amd64" "$DOCKER_IMAGE"
    
    # Tag for pushing to registry
    docker tag "cwl-oscar:linux-amd64" "$DOCKER_REGISTRY/cwl-oscar:linux-amd64"
    
    echo -e "${GREEN}✓ Linux AMD64 image built successfully${NC}"
    echo -e "${BLUE}Tagged as: cwl-oscar:linux-amd64, $DOCKER_IMAGE, and $DOCKER_REGISTRY/cwl-oscar:linux-amd64${NC}"
    
    # Show image details
    echo -e "${YELLOW}Image details:${NC}"
    docker inspect cwl-oscar:linux-amd64 --format '{{.Architecture}}/{{.Os}} - {{.Size}} bytes' 2>/dev/null || echo "Could not get image details"
}

build_multi_platform() {
    echo -e "${BLUE}Building cwl-oscar Docker image for multiple platforms...${NC}"
    echo -e "${YELLOW}This will build for linux/amd64 and linux/arm64${NC}"
    
    # * Update build info before building
    update_build_info
    
    # Create or use existing buildx builder
    if ! docker buildx inspect multiplatform-builder >/dev/null 2>&1; then
        echo -e "${YELLOW}Creating multiplatform builder...${NC}"
        docker buildx create --name multiplatform-builder --use
    else
        echo -e "${YELLOW}Using existing multiplatform builder...${NC}"
        docker buildx use multiplatform-builder
    fi
    
    # Build individual platform images
    echo -e "${YELLOW}Building AMD64 image...${NC}"
    docker buildx build \
        --platform linux/amd64 \
        --no-cache \
        -t "$DOCKER_REGISTRY/cwl-oscar:amd64" \
        --load \
        .
    
    echo -e "${YELLOW}Building ARM64 image...${NC}"
    docker buildx build \
        --platform linux/arm64 \
        --no-cache \
        -t "$DOCKER_REGISTRY/cwl-oscar:arm64" \
        --load \
        .
    
    # Tag for convenience
    docker tag "$DOCKER_REGISTRY/cwl-oscar:amd64" "$DOCKER_IMAGE"
    
    echo -e "${GREEN}✓ Multi-platform images built successfully${NC}"
    echo -e "${BLUE}Built: $DOCKER_REGISTRY/cwl-oscar:amd64${NC}"
    echo -e "${BLUE}Built: $DOCKER_REGISTRY/cwl-oscar:arm64${NC}"
    echo -e "${BLUE}Tagged latest as: $DOCKER_IMAGE${NC}"
}

build_and_push_multiplatform() {
    
    echo -e "${BLUE}Building and pushing multi-platform image to registry...${NC}"
    
    # * Update build info before building
    update_build_info
    
    REMOTE_TAG="$DOCKER_REGISTRY/cwl-oscar:latest"
    
    # Create or use existing buildx builder
    if ! docker buildx inspect multiplatform-builder >/dev/null 2>&1; then
        echo -e "${YELLOW}Creating multiplatform builder...${NC}"
        docker buildx create --name multiplatform-builder --use
    else
        echo -e "${YELLOW}Using existing multiplatform builder...${NC}"
        docker buildx use multiplatform-builder
    fi
    
    # Build and push for multiple platforms
    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        --no-cache \
        -t "$REMOTE_TAG" \
        --push \
        .
    
    echo -e "${GREEN}✓ Multi-platform image built and pushed successfully${NC}"
    echo -e "${BLUE}Available at: $REMOTE_TAG${NC}"
    echo -e "${BLUE}Platforms: linux/amd64, linux/arm64${NC}"
}

run_cwl_oscar() {
    echo -e "${BLUE}Running cwl-oscar with arguments: $*${NC}"
    docker run --rm -v "$(pwd)":/workspace -w /workspace "$DOCKER_IMAGE" "$@"
}

test_image() {
    echo -e "${BLUE}Testing cwl-oscar Docker image...${NC}"
    
    echo -e "${YELLOW}1. Testing help command...${NC}"
    if docker run --rm "$DOCKER_IMAGE" --help > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Help command works${NC}"
    else
        echo -e "${RED}✗ Help command failed${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}2. Testing version command...${NC}"
    VERSION=$(docker run --rm "$DOCKER_IMAGE" --version 2>/dev/null || echo "failed")
    if [[ "$VERSION" == *"cwl-oscar"* ]]; then
        echo -e "${GREEN}✓ Version command works: $VERSION${NC}"
    else
        echo -e "${RED}✗ Version command failed${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}3. Testing container file structure...${NC}"
    EXAMPLES=$(docker run --rm --entrypoint sh "$DOCKER_IMAGE" -c "ls examples/ | wc -l" 2>/dev/null || echo "0")
    if [[ "$EXAMPLES" -gt 0 ]]; then
        echo -e "${GREEN}✓ Examples directory contains $EXAMPLES files${NC}"
    else
        echo -e "${RED}✗ Examples directory not found or empty${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}4. Testing CWL parsing (dry run)...${NC}"
    # Test with a simple example but expect it to fail due to missing OSCAR credentials
    # The important thing is that it parses the CWL file correctly
    ERROR_OUTPUT=$(docker run --rm "$DOCKER_IMAGE" examples/date.cwl examples/empty_input.json 2>&1 || true)
    if [[ "$ERROR_OUTPUT" == *"either --cluster-token or --cluster-username is required"* ]]; then
        echo -e "${GREEN}✓ CWL parsing works (expected auth error)${NC}"
    else
        echo -e "${RED}✗ Unexpected error in CWL parsing:${NC}"
        echo "$ERROR_OUTPUT"
        exit 1
    fi
    
    echo -e "${YELLOW}5. Testing image architecture...${NC}"
    ARCH=$(docker inspect "$DOCKER_IMAGE" --format '{{.Architecture}}/{{.Os}}' 2>/dev/null || echo "unknown")
    echo -e "${GREEN}✓ Image architecture: $ARCH${NC}"
    
    echo -e "${GREEN}All tests passed! 🎉${NC}"
    echo -e "${BLUE}The cwl-oscar Docker image is ready to use.${NC}"
}

push_image() {
    echo -e "${BLUE}Pushing multiplatform images to registry: $DOCKER_REGISTRY${NC}"
    
    # Check if user is logged in to Docker registry
    if ! docker info | grep -q "Username:"; then
        echo -e "${YELLOW}Warning: You don't appear to be logged in to Docker registry.${NC}"
        echo -e "${YELLOW}Please run: docker login${NC}"
        echo -e "${YELLOW}Or if using a different registry: docker login your-registry.com${NC}"
        read -p "Do you want to continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${RED}Push cancelled. Please login first.${NC}"
            exit 1
        fi
    else
        CURRENT_USER=$(docker info | grep "Username:" | awk '{print $2}')
        echo -e "${GREEN}✓ Logged in as: $CURRENT_USER${NC}"
        if [[ "$CURRENT_USER" != "$DOCKER_REGISTRY" ]] && [[ "$DOCKER_REGISTRY" != *"/"* ]]; then
            echo -e "${YELLOW}Warning: Logged in as '$CURRENT_USER' but pushing to '$DOCKER_REGISTRY'${NC}"
            echo -e "${YELLOW}Make sure you have permission to push to this registry.${NC}"
        fi
    fi
    
    # Check if we have multiplatform images
    if docker image inspect "$DOCKER_REGISTRY/cwl-oscar:amd64" >/dev/null 2>&1 && \
       docker image inspect "$DOCKER_REGISTRY/cwl-oscar:arm64" >/dev/null 2>&1; then
        echo -e "${YELLOW}Found multiplatform images, pushing both and creating manifest...${NC}"
        
        # Push individual platform images
        echo -e "${BLUE}Pushing AMD64 image...${NC}"
        docker push "$DOCKER_REGISTRY/cwl-oscar:amd64"
        
        echo -e "${BLUE}Pushing ARM64 image...${NC}"
        docker push "$DOCKER_REGISTRY/cwl-oscar:arm64"
        
        # Create and push manifest list
        echo -e "${BLUE}Creating manifest list for latest tag...${NC}"
        docker manifest create --amend "$DOCKER_REGISTRY/cwl-oscar:latest" \
            "$DOCKER_REGISTRY/cwl-oscar:amd64" \
            "$DOCKER_REGISTRY/cwl-oscar:arm64"
        
        echo -e "${BLUE}Pushing manifest list...${NC}"
        docker manifest push "$DOCKER_REGISTRY/cwl-oscar:latest"
        
        echo -e "${GREEN}✓ Multiplatform image pushed successfully as $DOCKER_REGISTRY/cwl-oscar:latest${NC}"
        echo -e "${BLUE}Platforms: linux/amd64, linux/arm64${NC}"
    else
        # Fall back to single image push
        REMOTE_TAG="$DOCKER_REGISTRY/cwl-oscar:latest"
        echo -e "${BLUE}Tagging image as $REMOTE_TAG...${NC}"
        docker tag "$DOCKER_IMAGE" "$REMOTE_TAG"
        
        echo -e "${BLUE}Pushing to registry...${NC}"
        docker push "$REMOTE_TAG"
        echo -e "${GREEN}✓ Image pushed successfully${NC}"
    fi
}

pull_image() {
    REMOTE_TAG="$DOCKER_REGISTRY/cwl-oscar:latest"
    echo -e "${BLUE}Pulling from registry...${NC}"
    docker pull "$REMOTE_TAG"
    docker tag "$REMOTE_TAG" "$DOCKER_IMAGE"
    echo -e "${GREEN}✓ Image pulled successfully${NC}"
}

show_examples() {
    echo -e "${BLUE}CWL-OSCAR Docker Usage Examples${NC}"
    echo ""
    echo -e "${YELLOW}Build Options:${NC}"
    echo "   $0 build           # Build for current platform"
    echo "   $0 build-linux     # Build specifically for linux/amd64"
    echo "   $0 build-multi     # Build for linux/amd64 + linux/arm64"
    echo "   $0 build-push-multi# Build and push multi-platform image to registry"
    echo ""
    echo -e "${YELLOW}1. Run with OSCAR token authentication:${NC}"
    echo "   $0 run --cluster-endpoint https://oscar.example.com \\"
    echo "          --cluster-token YOUR_TOKEN \\"
    echo "          workflow.cwl input.json"
    echo ""
    echo -e "${YELLOW}2. Run with username/password authentication:${NC}"
    echo "   $0 run --cluster-endpoint https://oscar.example.com \\"
    echo "          --cluster-username your_user \\"
    echo "          --cluster-password your_pass \\"
    echo "          workflow.cwl input.json"
    echo ""
    echo -e "${YELLOW}3. Run with custom mount path and service:${NC}"
    echo "   $0 run --cluster-endpoint https://oscar.example.com \\"
    echo "          --cluster-token YOUR_TOKEN \\"
    echo "          --mount-path /custom/mount/path \\"
    echo "          --service-name my-service \\"
    echo "          workflow.cwl input.json"
    echo ""
    echo -e "${YELLOW}4. Run with debug output:${NC}"
    echo "   $0 run --cluster-endpoint https://oscar.example.com \\"
    echo "          --cluster-token YOUR_TOKEN \\"
    echo "          --debug \\"
    echo "          workflow.cwl input.json"
    echo ""
    echo -e "${YELLOW}5. Mount local directory for input/output files:${NC}"
    echo "   docker run --rm -v \$(pwd):/workspace -w /workspace \\"
    echo "       $DOCKER_IMAGE \\"
    echo "       --cluster-endpoint https://oscar.example.com \\"
    echo "       --cluster-token YOUR_TOKEN \\"
    echo "       /workspace/workflow.cwl /workspace/input.json"
    echo ""
    echo -e "${YELLOW}6. Use with environment variables:${NC}"
    echo "   export CLUSTER_ENDPOINT=https://oscar.example.com"
    echo "   export CLUSTER_TOKEN=your_token_here"
    echo "   docker run --rm -v \$(pwd):/workspace -w /workspace \\"
    echo "       -e CLUSTER_ENDPOINT -e CLUSTER_TOKEN \\"
    echo "       $DOCKER_IMAGE workflow.cwl input.json"
    echo ""
    echo -e "${BLUE}Available examples in the container:${NC}"
    docker run --rm --entrypoint sh "$DOCKER_IMAGE" -c "ls examples/"
}

start_shell() {
    echo -e "${BLUE}Starting interactive shell in cwl-oscar container...${NC}"
    echo -e "${YELLOW}You can explore the container and run cwl-oscar commands manually.${NC}"
    echo -e "${YELLOW}Type 'exit' to return to your host shell.${NC}"
    docker run --rm -it -v "$(pwd)":/workspace -w /workspace --entrypoint sh "$DOCKER_IMAGE"
}

run_security_check() {
    echo -e "${BLUE}Running Docker security check...${NC}"
    if [[ -f "./docker-security-check.sh" ]]; then
        ./docker-security-check.sh
    else
        echo -e "${RED}Error: docker-security-check.sh not found${NC}"
        echo "Make sure you're in the cwl-oscar project directory"
        exit 1
    fi
}

# Main script logic
case "${1:-help}" in
    build)
        build_image
        ;;
    build-linux)
        build_linux_image
        ;;
    build-multi)
        build_multi_platform
        ;;
    build-push-multi)
        build_and_push_multiplatform
        ;;
    run)
        shift
        run_cwl_oscar "$@"
        ;;
    test)
        test_image
        ;;
    security-check)
        run_security_check
        ;;
    push)
        push_image
        ;;
    pull)
        pull_image
        ;;
    examples)
        show_examples
        ;;
    shell)
        start_shell
        ;;
    help|--help|-h)
        print_usage
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        print_usage
        exit 1
        ;;
esac 
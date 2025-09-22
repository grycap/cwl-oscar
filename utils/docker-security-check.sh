#!/bin/bash

# Docker Security Check Script
# This script validates that no sensitive files are included in the Docker build context

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔒 Docker Security Check for cwl-oscar${NC}"
echo "================================================"

# Function to check for potentially sensitive files
check_sensitive_files() {
    echo -e "${YELLOW}Checking for potentially sensitive files in build context...${NC}"
    
    # Get list of files that would be included in Docker build context
    # This simulates what Docker would include, respecting .dockerignore
    BUILD_CONTEXT=$(docker build --dry-run . 2>&1 | grep "=> \[internal\] load build context" -A 100 | grep "transferring context" || echo "")
    
    # Alternative approach: use tar to see what would be sent to Docker daemon
    echo -e "${BLUE}Files that would be included in Docker build context:${NC}"
    echo "---------------------------------------------------"
    
    # Create a temporary tar file to see what Docker would include
    TEMP_TAR=$(mktemp)
    tar --exclude-from=.dockerignore -cf "$TEMP_TAR" . 2>/dev/null || true
    
    # List contents of the tar file
    tar -tf "$TEMP_TAR" | head -20
    TOTAL_FILES=$(tar -tf "$TEMP_TAR" | wc -l)
    echo "... (showing first 20 of $TOTAL_FILES total files)"
    
    # Check for sensitive patterns in the included files
    echo -e "\n${YELLOW}Scanning for sensitive file patterns...${NC}"
    
    SENSITIVE_PATTERNS=(
        "password"
        "secret"
        "token" 
        "credential"
        "key"
        "auth"
        ".env"
        "config.json"
        "config.yaml"
        "id_rsa"
        "id_dsa"
        ".pem"
        ".pfx"
        ".p12"
        "aws"
        "gcloud"
        "azure"
    )
    
    FOUND_SENSITIVE=false
    
    for pattern in "${SENSITIVE_PATTERNS[@]}"; do
        MATCHES=$(tar -tf "$TEMP_TAR" | grep -i "$pattern" || true)
        if [[ ! -z "$MATCHES" ]]; then
            echo -e "${RED}⚠️  Found potentially sensitive files matching pattern '$pattern':${NC}"
            echo "$MATCHES" | sed 's/^/    /'
            FOUND_SENSITIVE=true
        fi
    done
    
    # Clean up
    rm -f "$TEMP_TAR"
    
    if [[ "$FOUND_SENSITIVE" == "false" ]]; then
        echo -e "${GREEN}✅ No obviously sensitive files detected in build context${NC}"
    else
        echo -e "${RED}❌ Found potentially sensitive files! Review .dockerignore${NC}"
        return 1
    fi
}

# Function to check .dockerignore coverage
check_dockerignore_coverage() {
    echo -e "\n${YELLOW}Checking .dockerignore security coverage...${NC}"
    
    REQUIRED_PATTERNS=(
        "*.env"
        "*secret*"
        "*password*" 
        "*token*"
        "*credential*"
        "*.key"
        "*.pem"
        ".ssh/"
        ".aws/"
        "id_rsa"
    )
    
    MISSING_PATTERNS=()
    
    for pattern in "${REQUIRED_PATTERNS[@]}"; do
        if ! grep -q "$pattern" .dockerignore; then
            MISSING_PATTERNS+=("$pattern")
        fi
    done
    
    if [[ ${#MISSING_PATTERNS[@]} -eq 0 ]]; then
        echo -e "${GREEN}✅ .dockerignore has good security coverage${NC}"
    else
        echo -e "${YELLOW}⚠️  Consider adding these patterns to .dockerignore:${NC}"
        for pattern in "${MISSING_PATTERNS[@]}"; do
            echo -e "    $pattern"
        done
    fi
}

# Function to check Dockerfile security
check_dockerfile_security() {
    echo -e "\n${YELLOW}Checking Dockerfile security practices...${NC}"
    
    # Check if Dockerfile uses specific COPY commands (good)
    if grep -q "COPY \. \." Dockerfile; then
        echo -e "${RED}❌ Dockerfile uses 'COPY . .' which copies everything${NC}"
        echo "   Consider using specific COPY commands instead"
    else
        echo -e "${GREEN}✅ Dockerfile uses specific COPY commands${NC}"
    fi
    
    # Check if running as non-root user
    if grep -q "USER" Dockerfile; then
        echo -e "${GREEN}✅ Dockerfile runs as non-root user${NC}"
    else
        echo -e "${YELLOW}⚠️  Consider adding a non-root user to Dockerfile${NC}"
    fi
    
    # Check for hardcoded secrets in Dockerfile
    echo -e "\n${YELLOW}Checking for hardcoded secrets in Dockerfile...${NC}"
    SECRET_PATTERNS=("password" "token" "secret" "key")
    FOUND_SECRETS=false
    
    for pattern in "${SECRET_PATTERNS[@]}"; do
        MATCHES=$(grep -ni "$pattern" Dockerfile || true)
        if [[ ! -z "$MATCHES" ]]; then
            # Filter out known safe patterns
            FILTERED_MATCHES=$(echo "$MATCHES" | grep -v -E "(disabled-password|--disabled-password)" || true)
            if [[ ! -z "$FILTERED_MATCHES" ]]; then
                echo -e "${RED}❌ Found potential secret reference in Dockerfile: $pattern${NC}"
                echo "$FILTERED_MATCHES" | sed 's/^/    /'
                FOUND_SECRETS=true
            fi
        fi
    done
    
    if [[ "$FOUND_SECRETS" == "false" ]]; then
        echo -e "${GREEN}✅ No hardcoded secrets found in Dockerfile${NC}"
    fi
}

# Function to check for common sensitive files in the directory
check_common_sensitive_files() {
    echo -e "\n${YELLOW}Checking for common sensitive files in project directory...${NC}"
    
    SENSITIVE_FILES=(
        ".env"
        ".env.local"
        "config.json"
        "secrets.json"
        "credentials.json"
        "id_rsa"
        "id_dsa"
        "private_key.pem"
        "service-account.json"
        "gcloud-key.json"
        "aws-credentials"
        ".aws/credentials"
        "passwords.txt"
        "secrets.txt"
    )
    
    FOUND_FILES=()
    
    for file in "${SENSITIVE_FILES[@]}"; do
        if [[ -f "$file" ]]; then
            FOUND_FILES+=("$file")
        fi
    done
    
    if [[ ${#FOUND_FILES[@]} -eq 0 ]]; then
        echo -e "${GREEN}✅ No common sensitive files found in project directory${NC}"
    else
        echo -e "${RED}❌ Found sensitive files in project directory:${NC}"
        for file in "${FOUND_FILES[@]}"; do
            echo -e "    $file"
        done
        echo -e "${YELLOW}Make sure these are properly excluded in .dockerignore${NC}"
    fi
}

# Function to provide security recommendations
print_security_recommendations() {
    echo -e "\n${BLUE}🔒 Security Recommendations${NC}"
    echo "=================================="
    echo "1. Never include credentials directly in Docker images"
    echo "2. Use environment variables or secrets management for runtime credentials"
    echo "3. Regularly audit .dockerignore for security coverage"
    echo "4. Use specific COPY commands instead of 'COPY . .'"
    echo "5. Run containers as non-root users"
    echo "6. Use multi-stage builds to exclude development dependencies"
    echo "7. Scan images for vulnerabilities before deployment"
    echo ""
    echo -e "${YELLOW}For cwl-oscar specifically:${NC}"
    echo "- OSCAR credentials should be passed via --oscar-token or environment variables"
    echo "- Never bake OSCAR endpoints or tokens into the image"
    echo "- Mount credential files at runtime, don't include them in the image"
}

# Main execution
main() {
    check_sensitive_files
    check_dockerignore_coverage  
    check_dockerfile_security
    check_common_sensitive_files
    print_security_recommendations
    
    echo -e "\n${GREEN}🔒 Security check completed!${NC}"
    echo "Review any warnings above and update .dockerignore if needed."
}

# Run the security check
main 
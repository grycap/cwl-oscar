# * Minimap2 Debian-based Docker Image  
# Uses debian:bookworm-slim as base image for multi-platform support
FROM debian:bookworm-slim

# * Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# * Update package lists and install build dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    zlib1g-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# * Set working directory for compilation
WORKDIR /usr/local/src

# * Download and compile minimap2 from source
RUN wget https://github.com/lh3/minimap2/releases/download/v2.24/minimap2-2.24.tar.bz2 && \
    tar -xjf minimap2-2.24.tar.bz2 && \
    cd minimap2-2.24 && \
    # * Use generic compilation for ARM64 compatibility (disable SIMD optimizations)
    if [ "$(uname -m)" = "aarch64" ]; then \
        make sse2=no arm_neon=no; \
    else \
        make; \
    fi && \
    cp minimap2 /usr/local/bin/ && \
    cd .. && \
    rm -rf minimap2-2.24 minimap2-2.24.tar.bz2

# * Set working directory for execution
WORKDIR /data


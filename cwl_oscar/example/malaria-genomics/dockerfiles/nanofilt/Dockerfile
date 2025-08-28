# * NanoFilt Debian-based Docker Image
# Uses debian:bookworm-slim as base image for multi-platform support
FROM debian:bookworm-slim

# * Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# * Update package lists and install system dependencies
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-dev \
    build-essential \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# * Install NanoFilt via pip (using --break-system-packages for container)
RUN python3 -m pip install --upgrade pip --break-system-packages && \
    python3 -m pip install NanoFilt==2.8.0 --break-system-packages

# * Ensure gunzip is available (from gzip package)
RUN apt-get update && \
    apt-get install -y gzip \
    && rm -rf /var/lib/apt/lists/*

# * Set working directory
WORKDIR /data

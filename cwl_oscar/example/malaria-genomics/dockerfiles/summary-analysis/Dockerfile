# * Summary Analysis Python Container
# Uses debian:bookworm-slim as base image with pandas and required dependencies
FROM debian:bookworm-slim

# * Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# * Update package lists and install Python dependencies
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# * Install required Python packages for summarize-results.py
RUN python3 -m pip install --upgrade pip --break-system-packages && \
    python3 -m pip install pandas --break-system-packages

# * Set working directory
WORKDIR /data


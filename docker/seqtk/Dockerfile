# Multi-platform seqtk container based on Debian Bookworm Lite
# Supports both amd64 and arm64 architectures
FROM debian:bookworm-slim

# Metadata
LABEL maintainer="robertbio"
LABEL description="seqtk toolkit for FASTQ/FASTA processing - multiplatform"
LABEL version="1.4"

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        git \
        zlib1g-dev \
        libbz2-dev \
        liblzma-dev \
        libcurl4-gnutls-dev \
        libssl-dev \
        wget \
        ca-certificates \
        gawk \
        bash \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt

# Download and compile seqtk
RUN git clone https://github.com/lh3/seqtk.git && \
    cd seqtk && \
    git checkout v1.4 && \
    make && \
    cp seqtk /usr/local/bin/ && \
    cd .. && \
    rm -rf seqtk

# Verify installation
RUN seqtk 2>&1 | head -1

# Create a non-root user for security
RUN useradd -m -s /bin/bash seqtk_user

# Set default working directory
WORKDIR /data

# Default command
CMD ["seqtk"]

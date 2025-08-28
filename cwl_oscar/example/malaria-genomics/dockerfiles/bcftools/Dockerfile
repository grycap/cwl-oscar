# * BCFtools Debian-based Docker Image
# Uses debian:bookworm-slim as base image for multi-platform support
FROM debian:bookworm-slim

# * Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# * Update package lists and install build dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    wget \
    libncurses5-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    autoconf \
    && rm -rf /var/lib/apt/lists/*

# * Set working directory for compilation
WORKDIR /usr/local/src

# * Download and compile HTSlib (required by bcftools)
RUN wget https://github.com/samtools/htslib/releases/download/1.16/htslib-1.16.tar.bz2 && \
    tar -xjf htslib-1.16.tar.bz2 && \
    cd htslib-1.16 && \
    autoreconf -i && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    cd .. && \
    rm -rf htslib-1.16 htslib-1.16.tar.bz2

# * Download and compile bcftools
RUN wget https://github.com/samtools/bcftools/releases/download/1.16/bcftools-1.16.tar.bz2 && \
    tar -xjf bcftools-1.16.tar.bz2 && \
    cd bcftools-1.16 && \
    autoheader && \
    autoconf -Wno-syntax && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    cd .. && \
    rm -rf bcftools-1.16 bcftools-1.16.tar.bz2

# * Update library cache  
RUN ldconfig

# * Set PATH environment variable
ENV PATH="/usr/local/bin:${PATH}"

# * Set working directory for execution
WORKDIR /data



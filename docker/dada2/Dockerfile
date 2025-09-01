# Multi-platform DADA2 container based on Debian Bookworm Lite
# Supports both amd64 and arm64 architectures
FROM debian:bookworm-slim

# Metadata
LABEL maintainer="robertbio"
LABEL description="DADA2 and ShortRead for amplicon sequence analysis - multiplatform"
LABEL version="1.30.0"

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
        r-base \
        r-base-dev \
        build-essential \
        gfortran \
        libblas-dev \
        liblapack-dev \
        libxml2-dev \
        libcurl4-openssl-dev \
        libssl-dev \
        libgit2-dev \
        libfontconfig1-dev \
        libcairo2-dev \
        libharfbuzz-dev \
        libfribidi-dev \
        libfreetype6-dev \
        libpng-dev \
        libtiff5-dev \
        libjpeg-dev \
        zlib1g-dev \
        libbz2-dev \
        liblzma-dev \
        libncurses5-dev \
        wget \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN R -e "install.packages('BiocManager', repos='https://cran.rstudio.com/')" && \
    R -e "BiocManager::install(c('dada2', 'ShortRead'), ask=FALSE, update=FALSE)" && \
    R -e "library(dada2); library(ShortRead); cat('DADA2 version:', as.character(packageVersion('dada2')), '\n'); cat('ShortRead version:', as.character(packageVersion('ShortRead')), '\n')"

# Create a non-root user for security
RUN useradd -m -s /bin/bash dada2_user

# Set default working directory
WORKDIR /data

# Default command
CMD ["R", "--version"]

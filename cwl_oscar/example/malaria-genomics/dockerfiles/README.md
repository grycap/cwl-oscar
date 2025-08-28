# Custom Debian-based Docker Images for Malaria Surveillance Workflow

This directory contains custom Docker images based on `debian:bookworm-slim` to replace the Alpine-based BioContainers used in the malaria surveillance workflow.

## Tools Included

### 1. NanoFilt (dockerfiles/nanofilt/)
- **Purpose**: Quality filtering of nanopore FASTQ reads
- **Source**: Installed via pip from PyPI
- **Image**: `robertbio/nanofilt:2.8.0-debian`
- **Replaces**: `quay.io/biocontainers/nanofilt:2.8.0--py_0`

### 2. Minimap2 (dockerfiles/minimap2/)
- **Purpose**: Sequence alignment for nanopore reads
- **Source**: Compiled from source (GitHub: lh3/minimap2)
- **Image**: `robertbio/minimap2:2.24-debian`
- **Replaces**: `quay.io/biocontainers/minimap2:2.24--h7132678_1`

### 3. Samtools (dockerfiles/samtools/)
- **Purpose**: SAM/BAM file manipulation and processing
- **Source**: Compiled from source with HTSlib dependency
- **Image**: `robertbio/samtools:1.16.1-debian`
- **Replaces**: `quay.io/biocontainers/samtools:1.16.1--h6899075_1`

### 4. BCFtools (dockerfiles/bcftools/)
- **Purpose**: Variant calling and VCF file manipulation
- **Source**: Compiled from source with HTSlib dependency
- **Image**: `robertbio/bcftools:1.16-debian`
- **Replaces**: `quay.io/biocontainers/bcftools:1.16--hfe4b78e_1`

## Building the Images

### Single Platform Build (linux/amd64)
```bash
./build_all.sh
```

### Multi-Platform Build (linux/amd64 + linux/arm64)
```bash
./build_all.sh --multiplatform
```
**Note**: Multi-platform builds require Docker Buildx and may take significantly longer.



### Manual Individual Builds

```bash
# NanoFilt
cd nanofilt && docker build -t robertbio/nanofilt:2.8.0-debian .

# Minimap2
cd minimap2 && docker build -t robertbio/minimap2:2.24-debian .

# Samtools
cd samtools && docker build -t robertbio/samtools:1.16.1-debian .

# BCFtools
cd bcftools && docker build -t robertbio/bcftools:1.16-debian .
```

## Dependencies

All images include the necessary build tools and libraries:
- Build essentials (gcc, make, etc.)
- Compression libraries (zlib, bzip2, lzma)
- SSL/TLS support for secure downloads
- Platform-specific dependencies for each tool

## Multi-Platform Support

✅ **Successfully supports both linux/amd64 and linux/arm64:**
- **NanoFilt**: Full multi-platform support
- **Samtools**: Full multi-platform support  
- **BCFtools**: Full multi-platform support

⚠️ **Limited platform support:**
- **Minimap2**: Currently linux/amd64 only due to ARM64 SIMD compilation issues. Use alternative tool?

### Architecture Notes

- **Base Image**: `debian:bookworm-slim`
- **Multi-Platform**: Uses Docker Buildx with QEMU emulation for cross-platform builds
- **ARM64 Compatibility**: Most tools compile cleanly for ARM64, with minimap2 requiring x86-specific optimizations disabled

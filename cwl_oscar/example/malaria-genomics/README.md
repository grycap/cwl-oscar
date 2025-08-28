# Malaria Genomic Surveillance Workflow

## Overview

This Common Workflow Language (CWL) workflow implements end-to-end genomic surveillance for *Plasmodium falciparum* using mobile nanopore sequencing technology. The workflow is based on the methodology described in the study "Using a mobile nanopore sequencing lab for end-to-end genomic surveillance of *Plasmodium falciparum*: A feasibility study" published in PLOS Global Public Health.

## Workflow Description

The workflow processes raw nanopore sequencing data from malaria samples to:

1. **Quality Filter Reads**: Remove low-quality and inappropriate length reads
2. **Align to References**: Map filtered reads to specific marker reference sequences
3. **Call Variants**: Identify genetic variants from aligned reads
4. **Analyze Drug Resistance**: Detect known drug resistance mutations and haplotypes
5. **Generate Reports**: Produce comprehensive surveillance reports

## Input Data

The workflow expects the following input files:

- **Raw FASTQ file**: Nanopore sequencing data from malaria samples
- **Reference FASTA**: Target marker reference sequence
- **SNP positions CSV**: Database of known drug resistance SNP positions
- **Marker ID**: Identifier for the target marker (e.g., DR_dhfr_t25, DR_mdr1_1246)

## Drug Resistance Markers

The workflow supports analysis of key malaria drug resistance markers:

### Drug Resistance (DR) Markers
- **DR_dhfr_t25/t26**: Dihydrofolate reductase - resistance to antifolate drugs
- **DR_dhps_436-437/t49**: Dihydropteroate synthase - resistance to sulfadoxine  
- **DR_mdr1_1034-1042/1246/t34/t35**: Multidrug resistance protein 1 - multiple drug resistance
- **DR_mdr2_t96**: Multidrug resistance protein 2 - chloroquine resistance
- **DR_k13_520-580**: Kelch 13 protein - artemisinin resistance

### Molecular Markers (MH)
- **MH_ama1_D2_18**: Apical membrane antigen 1 - species identification
- **MH_csp_27**: Circumsporozoite protein - parasite identification
- **MH_cpmp_22**: Conserved Plasmodium membrane protein - population genetics
- **MH_cpp_30**: CPW-WPC family protein - genetic diversity
- **MH_t04/t73**: Additional molecular markers for population studies

## Workflow Steps

### 1. Quality Filtering
- **Tool**: NanoFilt
- **Purpose**: Filter reads based on quality scores and length
- **Parameters**: 
  - Minimum quality: 8
  - Length range: 500-2000 bp

### 2. Reference Alignment  
- **Tool**: Minimap2
- **Purpose**: Align filtered reads to marker reference sequences
- **Parameters**: Oxford Nanopore preset (map-ont)

### 3. SAM/BAM Processing
- **Tool**: SAMtools
- **Purpose**: Convert, sort, and index alignment files

### 4. Variant Calling
- **Tool**: BCFtools
- **Purpose**: Call genetic variants from aligned reads
- **Output**: VCF file with detected variants

### 5. Drug Resistance Analysis
- **Tool**: Custom Python script
- **Purpose**: Analyze variants against known resistance mutations
- **Output**: Haplotype report with resistance profile

## Usage Example

```bash
cwltool malaria-surveillance.cwl \
  --raw_fastq sample_reads.fastq.gz \
  --reference_fasta DR_dhfr_t25.fasta \
  --snp_positions drug_resistance_SNP_positions.csv \
  --marker_id DR_dhfr_t25
```

## Output Files

- `filtered_fastq`: Quality-filtered sequencing reads
- `filter_stats`: Read filtering statistics
- `alignment_bam`: Sorted BAM alignment with index
- `variants_vcf`: Called variants in VCF format
- `drug_resistance_report`: Haplotype analysis and resistance profile

## Requirements

### Software Dependencies
- NanoFilt (≥2.8.0)
- Minimap2 (≥2.24)
- SAMtools (≥1.16)
- BCFtools (≥1.16)
- Python 3.9+

### Docker Containers
All tools are containerized using Biocontainers for reproducibility:
- `quay.io/biocontainers/nanofilt:2.8.0--py_0`
- `quay.io/biocontainers/minimap2:2.24--h7132678_1`
- `quay.io/biocontainers/samtools:1.16.1--h6899075_1`
- `quay.io/biocontainers/bcftools:1.16--hfe4b78e_1`
- `python:3.9-slim`

The biocontainers currently use the alpine base. We packed the same tools in debian containers at `robertbio/tool_name:version`, referenced in the .cwl files.


## Citation

For the original workflow, please cite the original study:

> "Using a mobile nanopore sequencing lab for end-to-end genomic surveillance of *Plasmodium falciparum*: A feasibility study." *PLOS Global Public Health* (2023)

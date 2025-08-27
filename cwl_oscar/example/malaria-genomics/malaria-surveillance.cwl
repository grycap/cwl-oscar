#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow

label: "Malaria Genomic Surveillance Workflow"
doc: |
  End-to-end genomic surveillance workflow for Plasmodium falciparum 
  using mobile nanopore sequencing, focusing on drug resistance markers
  and molecular markers for malaria surveillance.
  
  Based on the study: "Using a mobile nanopore sequencing lab for 
  end-to-end genomic surveillance of Plasmodium falciparum: 
  A feasibility study"

requirements:
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
  - class: MultipleInputFeatureRequirement

inputs:
  # Input sequencing data
  raw_fastq:
    type: File
    doc: "Raw nanopore FASTQ sequencing data"
  
  # Reference sequences for specific marker
  reference_fasta:
    type: File
    doc: "Reference FASTA sequence for the target marker"
  
  # Drug resistance SNP positions
  snp_positions:
    type: File
    doc: "CSV file with drug resistance SNP positions"
  
  # Marker identifier
  marker_id:
    type: string
    doc: "Marker ID (e.g., DR_dhfr_t25, DR_mdr1_1246, MH_csp_27)"
  
  # Quality filtering parameters
  min_quality:
    type: int
    default: 8
    doc: "Minimum average read quality score"
  
  min_read_length:
    type: int
    default: 500
    doc: "Minimum read length for filtering"
  
  max_read_length:
    type: int
    default: 2000
    doc: "Maximum read length for filtering"
  
  # Alignment parameters
  threads:
    type: int
    default: 4
    doc: "Number of threads for processing"

steps:
  # Step 1: Quality filtering of nanopore reads
  quality_filter:
    run: tools/quality-filter-gzip.cwl
    in:
      fastq_file: raw_fastq
      min_quality: min_quality
      min_length: min_read_length
      max_length: max_read_length
    out: [filtered_reads]
  
  # Step 2: Alignment to reference sequence
  minimap_align:
    run: tools/minimap-align.cwl
    in:
      reference_fasta: reference_fasta
      fastq_reads: quality_filter/filtered_reads
      threads: threads
    out: [alignment_sam]
  
  # Step 3: Convert SAM to sorted BAM
  sam_to_bam:
    run: tools/sam-to-bam.cwl
    in:
      sam_file: minimap_align/alignment_sam
      threads: threads
    out: [sorted_bam]
  
  # Step 4: Variant calling
  variant_calling:
    run: tools/variant-calling.cwl
    in:
      reference_fasta: reference_fasta
      bam_file: sam_to_bam/sorted_bam
    out: [vcf_file]
  
  # Step 5: Drug resistance haplotype analysis
  haplotype_analysis:
    run: tools/haplotype-caller.cwl
    in:
      vcf_file: variant_calling/vcf_file
      snp_positions: snp_positions
      marker_id: marker_id
    out: [haplotype_report]

outputs:
  # Quality control outputs
  filtered_fastq:
    type: File
    outputSource: quality_filter/filtered_reads
    doc: "Quality filtered FASTQ reads"
  
  # filter_stats: # Removed - simple quality filter doesn't generate summary
  #   type: File?
  #   outputSource: quality_filter/filter_summary
  #   doc: "Read filtering statistics (optional)"
  
  # Alignment outputs
  alignment_bam:
    type: File
    outputSource: sam_to_bam/sorted_bam
    doc: "Sorted BAM alignment file with index"
  
  # Variant analysis outputs
  variants_vcf:
    type: File
    outputSource: variant_calling/vcf_file
    doc: "Called variants in VCF format"
  
  # Drug resistance analysis
  drug_resistance_report:
    type: File
    outputSource: haplotype_analysis/haplotype_report
    doc: "Drug resistance haplotype analysis report"

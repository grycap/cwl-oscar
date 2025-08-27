#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow

label: "Malaria Genomic Surveillance - Batch Processing"
doc: |
  Parallel processing of multiple malaria samples using CWL scatter.
  Processes an array of FASTQ files with corresponding reference sequences.

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement

inputs:
  # Array of input files
  fastq_files:
    type: File[]
    doc: "Array of FASTQ files to process"
  
  reference_fastas:
    type: File[]
    doc: "Array of reference FASTA files (same order as fastq_files)"
  
  marker_ids:
    type: string[]
    doc: "Array of marker IDs (same order as fastq_files)"
  
  snp_positions:
    type: File
    doc: "Drug resistance SNP positions database"
  
  # Processing parameters
  min_quality:
    type: int
    default: 8
    doc: "Minimum read quality score"
  
  min_read_length:
    type: int
    default: 100
    doc: "Minimum read length"
  
  max_read_length:
    type: int
    default: 3000
    doc: "Maximum read length"
  
  threads:
    type: int
    default: 2
    doc: "Number of threads for processing"

steps:
  # Scatter the surveillance workflow across all samples
  process_samples:
    run: malaria-surveillance.cwl
    scatter: [raw_fastq, reference_fasta, marker_id]
    scatterMethod: dotproduct
    in:
      raw_fastq: fastq_files
      reference_fasta: reference_fastas
      marker_id: marker_ids
      snp_positions: snp_positions
      min_quality: min_quality
      min_read_length: min_read_length
      max_read_length: max_read_length
      threads: threads
    out: [filtered_fastq, alignment_bam, variants_vcf, drug_resistance_report]

outputs:
  # Arrays of output files from all samples
  all_filtered_fastq:
    type: File[]
    outputSource: process_samples/filtered_fastq
    doc: "Quality filtered FASTQ files for all samples"
  
  all_alignment_bam:
    type: File[]
    outputSource: process_samples/alignment_bam
    doc: "Aligned BAM files for all samples"
  
  all_variants_vcf:
    type: File[]
    outputSource: process_samples/variants_vcf
    doc: "VCF files with variants for all samples"
  
  all_drug_resistance_reports:
    type: File[]
    outputSource: process_samples/drug_resistance_report
    doc: "Drug resistance analysis reports for all samples"

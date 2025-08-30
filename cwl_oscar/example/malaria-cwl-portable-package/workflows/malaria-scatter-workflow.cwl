#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow

label: "Malaria Drug Resistance Analysis - Multi-Sample Scatter"
doc: "Scatter workflow for processing multiple malaria samples in parallel"

requirements:
  ScatterFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}

inputs:
  samples:
    type:
      type: array
      items:
        type: record
        fields:
          fastq_file:
            type: File
            doc: "Raw FASTQ file for this sample"
          marker_id:
            type: string
            doc: "Marker identifier for this sample"
    doc: "Array of sample records with FASTQ files and marker IDs"

  snp_positions:
    type: File
    doc: "Drug resistance SNP positions database"

  # Global parameters (matching original zenodo_10086238 study)
  min_length:
    type: int?
    default: 10
    doc: "Minimum read length for filtering (original: 10bp)"

  max_length:
    type: int?
    default: 2100
    doc: "Maximum read length for filtering (original: 2100bp)"

  min_quality:
    type: int?
    default: 8
    doc: "Minimum quality score"

  min_coverage:
    type: int?
    default: 65
    doc: "Minimum coverage per haplotype"

outputs:
  all_mutations_reports:
    type: File[]
    outputSource: process_samples/mutations_report
    doc: "Mutations reports for all samples"

  all_resistance_summaries:
    type: File[]
    outputSource: process_samples/resistance_summary
    doc: "Resistance summaries for all samples"

  all_detailed_analyses:
    type: File[]
    outputSource: process_samples/detailed_analysis
    doc: "Detailed analyses for all samples"

  all_haplotypes:
    type: File[]
    outputSource: process_samples/haplotypes
    doc: "Haplotype sequences for all samples"

  all_filtering_stats:
    type: File[]
    outputSource: process_samples/filtering_stats
    doc: "Filtering statistics for all samples"

steps:
  process_samples:
    run: malaria-amplicon-workflow.cwl
    scatter: [raw_fastq, marker_id]
    scatterMethod: dotproduct
    in:
      raw_fastq:
        source: samples
        valueFrom: $(self.fastq_file)
      marker_id:
        source: samples
        valueFrom: $(self.marker_id)
      snp_positions: snp_positions
      min_length: min_length
      max_length: max_length
      min_quality: min_quality
      min_coverage: min_coverage
    out: [
      filtered_reads, filtering_stats,
      haplotypes, haplotype_counts,
      mutations_report, resistance_summary, detailed_analysis
    ]

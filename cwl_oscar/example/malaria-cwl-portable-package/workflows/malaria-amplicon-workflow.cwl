#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow

label: "Malaria Drug Resistance Amplicon Analysis Workflow"
doc: "Complete CPU-only workflow for detecting drug resistance mutations in malaria amplicon data"

requirements:
  MultipleInputFeatureRequirement: {}
  StepInputExpressionRequirement: {}

inputs:
  raw_fastq:
    type: File
    doc: "Raw nanopore FASTQ file"

  marker_id:
    type: string
    doc: "Marker identifier (e.g., DR_dhfr_t25)"

  snp_positions:
    type: File
    doc: "Drug resistance SNP positions database"

  # Quality filtering parameters (matching original study)
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

  # DADA2 parameters
  min_coverage:
    type: int?
    default: 65
    doc: "Minimum coverage per haplotype"

outputs:
  # Step 1 outputs
  filtered_reads:
    type: File
    outputSource: quality_filter/filtered_reads
    doc: "Quality filtered reads"

  filtering_stats:
    type: File
    outputSource: quality_filter/filtering_stats
    doc: "Quality filtering statistics"

  # Step 2 outputs
  haplotypes:
    type: File
    outputSource: dada2_reconstruction/haplotypes
    doc: "Reconstructed haplotypes"

  haplotype_counts:
    type: File
    outputSource: dada2_reconstruction/haplotype_counts
    doc: "Haplotype coverage and frequency data"

  # Step 3 outputs
  mutations_report:
    type: File
    outputSource: mutation_analysis/mutations_report
    doc: "Detailed mutations report"

  resistance_summary:
    type: File
    outputSource: mutation_analysis/resistance_summary
    doc: "Summary of detected resistance mutations"

  detailed_analysis:
    type: File
    outputSource: mutation_analysis/detailed_analysis
    doc: "Detailed mutation analysis data"

steps:
  quality_filter:
    run: ../tools/amplicon-quality-filter.cwl
    in:
      fastq_file: raw_fastq
      marker_id: marker_id
      min_length: min_length
      max_length: max_length
      min_quality: min_quality
    out: [filtered_reads, filtering_stats]

  dada2_reconstruction:
    run: ../tools/dada2-haplotypes.cwl
    in:
      filtered_reads: quality_filter/filtered_reads
      marker_id: marker_id
      output_prefix:
        source: marker_id
        valueFrom: $(self)_dada2
      min_coverage: min_coverage
    out: [haplotypes, haplotype_counts]

  mutation_analysis:
    run: ../tools/mutation-analysis.cwl
    in:
      haplotypes: dada2_reconstruction/haplotypes
      haplotype_counts: dada2_reconstruction/haplotype_counts
      snp_positions: snp_positions
      marker_id: marker_id
      output_prefix:
        source: marker_id
        valueFrom: $(self)_mutations
    out: [mutations_report, resistance_summary, detailed_analysis]

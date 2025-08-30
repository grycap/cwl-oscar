#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow

doc: |
  Complete malaria genomics workflow with integrated final report generation
  Combines scatter processing with final reporting

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement

inputs:
  samples:
    type:
      type: array
      items:
        type: record
        fields:
          fastq_file:
            type: File
            doc: "Input FASTQ file"
          marker_id:
            type: string
            doc: "Marker identifier (e.g., DR_dhfr_t25)"
    doc: "Array of samples to process"

  snp_positions:
    type: File
    doc: "SNP positions file for mutation analysis"

  input_yml_file:
    type: File
    doc: "Original input YAML file for sample mapping"

  min_length:
    type: int?
    default: 10
    doc: "Minimum read length for filtering"

  max_length:
    type: int?
    default: 2100
    doc: "Maximum read length for filtering"

  min_quality:
    type: int?
    default: 8
    doc: "Minimum quality threshold"

  min_coverage:
    type: int?
    default: 65
    doc: "Minimum coverage threshold"

outputs:
  # Individual results from scatter workflow
  all_mutations_reports:
    type: File[]
    outputSource: scatter_analysis/all_mutations_reports
  
  all_resistance_summaries:
    type: File[]
    outputSource: scatter_analysis/all_resistance_summaries
    
  all_detailed_analyses:
    type: File[]
    outputSource: scatter_analysis/all_detailed_analyses
    
  all_haplotypes:
    type: File[]
    outputSource: scatter_analysis/all_haplotypes
    
  all_filtering_stats:
    type: File[]
    outputSource: scatter_analysis/all_filtering_stats
  
  # Final consolidated reports
  final_haplotype_list:
    type: File
    outputSource: generate_reports/final_haplotype_list
    doc: "Complete haplotype list matching original study format"
  
  final_snp_list:
    type: File
    outputSource: generate_reports/final_snp_list
    doc: "SNP matrix matching original study format"
  
  marker_fastas:
    type: File[]
    outputSource: generate_reports/marker_fastas
    doc: "Individual FASTA files per marker"
  
  analysis_summary:
    type: File
    outputSource: generate_reports/analysis_summary
    doc: "Analysis summary"

steps:
  # Step 1: Run the scatter workflow to process all samples
  scatter_analysis:
    run: malaria-scatter-workflow.cwl
    in:
      samples: samples
      snp_positions: snp_positions
      min_length: min_length
      max_length: max_length
      min_quality: min_quality
      min_coverage: min_coverage
    out: [all_mutations_reports, all_resistance_summaries, all_detailed_analyses, all_haplotypes, all_filtering_stats]

  # Step 2: Generate final reports (array-based CLI)
  generate_reports:
    run: ../tools/final-reports.cwl
    in:
      detailed_analyses: scatter_analysis/all_detailed_analyses
      resistance_summaries: scatter_analysis/all_resistance_summaries
      haplotypes: scatter_analysis/all_haplotypes
      input_yml_file: input_yml_file
    out: [final_haplotype_list, final_snp_list, marker_fastas, analysis_summary]

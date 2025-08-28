#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Summarize Malaria Surveillance Results"
doc: |
  Analyze and summarize results from batch malaria genomic surveillance analysis.
  Generates comprehensive reports and statistics across all processed samples.

requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entry: $(inputs.summarize_script)
        entryname: summarize-results.py
      - entry: $(inputs.results_directory)
        entryname: results
        writable: true
  - class: DockerRequirement
    dockerPull: "robertbio/summary-analysis:1.0-debian"

baseCommand: ["python3"]

inputs:
  summarize_script:
    type: File
    doc: "The summarize-results.py script"
  
  results_directory:
    type: Directory
    doc: "Directory containing organized drug resistance reports"
  
  output_prefix:
    type: string
    default: "batch_analysis"
    doc: "Prefix for output filenames"

arguments:
  - "summarize-results.py"
  - "results"

outputs:
  summary_directory:
    type: Directory
    outputBinding:
      glob: "results/summary_analysis"
    doc: "Directory containing all summary reports and analysis"
  
  overall_summary:
    type: File
    outputBinding:
      glob: "results/summary_analysis/overall_summary.json"
    doc: "Overall summary statistics in JSON format"
  
  detailed_results:
    type: File
    outputBinding:
      glob: "results/summary_analysis/detailed_results.csv"
    doc: "Detailed results for all samples in CSV format"
  
  drug_resistance_summary:
    type: File
    outputBinding:
      glob: "results/summary_analysis/drug_resistance_summary.json"
    doc: "Drug resistance analysis summary"
  
  summary_report:
    type: File
    outputBinding:
      glob: "results/summary_analysis/SUMMARY_REPORT.txt"
    doc: "Human-readable summary report"

stdout: summary_log.txt

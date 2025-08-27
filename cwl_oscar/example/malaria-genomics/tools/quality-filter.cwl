#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Quality Filter for Nanopore Reads"
doc: |
  Filter nanopore FASTQ reads based on quality score and read length.
  This tool uses NanoFilt to filter reads for malaria genomic surveillance.

requirements:
  - class: InlineJavascriptRequirement
  - class: DockerRequirement
    dockerPull: "quay.io/biocontainers/nanofilt:2.8.0--py_0"

baseCommand: ["NanoFilt"]

inputs:
  fastq_file:
    type: File
    inputBinding:
      position: 1
    doc: "Input FASTQ file with nanopore reads"
  
  min_quality:
    type: int
    default: 8
    inputBinding:
      prefix: "--quality"
      position: 2
    doc: "Minimum average read quality score"
  
  min_length:
    type: int
    default: 500
    inputBinding:
      prefix: "--length"
      position: 3
    doc: "Minimum read length"
  
  max_length:
    type: int
    default: 2000
    inputBinding:
      prefix: "--maxlength"
      position: 4
    doc: "Maximum read length"

outputs:
  filtered_reads:
    type: stdout
    doc: "Quality filtered FASTQ reads"
  
  filter_summary:
    type: File?
    outputBinding:
      glob: "*.log"
    doc: "Optional summary statistics of the filtering process"

stdout: $(inputs.fastq_file.nameroot)_filtered.fastq


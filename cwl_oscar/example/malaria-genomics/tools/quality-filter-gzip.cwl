#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Quality Filter for Gzipped Nanopore Reads"
doc: |
  Filter nanopore FASTQ reads based on quality score and read length.
  Handles both plain and gzipped FASTQ files using gunzip and NanoFilt.

requirements:
  - class: InlineJavascriptRequirement
  - class: DockerRequirement
    dockerPull: "quay.io/biocontainers/nanofilt:2.8.0--py_0"

baseCommand: ["bash", "-c"]

inputs:
  fastq_file:
    type: File
    doc: "Input FASTQ file (can be gzipped or plain)"
  
  min_quality:
    type: int
    default: 8
    doc: "Minimum average read quality score"
  
  min_length:
    type: int
    default: 100
    doc: "Minimum read length"
  
  max_length:
    type: int
    default: 3000
    doc: "Maximum read length"

arguments:
  - |
    # Check if file is gzipped and decompress if needed, then filter
    INPUT_FILE="$(inputs.fastq_file.path)"
    if [[ "$INPUT_FILE" == *.gz ]]; then
      echo "Decompressing gzipped FASTQ file..."
      gunzip -c "$INPUT_FILE" | NanoFilt --quality $(inputs.min_quality) --length $(inputs.min_length) --maxlength $(inputs.max_length)
    else
      echo "Processing plain FASTQ file..."
      NanoFilt "$INPUT_FILE" --quality $(inputs.min_quality) --length $(inputs.min_length) --maxlength $(inputs.max_length)
    fi

outputs:
  filtered_reads:
    type: stdout
    doc: "Quality filtered FASTQ reads"

stdout: |
  ${
    var base = inputs.fastq_file.nameroot;
    if (base.endsWith('.fastq')) {
      return base.slice(0, -6) + '_filtered.fastq';
    } else {
      return base + '_filtered.fastq';
    }
  }


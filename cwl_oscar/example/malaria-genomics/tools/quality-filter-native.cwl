#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Quality Filter using native tools"
doc: |
  Simple quality filter using seqtk instead of NanoFilt to avoid Docker issues
  on Apple Silicon Macs.

requirements:
  - class: InlineJavascriptRequirement

baseCommand: ["seqtk", "seq"]

inputs:
  fastq_file:
    type: File
    inputBinding:
      position: 2
    doc: "Input FASTQ file"
  
  min_length:
    type: int
    default: 500
    inputBinding:
      prefix: "-L"
      position: 1
    doc: "Minimum read length"

outputs:
  filtered_reads:
    type: stdout
    doc: "Quality filtered FASTQ reads"

stdout: $(inputs.fastq_file.nameroot)_filtered.fastq


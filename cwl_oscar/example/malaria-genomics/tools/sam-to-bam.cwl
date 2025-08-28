#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "SAM to BAM Conversion"
doc: |
  Convert SAM alignment files to sorted and indexed BAM format
  for downstream variant calling analysis.

requirements:
  - class: InlineJavascriptRequirement
  - class: ResourceRequirement
    coresMin: 2
  - class: DockerRequirement
    dockerPull: "robertbio/samtools:1.16.1-debian"

baseCommand: ["bash", "-c"]

inputs:
  sam_file:
    type: File
    doc: "Input SAM alignment file"
  
  threads:
    type: int
    default: 2
    doc: "Number of threads for sorting"

arguments:
  - |
    samtools view -bS $(inputs.sam_file.path) | \
    samtools sort -@ $(inputs.threads) -o $(inputs.sam_file.nameroot).bam - && \
    samtools index $(inputs.sam_file.nameroot).bam

outputs:
  sorted_bam:
    type: File
    outputBinding:
      glob: "*.bam"
    secondaryFiles:
      - ".bai"
    doc: "Sorted BAM file with index"


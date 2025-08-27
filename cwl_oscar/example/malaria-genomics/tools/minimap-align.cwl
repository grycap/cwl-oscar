#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Minimap2 Alignment for Malaria Markers"
doc: |
  Align filtered nanopore reads to reference sequences for malaria drug resistance
  and molecular markers using minimap2.

requirements:
  - class: InlineJavascriptRequirement
  - class: ResourceRequirement
    coresMin: 4
  - class: DockerRequirement
    dockerPull: "quay.io/biocontainers/minimap2:2.24--h7132678_1"

baseCommand: ["minimap2"]

inputs:
  reference_fasta:
    type: File
    inputBinding:
      position: 1
    doc: "Reference FASTA file for specific marker"
  
  fastq_reads:
    type: File
    inputBinding:
      position: 2
    doc: "Filtered FASTQ reads to align"
  
  preset:
    type: string
    default: "map-ont"
    inputBinding:
      prefix: "-x"
      position: 0
    doc: "Minimap2 preset for Oxford Nanopore reads"
  
  threads:
    type: int
    default: 4
    inputBinding:
      prefix: "-t"
      position: 0
    doc: "Number of threads to use"

arguments:
  - "-a"  # ! Output in SAM format
  - "--secondary=no"  # ! Don't output secondary alignments
  - "--MD"  # ! Generate MD tag for SNP/variant calling

outputs:
  alignment_sam:
    type: stdout
    doc: "SAM alignment file"

stdout: $(inputs.fastq_reads.nameroot)_$(inputs.reference_fasta.nameroot)_aligned.sam

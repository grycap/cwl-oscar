#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Variant Calling for Malaria Drug Resistance"
doc: |
  Call variants from aligned reads using bcftools mpileup and call.
  Focuses on drug resistance SNPs for malaria genomic surveillance.

requirements:
  - class: InlineJavascriptRequirement
  - class: ResourceRequirement
    coresMin: 2
  - class: DockerRequirement
    dockerPull: "robertbio/bcftools:1.16-debian"

baseCommand: ["bash", "-c"]

inputs:
  reference_fasta:
    type: File
    doc: "Reference FASTA file"
  
  bam_file:
    type: File
    secondaryFiles:
      - ".bai"
    doc: "Sorted BAM file with index"
  
  min_base_quality:
    type: int
    default: 20
    doc: "Minimum base quality for variant calling"
  
  min_mapping_quality:
    type: int
    default: 20
    doc: "Minimum mapping quality"

arguments:
  - |
    # Copy reference FASTA to writable directory and create index
    cp $(inputs.reference_fasta.path) ./reference.fasta
    samtools faidx ./reference.fasta
    
    # Run variant calling with local reference
    bcftools mpileup \
      -f ./reference.fasta \
      -q $(inputs.min_mapping_quality) \
      -Q $(inputs.min_base_quality) \
      --max-depth 1000 \
      --annotate FORMAT/AD,FORMAT/DP \
      $(inputs.bam_file.path) | \
    bcftools call \
      --multiallelic-caller \
      --variants-only \
      --output-type v \
      --output $(inputs.bam_file.nameroot)_variants.vcf

outputs:
  vcf_file:
    type: File
    outputBinding:
      glob: "*_variants.vcf"
    doc: "VCF file with called variants"


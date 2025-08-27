#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Drug Resistance Haplotype Analysis"
doc: |
  Analyze drug resistance mutations and haplotypes based on called variants
  and known SNP positions for malaria genomic surveillance.

requirements:
  - class: InlineJavascriptRequirement
  - class: ResourceRequirement
    ramMin: 1024
  - class: DockerRequirement
    dockerPull: "python:3.9-slim"

baseCommand: ["python3", "-c"]

inputs:
  vcf_file:
    type: File
    doc: "VCF file with called variants"
  
  snp_positions:
    type: File
    doc: "CSV file with drug resistance SNP positions"
  
  marker_id:
    type: string
    doc: "Marker ID to analyze (e.g., DR_dhfr_t25, DR_mdr1_1246)"

arguments:
  - |
    import csv
    import sys
    import os
    
    # Read SNP positions for the specific marker
    snp_positions = {}
    marker_id = "$(inputs.marker_id)"
    
    with open("$(inputs.snp_positions.path)", 'r') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            if row['Marker'] == marker_id and row['Use'] == 'T':
                pos_key = f"{row['Chromosome']}:{row['Position']}"
                snp_positions[pos_key] = {
                    'gene': row['Gene'],
                    'mutation': row['Mutation'],
                    'ref': row['REF'],
                    'alt': row['ALT'],
                    'comment': row.get('Comment', '')
                }
    
    # Parse VCF file
    detected_variants = []
    with open("$(inputs.vcf_file.path)", 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            
            fields = line.strip().split('\t')
            if len(fields) < 8:
                continue
                
            chrom = fields[0]
            pos = fields[1]
            ref = fields[3]
            alt = fields[4]
            qual = fields[5]
            info = fields[7]
            
            pos_key = f"{chrom}:{pos}"
            if pos_key in snp_positions:
                expected = snp_positions[pos_key]
                detected_variants.append({
                    'position': pos_key,
                    'ref': ref,
                    'alt': alt,
                    'expected_ref': expected['ref'],
                    'expected_alt': expected['alt'],
                    'mutation': expected['mutation'],
                    'quality': qual,
                    'match': (ref == expected['ref'] and alt == expected['alt'])
                })
    
    # Generate report
    output_file = f"{marker_id}_haplotype_report.txt"
    with open(output_file, 'w') as f:
        f.write(f"Drug Resistance Haplotype Analysis Report\n")
        f.write(f"Marker ID: {marker_id}\n")
        f.write(f"VCF File: $(inputs.vcf_file.basename)\n")
        f.write(f"Date: $(new Date().toISOString().split('T')[0])\n\n")
        
        f.write("Detected Drug Resistance Variants:\n")
        f.write("-" * 50 + "\n")
        
        if detected_variants:
            for var in detected_variants:
                status = "MATCH" if var['match'] else "MISMATCH"
                f.write(f"Position: {var['position']}\n")
                f.write(f"Mutation: {var['mutation']}\n")
                f.write(f"Detected: {var['ref']} -> {var['alt']}\n")
                f.write(f"Expected: {var['expected_ref']} -> {var['expected_alt']}\n")
                f.write(f"Status: {status}\n")
                f.write(f"Quality: {var['quality']}\n\n")
        else:
            f.write("No drug resistance variants detected for this marker.\n")
        
        f.write(f"\nSummary:\n")
        f.write(f"Total variants detected: {len(detected_variants)}\n")
        f.write(f"Matching expected variants: {sum(1 for v in detected_variants if v['match'])}\n")
        f.write(f"Non-matching variants: {sum(1 for v in detected_variants if not v['match'])}\n")

outputs:
  haplotype_report:
    type: File
    outputBinding:
      glob: "*_haplotype_report.txt"
    doc: "Drug resistance haplotype analysis report"


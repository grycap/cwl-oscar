#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Drug Resistance Mutation Analysis"
doc: "Analyze haplotypes for known drug resistance mutations"

requirements:
  DockerRequirement:
    dockerPull: "python:3.13-slim"
  InitialWorkDirRequirement:
    listing:
      - entryname: analyze_mutations.py
        entry: |
          #!/usr/bin/env python3
          import sys
          import csv
          import argparse
          
          def parse_fasta(fasta_file):
              """Simple FASTA parser that returns {id: sequence} dict"""
              sequences = {}
              current_id = None
              current_seq = []
              
              with open(fasta_file, 'r') as f:
                  for line in f:
                      line = line.strip()
                      if line.startswith('>'):
                          if current_id:
                              sequences[current_id] = ''.join(current_seq)
                          current_id = line[1:]  # Remove '>'
                          current_seq = []
                      else:
                          current_seq.append(line)
                  
                  # Don't forget the last sequence
                  if current_id:
                      sequences[current_id] = ''.join(current_seq)
              
              return sequences
          
          def read_snp_positions(snp_file, marker_id):
              """Read SNP positions for the specific marker"""
              snp_positions = []
              with open(snp_file, 'r') as f:
                  reader = csv.DictReader(f, delimiter=';')
                  for row in reader:
                      if row.get('Marker') == marker_id and row.get('Use') == 'T':
                          snp_positions.append({
                              'gene': row.get('Gene', ''),
                              'mutation': row.get('Mutation', ''),
                              'position': int(row.get('nt_position_coding_strand', 0)) if row.get('nt_position_coding_strand') else 0,
                              'ref': row.get('REF', ''),
                              'alt': row.get('ALT', ''),
                              'comment': row.get('Comment', '')
                          })
              return snp_positions
          
          def read_haplotype_counts(counts_file):
              """Read haplotype counts and frequencies"""
              haplotype_data = {}
              with open(counts_file, 'r') as f:
                  reader = csv.DictReader(f)
                  for row in reader:
                      if row.get('Status') == 'Valid':
                          haplotype_data[row.get('HaplotypeID')] = {
                              'sequence': row.get('Sequence', ''),
                              'coverage': int(row.get('Coverage', 0)),
                              'frequency': float(row.get('Frequency', 0)),
                              'status': row.get('Status', '')
                          }
              return haplotype_data
          
          def analyze_haplotype_mutations(haplotype_seq, snp_positions):
              """Analyze a haplotype for drug resistance mutations"""
              mutations_found = []
              
              for snp in snp_positions:
                  pos = snp['position']
                  if pos > 0 and pos <= len(haplotype_seq):
                      # Convert to 0-based indexing
                      hap_base = haplotype_seq[pos-1].upper()
                      ref_base = snp['ref'].upper()
                      alt_base = snp['alt'].upper()
                      
                      # Check for mutation
                      if hap_base == alt_base and hap_base != ref_base:
                          mutations_found.append({
                              'position': pos,
                              'gene': snp['gene'],
                              'mutation': snp['mutation'],
                              'reference': ref_base,
                              'observed': hap_base,
                              'expected_alt': alt_base,
                              'match_type': 'Known_resistance'
                          })
                      elif hap_base != ref_base and hap_base != alt_base:
                          mutations_found.append({
                              'position': pos,
                              'gene': snp['gene'], 
                              'mutation': f'Novel_{pos}',
                              'reference': ref_base,
                              'observed': hap_base,
                              'expected_alt': alt_base,
                              'match_type': 'Novel_variant'
                          })
              
              return mutations_found
          
          def main():
              parser = argparse.ArgumentParser(description='Analyze mutations in malaria haplotypes')
              parser.add_argument('haplotypes_fasta', help='Haplotype sequences FASTA')
              parser.add_argument('counts_csv', help='Haplotype counts CSV')
              parser.add_argument('snp_positions_csv', help='SNP positions database')
              parser.add_argument('marker_id', help='Marker ID')
              parser.add_argument('output_prefix', help='Output prefix')
              
              args = parser.parse_args()
              
              print(f"=== Mutation Analysis: {args.marker_id} ===")
              
              # Read SNP positions for this marker
              snp_positions = read_snp_positions(args.snp_positions_csv, args.marker_id)
              print(f"Found {len(snp_positions)} SNP positions for {args.marker_id}")
              
              if not snp_positions:
                  print("No SNP positions found for this marker")
                  # Create empty output files for markers without SNP positions (e.g., MH markers)
                  
                  # Read haplotype counts to get basic info
                  haplotype_data = read_haplotype_counts(args.counts_csv)
                  
                  # Create empty mutations file
                  mutations_file = f"{args.output_prefix}_mutations.txt"
                  with open(mutations_file, 'w') as f:
                      f.write(f"=== Mutation Analysis Results for {args.marker_id} ===\\n")
                      f.write("No drug resistance SNP positions defined for this marker\\n")
                      f.write("This is normal for microhaplotype (MH) markers\\n")
                  
                  # Create empty resistance summary
                  summary_file = f"{args.output_prefix}_resistance_summary.csv"
                  with open(summary_file, 'w', newline='') as f:
                      writer = csv.writer(f)
                      writer.writerow(['Marker', 'Gene', 'Mutation', 'Frequency', 'Coverage', 'Status'])
                      # No data rows for markers without SNP positions
                  
                  # Create detailed analysis with haplotype info but no mutations
                  detailed_file = f"{args.output_prefix}_detailed_analysis.csv"
                  with open(detailed_file, 'w', newline='') as f:
                      writer = csv.writer(f)
                      writer.writerow(['HaplotypeID', 'Coverage', 'Frequency', 'Gene', 'Mutation', 'Position', 'Reference', 'Observed', 'Type'])
                      
                      # Add haplotype entries with no mutations (for MH markers)
                      for hap_id, hap_data in haplotype_data.items():
                          writer.writerow([
                              hap_id,
                              hap_data['coverage'],
                              f"{hap_data['frequency']:.4f}",
                              'None',
                              'No_mutations',
                              '',
                              '',
                              '',
                              'No_SNP_positions'
                          ])
                  
                  print(f"Created empty output files for marker without SNP positions: {args.marker_id}")
                  return
              
              # Read haplotype counts
              haplotype_data = read_haplotype_counts(args.counts_csv)
              print(f"Found {len(haplotype_data)} valid haplotypes")
              
              # Read haplotype sequences
              try:
                  fasta_sequences = parse_fasta(args.haplotypes_fasta)
                  haplotype_sequences = {}
                  for header, sequence in fasta_sequences.items():
                      # Extract haplotype ID from header (remove coverage info)
                      hap_id = header.split('_cov')[0]
                      haplotype_sequences[hap_id] = sequence
              except Exception as e:
                  print(f"Error reading haplotype sequences: {e}")
                  return
              
              # Analyze each haplotype
              all_results = []
              mutations_summary = {}
              
              for hap_id, sequence in haplotype_sequences.items():
                  if hap_id in haplotype_data:
                      hap_data = haplotype_data[hap_id]
                      mutations = analyze_haplotype_mutations(sequence, snp_positions)
                      
                      result = {
                          'haplotype_id': hap_id,
                          'sequence': sequence,
                          'coverage': hap_data['coverage'],
                          'frequency': hap_data['frequency'],
                          'mutations': mutations,
                          'mutation_count': len([m for m in mutations if m['match_type'] == 'Known_resistance']),
                          'novel_count': len([m for m in mutations if m['match_type'] == 'Novel_variant'])
                      }
                      all_results.append(result)
                      
                      # Summary for each mutation type
                      for mut in mutations:
                          if mut['match_type'] == 'Known_resistance':
                              key = f"{mut['gene']}_{mut['mutation']}"
                              if key not in mutations_summary:
                                  mutations_summary[key] = {'frequency': 0, 'coverage': 0}
                              mutations_summary[key]['frequency'] += hap_data['frequency']
                              mutations_summary[key]['coverage'] += hap_data['coverage']
              
              # Write outputs
              mutations_file = f"{args.output_prefix}_mutations.txt"
              summary_file = f"{args.output_prefix}_resistance_summary.csv"
              detailed_file = f"{args.output_prefix}_detailed_analysis.csv"
              
              # Write mutations report
              with open(mutations_file, 'w') as f:
                  f.write(f"=== Drug Resistance Mutation Analysis ===\\n")
                  f.write(f"Marker: {args.marker_id}\\n\\n")
                  
                  total_resistance = sum(r['mutation_count'] for r in all_results)
                  total_novel = sum(r['novel_count'] for r in all_results)
                  
                  for result in all_results:
                      f.write(f"Haplotype: {result['haplotype_id']}\\n")
                      f.write(f"Coverage: {result['coverage']} reads ({result['frequency']:.1%})\\n")
                      f.write(f"Sequence length: {len(result['sequence'])}bp\\n")
                      
                      if result['mutations']:
                          f.write("Mutations found:\\n")
                          for mut in result['mutations']:
                              f.write(f"  {mut['gene']} {mut['mutation']}: {mut['reference']}->{mut['observed']} (pos {mut['position']}) [{mut['match_type']}]\\n")
                      else:
                          f.write("No resistance mutations detected\\n")
                      f.write("\\n")
                  
                  f.write(f"SUMMARY:\\n")
                  f.write(f"Total haplotypes analyzed: {len(all_results)}\\n")
                  f.write(f"Drug resistance mutations: {total_resistance}\\n")
                  f.write(f"Novel variants: {total_novel}\\n")
              
              # Write CSV summary
              with open(summary_file, 'w', newline='') as f:
                  writer = csv.writer(f)
                  writer.writerow(['Marker', 'Gene', 'Mutation', 'Frequency', 'Coverage', 'Status'])
                  
                  for mut_type, data in mutations_summary.items():
                      gene, mutation = mut_type.split('_', 1)
                      writer.writerow([args.marker_id, gene, mutation, f"{data['frequency']:.4f}", data['coverage'], 'Detected'])
              
              # Write detailed analysis
              with open(detailed_file, 'w', newline='') as f:
                  writer = csv.writer(f)
                  writer.writerow(['HaplotypeID', 'Coverage', 'Frequency', 'Gene', 'Mutation', 'Position', 'Reference', 'Observed', 'Type'])
                  
                  for result in all_results:
                      if result['mutations']:
                          for mut in result['mutations']:
                              writer.writerow([
                                  result['haplotype_id'],
                                  result['coverage'],
                                  f"{result['frequency']:.4f}",
                                  mut['gene'],
                                  mut['mutation'],
                                  mut['position'],
                                  mut['reference'],
                                  mut['observed'],
                                  mut['match_type']
                              ])
                      else:
                          writer.writerow([
                              result['haplotype_id'],
                              result['coverage'],
                              f"{result['frequency']:.4f}",
                              'None',
                              'No_mutations',
                              '',
                              '',
                              '',
                              'Wild_type'
                          ])
              
              print(f"Analysis complete! Results written to {args.output_prefix}_*")
          
          if __name__ == "__main__":
              main()

baseCommand: [sh, -c]

arguments:
  - |
    python3 analyze_mutations.py "$(inputs.haplotypes.path)" "$(inputs.haplotype_counts.path)" "$(inputs.snp_positions.path)" "$(inputs.marker_id)" "$(inputs.output_prefix)"

inputs:
  haplotypes:
    type: File
    doc: "Haplotype sequences in FASTA format"

  haplotype_counts:
    type: File
    doc: "Haplotype coverage and frequency data"

  snp_positions:
    type: File
    doc: "Drug resistance SNP positions database"

  marker_id:
    type: string
    doc: "Marker identifier"

  output_prefix:
    type: string
    doc: "Output file prefix"

outputs:
  mutations_report:
    type: File
    outputBinding:
      glob: "*_mutations.txt"
    doc: "Detailed mutations report"

  resistance_summary:
    type: File
    outputBinding:
      glob: "*_resistance_summary.csv"
    doc: "Summary of detected resistance mutations"

  detailed_analysis:
    type: File
    outputBinding:
      glob: "*_detailed_analysis.csv"
    doc: "Detailed mutation analysis data"

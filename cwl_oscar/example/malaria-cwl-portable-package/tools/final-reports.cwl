#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Generate Final Reports (array inputs, no staging)"
doc: "Aggregate per-sample outputs into final CSVs/FASTAs matching the paper; accepts File[] via CLI flags."

requirements:
  DockerRequirement:
    dockerPull: python:3.13-slim
  InitialWorkDirRequirement:
    listing:
      - entryname: generate_final_reports.py
        entry: |
          #!/usr/bin/env python3
          import os, sys, csv, json, argparse, glob
          from collections import defaultdict

          def load_sample_mapping(input_yml_file):
              mapping = {}
              try:
                  with open(input_yml_file, 'r') as f:
                      lines = f.read().splitlines()
                  idx = 1
                  for line in lines:
                      if 'path:' in line and '.fastq' in line:
                          path = line.split('path:')[-1].strip()
                          name = os.path.basename(path).replace('.fastq.gz','').replace('_F','')
                          mapping[idx] = name
                          idx += 1
              except Exception:
                  pass
              return mapping

          def infer_marker_from_haplotype_file(path):
              base = os.path.basename(path)
              if '_dada2_haplotypes' in base:
                  return base.split('_dada2_haplotypes')[0]
              try:
                  with open(path, 'r') as f:
                      for line in f:
                          if line.startswith('>'):
                              hdr = line[1:].strip()
                              if '_hap' in hdr:
                                  return hdr.split('_hap')[0]
                              return hdr.split()[0]
              except Exception:
                  pass
              return 'Unknown'

          def main():
              p = argparse.ArgumentParser()
              p.add_argument('--detailed', action='append', default=[])
              p.add_argument('--resistance', action='append', default=[])
              p.add_argument('--haplotype', action='append', default=[])
              p.add_argument('--input-yml', required=True)
              p.add_argument('--output-dir', default='final_reports')
              a = p.parse_args()
              
              # Handle itemSeparator format - split on embedded flags
              def split_paths(path_list):
                  result = []
                  for item in path_list:
                      if ' --detailed ' in item:
                          result.extend(item.split(' --detailed '))
                      elif ' --resistance ' in item:
                          result.extend(item.split(' --resistance '))
                      elif ' --haplotype ' in item:
                          result.extend(item.split(' --haplotype '))
                      else:
                          result.append(item)
                  return [p.strip() for p in result if p.strip()]
              
              a.detailed = split_paths(a.detailed)
              a.resistance = split_paths(a.resistance)
              a.haplotype = split_paths(a.haplotype)

              os.makedirs(a.output_dir, exist_ok=True)
              sample_map = load_sample_mapping(a.input_yml)

              # Detailed analyses → haplotype list
              all_haps = []
              marker_to_haps = defaultdict(list)
              for fp in sorted(a.detailed):
                  if not os.path.exists(fp) or os.path.getsize(fp) < 50:
                      continue
                  base = os.path.basename(fp)
                  marker = base.split('_mutations_')[0] if '_mutations_' in base else 'Unknown'
                  # sample index if present
                  sample_id = sample_map.get(1, 'Sample_1')
                  if '_detailed_analysis.csv_' in base:
                      suf = base.split('_detailed_analysis.csv_')[-1]
                      try:
                          sample_id = sample_map.get(int(suf), f'Sample_{suf}')
                      except Exception:
                          sample_id = f'Sample_{suf}'
                  with open(fp, 'r') as f:
                      r = csv.DictReader(f)
                      i = 0
                      # Group mutations by haplotype to create position-based mutation strings
                      hap_mutations = defaultdict(list)
                      for row in r:
                          hap_id = row.get('HaplotypeID', f'{marker}_ID{i+1}')
                          mutation = row.get('Mutation', '')
                          position = row.get('Position', '')
                          
                          # For positional mutations (like original script), collect positions
                          if mutation and mutation != 'No_mutations' and position:
                              try:
                                  pos_num = int(position)
                                  hap_mutations[hap_id].append(f'p{pos_num}')
                              except ValueError:
                                  # Handle non-numeric positions or named mutations
                                  if mutation != 'None':
                                      hap_mutations[hap_id].append(mutation)
                      
                      # Create haplotype records with aggregated mutations
                      # Re-read the file since DictReader doesn't support seek
                  with open(fp, 'r') as f2:
                      r2 = csv.DictReader(f2)
                      processed_haps = set()
                      for row in r2:
                          hap_id = row.get('HaplotypeID', f'{marker}_ID{i+1}')
                          if hap_id in processed_haps:
                              continue
                          processed_haps.add(hap_id)
                          
                          cov = int(row.get('Coverage', 0)) if row.get('Coverage') else 0
                          mutations_list = hap_mutations.get(hap_id, [])
                          mutations_str = ','.join(sorted(set(mutations_list))) if mutations_list else ''
                          
                          rec = {'SampleID': sample_id, 'MarkerID': marker, 'Haplotype': hap_id, 'Reads': cov, 'Strain': 'NA', 'Mutations': mutations_str}
                          all_haps.append(rec)
                          marker_to_haps[marker].append(rec)
                          i += 1

              hap_csv = os.path.join(a.output_dir, 'finalHaplotypList_MinION_DR_CO.csv')
              with open(hap_csv, 'w', newline='') as f:
                  fn = ['SampleID','MarkerID','Haplotype','Reads','Strain','Mutations']
                  w = csv.DictWriter(f, fieldnames=fn, quoting=csv.QUOTE_ALL)
                  w.writeheader()
                  for rec in sorted(all_haps, key=lambda x: (x['MarkerID'], x['SampleID'])):
                      w.writerow(rec)

              # Resistance summaries → SNP matrix
              muts_by_sample = defaultdict(dict)
              all_positions = set()
              for fp in sorted(a.resistance):
                  if not os.path.exists(fp) or os.path.getsize(fp) < 50:
                      continue
                  base = os.path.basename(fp)
                  marker = base.split('_mutations_')[0] if '_mutations_' in base else 'Unknown'
                  sample_id = sample_map.get(1, 'Sample_1')
                  if '_resistance_summary.csv_' in base:
                      suf = base.split('_resistance_summary.csv_')[-1]
                      try:
                          sample_id = sample_map.get(int(suf), f'Sample_{suf}')
                      except Exception:
                          sample_id = f'Sample_{suf}'
                  with open(fp, 'r') as f:
                      r = csv.DictReader(f)
                      for row in r:
                          mut = row.get('Mutation', '')
                          freq = float(row.get('Frequency', 0) or 0)
                          if mut and mut != 'None' and freq > 0:
                              key = f'{marker}_{mut}'
                              all_positions.add(key)
                              muts_by_sample[sample_id][key] = mut

              snp_csv = os.path.join(a.output_dir, 'finalSNPList_MinION_DR_CO.csv')
              with open(snp_csv, 'w', newline='') as f:
                  w = csv.writer(f, quoting=csv.QUOTE_ALL)
                  pos_list = sorted(all_positions)
                  w.writerow([''] + pos_list)
                  for sid in sorted(muts_by_sample.keys()):
                      row = [sid]
                      for pos in pos_list:
                          val = muts_by_sample[sid].get(pos, '')
                          if val and len(val) > 1 and val[-1].isalpha():
                              val = val[-1]
                          row.append(val)
                      w.writerow(row)

              # Marker FASTAs from haplotype FASTAs
              for marker in sorted(marker_to_haps.keys()):
                  out_fa = os.path.join(a.output_dir, f'HaplotypeList_MinION_DR_CO_{marker}.fasta')
                  sel = [p for p in a.haplotype if infer_marker_from_haplotype_file(p) == marker]
                  with open(out_fa, 'w') as out:
                      count = 0
                      for fp in sorted(sel):
                          try:
                              with open(fp, 'r') as inf:
                                  content = inf.read().strip()
                                  if content:
                                      out.write(content)
                                      if not content.endswith('\n'):
                                          out.write('\n')
                                      count += 1
                          except Exception:
                              pass

              # Summary JSON
              total_reads = sum(int(h['Reads']) for h in all_haps) if all_haps else 0
              stats = {
                  'total_samples': len(set(h['SampleID'] for h in all_haps)),
                  'total_haplotypes': len(all_haps),
                  'markers_analyzed': len(set(h['MarkerID'] for h in all_haps)),
                  'samples_with_mutations': len(muts_by_sample),
                  'total_mutations_detected': sum(len(v) for v in muts_by_sample.values()),
                  'total_reads_analyzed': total_reads,
                  'average_reads_per_haplotype': (total_reads / len(all_haps)) if all_haps else 0.0
              }
              with open(os.path.join(a.output_dir, 'analysis_summary.json'), 'w') as f:
                  json.dump(stats, f, indent=2)

          if __name__ == '__main__':
              main()

baseCommand: [python3, generate_final_reports.py]

inputs:
  detailed_analyses:
    type: File[]
    inputBinding:
      prefix: --detailed
      itemSeparator: " --detailed "
  resistance_summaries:
    type: File[]
    inputBinding:
      prefix: --resistance
      itemSeparator: " --resistance "
  haplotypes:
    type: File[]
    inputBinding:
      prefix: --haplotype
      itemSeparator: " --haplotype "
  input_yml_file:
    type: File
    inputBinding:
      prefix: --input-yml
  output_dir:
    type: string
    default: "final_reports"
    inputBinding:
      prefix: --output-dir

outputs:
  final_haplotype_list:
    type: File
    outputBinding:
      glob: "final_reports/finalHaplotypList_MinION_DR_CO.csv"
  final_snp_list:
    type: File
    outputBinding:
      glob: "final_reports/finalSNPList_MinION_DR_CO.csv"
  marker_fastas:
    type: File[]
    outputBinding:
      glob: "final_reports/HaplotypeList_MinION_DR_CO_*.fasta"
  analysis_summary:
    type: File
    outputBinding:
      glob: "final_reports/analysis_summary.json"



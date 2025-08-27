#!/bin/bash
# Script to create a properly configured input file for your system

CURRENT_DIR=$(pwd)

echo "🔧 Creating test-input-local.yml for your system..."
echo "Current directory: $CURRENT_DIR"

# Create input file with current directory paths
cat > test-input-local.yml << EOF
# Test input file for Malaria Genomic Surveillance Workflow
# Auto-generated for: $CURRENT_DIR

# Raw nanopore sequencing data
raw_fastq:
  class: File
  path: $CURRENT_DIR/test_data/Pilot_study_1_DBS_DR_DR_dhfr_t26_F.fastq.gz

# Reference sequence for DHFR drug resistance marker
reference_fasta:
  class: File
  path: $CURRENT_DIR/data/DR_dhfr_t26.fasta

# Drug resistance SNP positions database
snp_positions:
  class: File
  path: $CURRENT_DIR/data/drug_resistance_SNP_positions.csv

# Target marker ID
marker_id: "DR_dhfr_t26"

# Quality filtering parameters
min_quality: 8
min_read_length: 100
max_read_length: 3000

# Processing threads
threads: 2
EOF

echo "✅ Created test-input-local.yml with absolute paths"
echo ""
echo "🔍 File contents:"
cat test-input-local.yml
echo ""
echo "🧪 To test the workflow:"
echo "   cwltool malaria-surveillance.cwl test-input-local.yml"

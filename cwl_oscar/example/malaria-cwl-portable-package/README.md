# 🧬 Malaria Drug Resistance CWL Workflow - Portable Package

This workflow reproduces the methodology from the original zenodo_10086238 study using DADA2 for haplotype reconstruction and comprehensive mutation analysis.

## 🚀 Quick Start

### Prerequisites for local run
- **CWL Runtime**: `cwltool` (Common Workflow Language reference implementation)
- **Docker**: For running containerized tools (DADA2, Python)
- **zenodo_10086238.zip**: download from https://zenodo.org/records/10086238 and uncompress in a folder named `zenodo_10086238` inside this same directory

## 📊 Available Workflows

### 1. 🎯 **Complete Analysis with Final Reports** (RECOMMENDED)
**File**: `malaria-with-reports-clean.cwl`

**Features**:
- ✅ Processes multiple samples in parallel
- ✅ Generates comprehensive final reports matching original study format
- ✅ Creates publication-ready outputs
- ✅ Single command for complete analysis

**Usage**:
```bash
cd workflows
cwltool --parallel malaria-with-reports-clean.cwl ../examples/portable-test-input.yml
```

**Outputs**:
- `finalHaplotypList_MinION_DR_CO.csv` - Complete haplotype listing
- `finalSNPList_MinION_DR_CO.csv` - Drug resistance mutation matrix  
- `HaplotypeList_MinION_DR_CO_*.fasta` - Per-marker sequence files
- `analysis_summary.json` - Comprehensive statistics
- Individual sample results (detailed analyses, FASTA files, etc.)

### 2. **Multi-Sample Scatter Processing**  
**File**: `malaria-scatter-workflow.cwl`

**Features**:
- ✅ Processes multiple samples in parallel
- ❌ No final report consolidation
- ✅ Individual sample outputs

**Usage**:
```bash
cd workflows  
cwltool --parallel malaria-scatter-workflow.cwl ../examples/portable-test-input.yml
```

### 3. **Single Sample Analysis**
**File**: `malaria-amplicon-workflow.cwl`

**Features**:
- ✅ Processes one sample at a time
- ✅ Good for testing and development
- ✅ Detailed individual outputs

**Usage**:
```bash
cd workflows
cwltool malaria-amplicon-workflow.cwl ../examples/portable-single-sample.yml  
```

## 🧬 Scientific Methodology

This workflow implements the complete malaria drug resistance analysis pipeline:

### Step 1: Quality Filtering (`amplicon-quality-filter.cwl`)
- Filters reads by length (10-2100 bp, matching original study)
- Removes low-quality sequences (Q8+ threshold)
- Eliminates N-containing reads
- Uses `seqtk` for processing

### Step 2: Haplotype Reconstruction (`dada2-haplotypes.cwl`)  
- **DADA2 error correction**: Denoising and error model learning
- **Chimera removal**: Eliminates PCR artifacts
- **Coverage filtering**: Minimum 65 reads per haplotype (original study threshold)
- **Frequency thresholds**: 2% minimum frequency cutoff
- Uses `blekhmanlab/dada2` Docker container

### Step 3: Drug Resistance Analysis (`mutation-analysis.cwl`)
- **SNP detection**: Compares haplotypes against known resistance positions
- **Mutation calling**: Identifies amino acid changes
- **Frequency calculation**: Quantifies mutation prevalence
- **Resistance profiling**: Generates comprehensive resistance summaries

### Step 4: Final Report Generation (Integrated in main workflow)
- **Data consolidation**: Aggregates results across all samples
- **Format standardization**: Matches original zenodo_10086238 study outputs
- **Publication-ready tables**: CSV matrices and FASTA sequences
- **Statistical summaries**: JSON format with comprehensive metrics

## 📝 Input File Format

### Multi-Sample Input (for main workflow)
```yaml
samples:
  - fastq_file:
      class: File
      path: path/to/sample1.fastq
    marker_id: "DR_dhfr_t25"
  - fastq_file:
      class: File  
      path: path/to/sample2.fastq
    marker_id: "DR_dhps_436-437"

snp_positions:
  class: File
  path: data/drug_resistance_SNP_positions.csv

input_yml_file:
  class: File
  path: input.yml  # Self-reference for sample mapping

# Optional parameters (defaults shown)
min_length: 10      # Minimum read length
max_length: 2100    # Maximum read length  
min_quality: 8      # Minimum quality score
min_coverage: 65    # Minimum haplotype coverage
```

### Single-Sample Input
```yaml
raw_fastq:
  class: File
  path: path/to/sample.fastq

marker_id: "DR_dhfr_t25"

snp_positions:
  class: File
  path: data/drug_resistance_SNP_positions.csv

# Optional parameters
min_length: 10
max_length: 2100
min_quality: 8
min_coverage: 65
```

## 🎯 Supported Markers

The workflow supports all drug resistance markers from the original study:

- **dhfr**: Dihydrofolate reductase (sulfadoxine-pyrimethamine resistance)
- **dhps**: Dihydropteroate synthase (sulfadoxine-pyrimethamine resistance)
- **k13**: Kelch13 (artemisinin resistance)
- **mdr1**: Multidrug resistance protein 1 (chloroquine/mefloquine resistance)
- **crt**: Chloroquine resistance transporter
- **And others** as defined in `drug_resistance_SNP_positions.csv`

## 🔧 Customization

### Adding New Samples
1. Place FASTQ files in a convenient location
2. Update the input YAML file to reference your samples:
```yaml
samples:
  - fastq_file:
      class: File
      path: /absolute/path/to/your/sample.fastq
    marker_id: "YOUR_MARKER_ID"
```

### Adjusting Parameters
All parameters can be overridden in your input YAML:
```yaml
# More stringent filtering
min_length: 50
max_length: 1500
min_quality: 15
min_coverage: 100
```

### Adding New Markers
1. Add marker-specific SNP positions to `drug_resistance_SNP_positions.csv`
2. Use the marker ID in your sample definitions

## 📊 Output Files

### Final Reports (from main workflow)
- **`finalHaplotypList_MinION_DR_CO.csv`**: Complete haplotype catalog with sample IDs, marker IDs, read counts, and mutations
- **`finalSNPList_MinION_DR_CO.csv`**: SNP matrix showing mutation status across all samples and positions
- **`HaplotypeList_MinION_DR_CO_*.fasta`**: FASTA files containing consensus sequences for each marker
- **`analysis_summary.json`**: Statistical summary with sample counts, mutation frequencies, and coverage metrics

### Individual Sample Results
- **`*_mutations_detailed_analysis.csv`**: Per-haplotype mutation details with coverage and frequency
- **`*_mutations_resistance_summary.csv`**: Summary of resistance mutations detected
- **`*_mutations_mutations.txt`**: Detailed mutation call information
- **`*_dada2_haplotypes.fasta`**: Reconstructed haplotype sequences
- **`*_filter_stats.txt`**: Quality filtering statistics

## 🏥 Clinical Applications

This workflow is designed for:
- **Drug resistance surveillance**: Population-level monitoring of resistance alleles
- **Clinical diagnostics**: Patient-specific resistance profiling  
- **Epidemiological monitoring**: Tracking resistance emergence and spread

## 📚 Citation

If you use this workflow in your research, please cite:

1. **Original Study**: [Include the original zenodo_10086238 paper citation]


## 🧪 Development

This package was developed and tested with:
- CWL v1.2 specification
- cwltool v3.1+  
- Docker Desktop
- macOS and Linux environments

### Testing
```bash
# Quick validation test
cd workflows
cwltool --validate malaria-with-reports-clean.cwl

# Run test with sample data
cwltool --parallel malaria-with-reports-clean.cwl ../examples/portable-test-input.yml
```

---

🎊 **Ready to analyze malaria drug resistance mutations!** 

Start with the test data, then substitute your own FASTQ files for analysis.

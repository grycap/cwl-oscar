# CWL2FDL

A Python script that converts Common Workflow Language (CWL) workflows to OSCAR Function Definition Language (FDL) format.

## Usage

```bash
python cwl2fdl.py <cwl_file> [--output-dir OUTPUT_DIR]
```

### Arguments

- `cwl_file`: Path to the CWL workflow file
- `--output-dir`: (Optional) Output directory for FDL files (default: 'fdl-output')

### Output

The tool generates:
- FDL configuration file (`oscar-functions.yml`)
- Executable shell scripts for each workflow step

## Requirements

- Python 3
- PyYAML 
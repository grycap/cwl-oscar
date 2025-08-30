# Local Runner Documentation

The Local Runner (`local_runner.py`) is a tool for running CWL workflows from your local machine on remote OSCAR clusters.

## What It Does

- ⬆️ **Upload** your local workflow and input files to OSCAR storage
- 🔄 **Execute** the workflow on remote OSCAR infrastructure  
- ⬇️ **Download** results back to your local machine
- 🧹 **Cleanup** temporary files automatically

## Quick Start

### Single Cluster

**With OIDC Token:**
```bash
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://oscar.example.com \
  --cluster-token your-oidc-token \
  cwl_oscar/example/hello.cwl \
  cwl_oscar/example/input_hello.json
```

**With Username/Password:**
```bash
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://oscar.example.com \
  --cluster-username your-username \
  --cluster-password your-password \
  cwl_oscar/example/hello.cwl \
  cwl_oscar/example/input_hello.json
```

### Multiple Clusters

When using multiple clusters, you need shared MinIO storage:

```bash
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://cluster1.example.com \
  --cluster-token token1 \
  --cluster-endpoint https://cluster2.example.com \
  --cluster-username user2 \
  --cluster-password pass2 \
  --shared-minio-endpoint https://minio.shared.com \
  --shared-minio-access-key ACCESS_KEY \
  --shared-minio-secret-key SECRET_KEY \
  cwl_oscar/example/workflow.cwl \
  cwl_oscar/example/input.json
```

## Common Options

### Authentication
- `--cluster-endpoint`: OSCAR cluster URL (required, can specify multiple)
- `--cluster-token`: OIDC token for authentication
- `--cluster-username` / `--cluster-password`: Basic authentication

### Execution
- `--parallel`: Enable parallel execution
- `--timeout 1200`: Set timeout in seconds (default: 600)
- `--output-dir ./my-results`: Specify output directory (default: ./results)
- `--service-name my-service`: OSCAR service name (default: cwl-oscar)

### Logging
- `--debug`: Show detailed debug information
- `--quiet`: Only show warnings and errors  
- `--verbose`: Default logging level

### Shared Storage (Multi-cluster)
- `--shared-minio-endpoint`: MinIO endpoint for shared storage
- `--shared-minio-access-key`: MinIO access key
- `--shared-minio-secret-key`: MinIO secret key
- `--shared-minio-region`: MinIO region (optional)

## Examples

### Simple Workflow
```bash
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://oscar.fedcloud.eu \
  --cluster-token abc123 \
  cwl_oscar/example/date.cwl \
  cwl_oscar/example/empty_input.json
```

### With Debug Output
```bash
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://oscar.fedcloud.eu \
  --cluster-username myuser \
  --cluster-password mypass \
  --debug \
  --timeout 900 \
  cwl_oscar/example/hello.cwl \
  cwl_oscar/example/input_hello.json
```

### Multi-cluster with Parallel Execution
```bash
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://hpc-cluster.edu \
  --cluster-token hpc-token \
  --cluster-endpoint https://cloud-cluster.com \
  --cluster-token cloud-token \
  --shared-minio-endpoint https://storage.shared.org \
  --shared-minio-access-key SHARED_ACCESS \
  --shared-minio-secret-key SHARED_SECRET \
  --parallel \
  --output-dir ./complex-results \
  workflows/complex-workflow.cwl \
  inputs/complex-input.json
```

## Troubleshooting

### Common Issues

**"Error: --cluster-endpoint is required"**
- Solution: Specify at least one cluster endpoint

**"Error: cluster 1 needs --cluster-password when using --cluster-username"**
- Solution: Provide both username and password for basic auth

**"Error: --shared-minio-endpoint is required for multi-cluster mode"**
- Solution: Configure shared MinIO when using multiple clusters

**"404 Client Error: Not Found"**
- Solution: Check your OSCAR endpoint URL is correct and accessible

### Debug Mode

Add `--debug` to see detailed execution logs:

```bash
python cwl_oscar/local_runner.py --debug \
  --cluster-endpoint https://oscar.example.com \
  --cluster-token your-token \
  workflow.cwl input.json
```

## Requirements

- Python 3.6+
- `oscar-python` package
- Access to OSCAR cluster(s)
- CWL workflow files
- Input JSON/YAML files

For more details, see the main [cwl-oscar documentation](cwl_oscar/README.md).

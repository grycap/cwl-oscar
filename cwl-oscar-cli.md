# CWL-OSCAR CLI Documentation

The `cwl-oscar-cli` command allows you to run CWL workflows from your local machine on remote OSCAR clusters.

## Installation

```bash
pip install cwl-oscar
```

## Setting Up the CWL-OSCAR Orchestrator

Before running workflows, you need to set up the cwl-oscar orchestrator service on your OSCAR cluster. This service acts as the workflow execution engine.

### Initialize the Orchestrator Service

**Remote OSCAR cluster:**
```bash
cwl-oscar-cli --init \
  --cluster-endpoint https://oscar.example.com \
  --cluster-token your-oidc-token
```

**Local OSCAR cluster (default localhost):**
```bash
cwl-oscar-cli --init \
  --cluster-token your-oidc-token
```

### Service Configuration

By default the orchestrator service is configured with:
- **Memory**: 500Mi
- **CPU**: 0.5 cores  
- **Storage**: Automatic MinIO buckets for input/output/mount
- **Environment**: Mount path and execution variables
- **Script**: Bash execution engine for CWL commands
- **Name**: cwl-oscar (set another name using the --service-name parameter)

You should see your service (default: `cwl-oscar`) in the oscar services page.

### Local vs Remote OSCAR Clusters

**Local OSCAR cluster behavior:**
- When no `--cluster-endpoint` is specified, defaults to `http://localhost`
- Automatically converts localhost endpoints to `http://oscar.oscar.svc.cluster.local:8080` in generated scripts (always HTTP)
- This allows the cwl-oscar service running inside Kubernetes to connect to the OSCAR API
- Authentication parameters (`--cluster-token` or `--cluster-username`/`--cluster-password`) are still required

**Remote OSCAR cluster behavior:**
- Requires explicit `--cluster-endpoint` specification
- Uses the provided endpoint directly in generated scripts
- Suitable for connecting to external OSCAR clusters

For multi-cluster workflows you only need the orchestrator on one of the clusters.

## What cwl-oscar-cli Does

- ⬆️ **Upload** your local workflow and input files to OSCAR storage
- 🔄 **Execute** the workflow on remote OSCAR infrastructure  
- ⬇️ **Download** results back to your local machine
- 🧹 **Cleanup** temporary files automatically

## Quick Start

### Single Cluster

**Remote OSCAR cluster:**
```bash
cwl-oscar-cli \
  --cluster-endpoint https://oscar.example.com \
  --cluster-token your-oidc-token \
  workflow.cwl input.json
```

**Local OSCAR cluster (default localhost):**
```bash
cwl-oscar-cli \
  --cluster-token your-oidc-token \
  workflow.cwl input.json
```

### Multiple Clusters

For workflows that need multiple clusters, configure shared MinIO storage:

```bash
cwl-oscar-cli \
  --cluster-endpoint https://cluster1.example.com \
  --cluster-token token1 \
  --cluster-endpoint https://cluster2.example.com \
  --cluster-token token2 \
  --shared-minio-endpoint https://minio.shared.com \
  --shared-minio-access-key ACCESS_KEY \
  --shared-minio-secret-key SECRET_KEY \
  workflow.cwl input.json
```

## Common Options

### Authentication
- `--cluster-endpoint`: OSCAR cluster URL (required, can specify multiple)
- `--cluster-token`: OIDC token for authentication
- `--cluster-username` / `--cluster-password`: Basic authentication

### Execution
- `--parallel`: Enable parallel execution
- `--timeout 1200`: Set timeout in seconds (default: 600)
- `--output-dir ./results`: Specify output directory
- `--service-name my-service`: OSCAR service name (default: cwl-oscar)
- `--cluster-steps`: Comma-separated list of workflow steps to execute on corresponding cluster

### Logging
- `--debug`: Show detailed debug information
- `--quiet`: Only show warnings and errors  
- `--verbose`: Default logging level

### Multi-cluster Storage
- `--shared-minio-endpoint`: MinIO endpoint for shared storage
- `--shared-minio-access-key`: MinIO access key
- `--shared-minio-secret-key`: MinIO secret key

### SSL Configuration
- `--cluster-disable-ssl`: Disable SSL verification (development only)
- `--shared-minio-disable-ssl`: Disable SSL for MinIO (development only)

## Step-to-Cluster Mapping

Assign specific workflow steps to specific clusters:

```bash
cwl-oscar-cli \
  --cluster-endpoint https://cpu-cluster.example.com \
  --cluster-token cpu-token \
  --cluster-steps data_prep,normalize \
  --cluster-endpoint https://gpu-cluster.example.com \
  --cluster-token gpu-token \
  --cluster-steps training,classify \
  --shared-minio-endpoint https://minio.shared.com \
  --shared-minio-access-key ACCESS_KEY \
  --shared-minio-secret-key SECRET_KEY \
  workflow.cwl input.json
```

## Examples

### Simple Workflow with Debug
```bash
cwl-oscar-cli \
  --cluster-endpoint https://oscar.fedcloud.eu \
  --cluster-token abc123 \
  --debug \
  --timeout 900 \
  hello.cwl input.json
```

### Multi-cluster Parallel Execution
```bash
cwl-oscar-cli \
  --cluster-endpoint https://hpc-cluster.edu \
  --cluster-token hpc-token \
  --cluster-endpoint https://cloud-cluster.com \
  --cluster-token cloud-token \
  --shared-minio-endpoint https://storage.shared.org \
  --shared-minio-access-key SHARED_ACCESS \
  --shared-minio-secret-key SHARED_SECRET \
  --parallel \
  --output-dir ./results \
  complex-workflow.cwl complex-input.json
```

## Troubleshooting

### Common Issues

**"Error: --cluster-endpoint is required"**
- Specify at least one cluster endpoint

**"Error: --shared-minio-endpoint is required for multi-cluster mode"**
- Configure shared MinIO when using multiple clusters

**"SSL Certificate verification failed"**
- Use `--cluster-disable-ssl` for development/testing only
- Use `--shared-minio-disable-ssl` for MinIO SSL issues

### Debug Mode

Add `--debug` to see detailed execution logs:

```bash
cwl-oscar-cli --debug \
  --cluster-endpoint https://oscar.example.com \
  --cluster-token your-token \
  workflow.cwl input.json
```

## Requirements

- Python 3.12+
- Access to OSCAR cluster(s)
- CWL workflow files
- Input JSON/YAML files

For more information, see the [main documentation](README.md).
# CWL-OSCAR

A repository containing tools for converting and executing Common Workflow Language (CWL) workflows with OSCAR clusters.

## Projects

- [cwl-oscar](./cwl_oscar) - CWL executor for OSCAR clusters
- [cwl-example](./cwl-example) - Example CWL workflows
- [fdl-examples](./fdl-examples) - FDL workflow examples
- [cwl2fdl script](./cwl2fdl.md) - CWL to FDL conversion utility

## Development Setup

### Prerequisites

- Python 3.6+
- pip

### Setting up the Development Environment

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd cwl-oscar
   ```

2. **Create and activate a virtual environment:**
   ```bash
   # Create virtual environment
   python -m venv .venv
   
   # Activate virtual environment
   # On Linux/macOS:
   source .venv/bin/activate
   
   # On Windows:
   .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify installation:**
   ```bash
   python cwl-oscar --version
   ```

### Testing

Run the comprehensive test suite:

```bash
# Test basic functionality (no OSCAR connection required)
python cwl_oscar/test_oscar.py

# Test with example workflow
python cwl-oscar --cluster-endpoint https://test.example.com \
                 --cluster-token dummy-token \
                 cwl_oscar/example/hello.cwl \
                 cwl_oscar/example/input.json
```

### Deactivating the Virtual Environment

When you're done developing:
```bash
deactivate
```

## Quick Start

### Option 1: Local Runner (Recommended for beginners)

Run workflows from local files on remote OSCAR clusters:

```bash
# Install dependencies
pip install -r requirements.txt

# Run a workflow on single cluster
python cwl_oscar/local_runner.py \
  --cluster-endpoint YOUR_OSCAR_ENDPOINT \
  --cluster-token YOUR_TOKEN \
  cwl_oscar/example/hello.cwl \
  cwl_oscar/example/input_hello.json

# Run with step-to-cluster mapping (multi-cluster)
python cwl_oscar/local_runner.py \
  --cluster-endpoint https://cpu-cluster.example.com \
  --cluster-token cpu-token \
  --cluster-steps create_file,data_prep \
  --cluster-endpoint https://gpu-cluster.example.com \
  --cluster-token gpu-token \
  --cluster-steps classify,training \
  --shared-minio-endpoint https://minio.shared.com \
  --shared-minio-access-key ACCESS_KEY \
  --shared-minio-secret-key SECRET_KEY \
  cwl_oscar/example/workflow.cwl \
  cwl_oscar/example/input.json
```

### Option 2: Direct CWL-OSCAR

Use the main cwl-oscar tool directly from the oscar service:

```bash
# Test installation
python cwl-oscar --version

# Execute workflow on single cluster
python cwl-oscar \
  --cluster-endpoint YOUR_OSCAR_ENDPOINT \
  --cluster-token YOUR_TOKEN \
  cwl_oscar/example/hello.cwl \
  cwl_oscar/example/input.json

# Execute with step-to-cluster mapping (multi-cluster)
python cwl-oscar \
  --cluster-endpoint https://cpu-cluster.example.com \
  --cluster-token cpu-token \
  --cluster-steps create_file,data_prep \
  --cluster-endpoint https://gpu-cluster.example.com \
  --cluster-token gpu-token \
  --cluster-steps classify,training \
  --shared-minio-endpoint https://minio.shared.com \
  --shared-minio-access-key ACCESS_KEY \
  --shared-minio-secret-key SECRET_KEY \
  cwl_oscar/example/workflow.cwl \
  cwl_oscar/example/input.json
```

## Key Features

### 🎯 Step-to-Cluster Mapping

Assign specific workflow steps to specific clusters for optimized resource usage:

- **CPU-intensive steps** → CPU clusters
- **GPU-intensive steps** → GPU clusters  
- **Memory-intensive steps** → High-memory clusters
- **Unmapped steps** → Automatic round-robin scheduling

**Benefits:**
- ⚡ **Performance**: Right workload on right hardware
- 💰 **Cost optimization**: Use expensive resources only when needed
- 🔄 **Flexibility**: Mix explicit mapping with automatic scheduling
- 📊 **Resource isolation**: Separate different types of workloads

### 🌐 Multi-Cluster Support

- Execute workflows across multiple OSCAR clusters
- Shared MinIO storage for seamless data transfer
- Automatic load balancing with round-robin scheduling
- SSL configuration per cluster

### 🔧 Execution Modes

- **Local Runner**: Upload local files, execute remotely, download results
- **Direct Execution**: Run workflows directly on OSCAR infrastructure
- **Docker Support**: Containerized execution environment

For detailed instructions:
- [Local Runner Guide](./LOCAL_RUNNER.md) - Simple workflow execution 
- [CWL-OSCAR Documentation](./cwl_oscar/README.md) - Advanced usage

## Docker Usage

### Quick Start with Docker

If you prefer using Docker instead of setting up a local Python environment:

1. **Build the Docker image:**
   ```bash
   ./docker-run.sh build
   ```

2. **Run cwl-oscar with Docker:**
   ```bash
   # Show version
   ./docker-run.sh run --version
   
   # Run a workflow with OIDC token
   ./docker-run.sh run --cluster-endpoint YOUR_OSCAR_ENDPOINT \
                       --cluster-token YOUR_TOKEN \
                       examples/hello.cwl examples/input.json
   
   # Run with step-to-cluster mapping
   ./docker-run.sh run --cluster-endpoint https://cpu-cluster.example.com \
                       --cluster-token cpu-token \
                       --cluster-steps create_file,data_prep \
                       --cluster-endpoint https://gpu-cluster.example.com \
                       --cluster-token gpu-token \
                       --cluster-steps classify,training \
                       --shared-minio-endpoint https://minio.shared.com \
                       --shared-minio-access-key ACCESS_KEY \
                       --shared-minio-secret-key SECRET_KEY \
                       examples/workflow.cwl examples/input.json
   ```

3. **Run tests in Docker:**
   ```bash
   ./docker-run.sh test
   ```

4. **Start an interactive shell:**
   ```bash
   ./docker-run.sh shell
   ```

### Docker Environment Variables

You can set these environment variables for easier usage:

```bash
export CLUSTER_ENDPOINT=https://oscar.fedcloud.eu
export CLUSTER_TOKEN=your_token_here
export SERVICE_NAME=run-script-event2
export MOUNT_PATH=/mnt/cwl2o-data/mount

# Now you can run without specifying these parameters
./docker-run.sh run examples/hello.cwl examples/input.json
```

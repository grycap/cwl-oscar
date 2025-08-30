# CWL OSCAR Executor

A CWL executor that runs Common Workflow Language (CWL) workflows on OSCAR clusters using the OSCAR Python client.

## Features

- **OSCAR Integration**: Executes CWL workflows on OSCAR clusters
- **Mounted Storage**: Uses shared mount paths for efficient data access
- **Multi-Cluster Support**: Execute workflows across multiple OSCAR clusters
- **Local Runner**: Run workflows from local files on remote OSCAR infrastructure
- **Token Authentication**: Supports OIDC token authentication
- **Synchronous Execution**: Currently supports synchronous job execution
- **Full CWL Compatibility**: Uses cwltool's core functionality for CWL parsing

## Prerequisites

- Python 3.6+
- cwltool
- oscar-python package
- Access to an OSCAR cluster

## Installation

### Option 1: Local Installation

1. Install dependencies:
```bash
pip install cwltool oscar-python
```

2. Make the entry point executable:
```bash
chmod +x cwl-oscar
```

### Option 2: Docker Installation

1. **Build the Docker image:**
   ```bash
   docker build -t cwl-oscar .
   # OR use the helper script
   ./docker-run.sh build
   ```

2. **Run with Docker:**
   ```bash
   # Using helper script (recommended)
   ./docker-run.sh run --cluster-endpoint YOUR_ENDPOINT --cluster-token YOUR_TOKEN workflow.cwl input.json
   
   # Using docker directly
   docker run --rm -v $(pwd):/workspace cwl-oscar \
     --cluster-endpoint YOUR_ENDPOINT --cluster-token YOUR_TOKEN \
     workflow.cwl input.json
   ```

3. **Run tests:**
   ```bash
   ./docker-run.sh test
   ```

## Configuration

### Required Parameters

- `--cluster-endpoint`: OSCAR cluster endpoint URL (can specify multiple for multi-cluster)

**Authentication (choose one per cluster):**
- `--cluster-token`: OSCAR OIDC authentication token
- `--cluster-username` + `--cluster-password`: OSCAR username and password for basic authentication

### Optional Parameters

- `--mount-path`: Mount path for shared data (default: `/mnt/cwl2o-data/mount`)
- `--service-name`: OSCAR service name to use (default: `run-script-event2`)
- `--cluster-disable-ssl`: Disable SSL verification for corresponding cluster
- `--shared-minio-endpoint`: Shared MinIO endpoint for multi-cluster support
- `--shared-minio-access-key`: Shared MinIO access key for multi-cluster support
- `--shared-minio-secret-key`: Shared MinIO secret key for multi-cluster support
- `--shared-minio-region`: Shared MinIO region for multi-cluster support
- `--shared-minio-disable-ssl`: Disable SSL certificate verification for shared MinIO
- `--parallel`: Enable parallel execution
- `--debug`: Enable detailed debug logging

## Usage

### Basic Usage

**Single cluster execution with OIDC token:**
```bash
./cwl-oscar --cluster-endpoint https://oscar.test.fedcloud.eu \
           --cluster-token YOUR_TOKEN \
           workflow.cwl inputs.json
```

**Single cluster execution with username/password:**
```bash
./cwl-oscar --cluster-endpoint https://oscar.test.fedcloud.eu \
           --cluster-username YOUR_USERNAME \
           --cluster-password YOUR_PASSWORD \
           workflow.cwl inputs.json
```

**Multi-cluster execution with shared storage:**
```bash
./cwl-oscar --cluster-endpoint https://cluster1.example.com \
           --cluster-token TOKEN1 \
           --cluster-endpoint https://cluster2.example.com \
           --cluster-username USER2 \
           --cluster-password PASS2 \
           --shared-minio-endpoint https://minio.shared.com \
           --shared-minio-access-key ACCESS_KEY \
           --shared-minio-secret-key SECRET_KEY \
           --parallel \
           workflow.cwl inputs.json
```

**Docker execution:**
```bash
# Using OIDC token
./docker-run.sh run --cluster-endpoint https://oscar.test.fedcloud.eu \
                    --cluster-token YOUR_TOKEN \
                    workflow.cwl inputs.json

# Using username/password
./docker-run.sh run --cluster-endpoint https://oscar.test.fedcloud.eu \
                    --cluster-username YOUR_USERNAME \
                    --cluster-password YOUR_PASSWORD \
                    workflow.cwl inputs.json

# Or with environment variables
export CLUSTER_ENDPOINT=https://oscar.test.fedcloud.eu
export CLUSTER_TOKEN=YOUR_TOKEN  # OR set CLUSTER_USERNAME and CLUSTER_PASSWORD
./docker-run.sh run workflow.cwl inputs.json
```

### With Custom Mount Path

```bash
# With OIDC token
./cwl-oscar --cluster-endpoint https://oscar.test.fedcloud.eu \
           --cluster-token YOUR_TOKEN \
           --mount-path /mnt/custom/mount \
           workflow.cwl inputs.json

# With username/password
./cwl-oscar --cluster-endpoint https://oscar.test.fedcloud.eu \
           --cluster-username YOUR_USERNAME \
           --cluster-password YOUR_PASSWORD \
           --mount-path /mnt/custom/mount \
           workflow.cwl inputs.json
```

### With Custom Service Name

```bash
./cwl-oscar --cluster-endpoint https://oscar.test.fedcloud.eu \
           --cluster-username YOUR_USERNAME \
           --cluster-password YOUR_PASSWORD \
           --service-name my-custom-service \
           workflow.cwl inputs.json
```

### Debug Mode

```bash
./cwl-oscar --cluster-endpoint https://oscar.test.fedcloud.eu \
           --cluster-username YOUR_USERNAME \
           --cluster-password YOUR_PASSWORD \
           --debug \
           workflow.cwl inputs.json
```

### SSL Configuration

By default, SSL certificate verification is enabled for both OSCAR clusters and shared MinIO storage. You can disable SSL verification when working with self-signed certificates or in development environments.

**Disable SSL for specific clusters:**
```bash
# Mixed SSL configuration: secure cluster + insecure cluster
./cwl-oscar --cluster-endpoint https://secure-cluster.com \
           --cluster-token TOKEN1 \
           --cluster-endpoint https://insecure-cluster.local \
           --cluster-token TOKEN2 \
           --cluster-disable-ssl \
           --shared-minio-endpoint https://minio.shared.com \
           --shared-minio-access-key ACCESS_KEY \
           --shared-minio-secret-key SECRET_KEY \
           workflow.cwl inputs.json
```

**Disable SSL for shared MinIO:**
```bash
# Secure clusters with insecure MinIO storage
./cwl-oscar --cluster-endpoint https://cluster1.com \
           --cluster-token TOKEN1 \
           --cluster-endpoint https://cluster2.com \
           --cluster-token TOKEN2 \
           --shared-minio-endpoint https://minio.local \
           --shared-minio-access-key ACCESS_KEY \
           --shared-minio-secret-key SECRET_KEY \
           --shared-minio-disable-ssl \
           workflow.cwl inputs.json
```

**Disable SSL for everything (development/testing only):**
```bash
# WARNING: Only use in development/testing environments
./cwl-oscar --cluster-endpoint https://dev-cluster.local \
           --cluster-token DEV_TOKEN \
           --cluster-disable-ssl \
           --shared-minio-endpoint https://dev-minio.local \
           --shared-minio-access-key DEV_ACCESS \
           --shared-minio-secret-key DEV_SECRET \
           --shared-minio-disable-ssl \
           workflow.cwl inputs.json
```

## Local Runner

The Local Runner (`local_runner.py`) allows you to run CWL workflows from your local machine on remote OSCAR clusters. It handles file uploads, workflow execution, and result downloads automatically.

### Usage

**Single Cluster with Token:**
```bash
python local_runner.py \
  --cluster-endpoint https://oscar.example.com \
  --cluster-token your-oidc-token \
  example/hello.cwl \
  example/input_hello.json
```

**Single Cluster with Username/Password:**
```bash
python local_runner.py \
  --cluster-endpoint https://oscar.example.com \
  --cluster-username your-username \
  --cluster-password your-password \
  example/hello.cwl \
  example/input_hello.json
```

**Multiple Clusters with Shared Storage:**
```bash
python local_runner.py \
  --cluster-endpoint https://cluster1.example.com \
  --cluster-token token1 \
  --cluster-endpoint https://cluster2.example.com \
  --cluster-username user2 \
  --cluster-password pass2 \
  --shared-minio-endpoint https://minio.shared.com \
  --shared-minio-access-key ACCESS_KEY \
  --shared-minio-secret-key SECRET_KEY \
  --parallel \
  --debug \
  example/workflow.cwl \
  example/input.json
```

### Common Options

- `--parallel`: Enable parallel execution
- `--debug`: Enable debug logging  
- `--quiet`: Only show warnings and errors
- `--timeout 1200`: Set timeout in seconds
- `--output-dir ./results`: Specify output directory
- `--service-name my-service`: Use custom OSCAR service name

### How It Works

1. **Upload**: Uploads your local workflow and input files to OSCAR storage
2. **Execute**: Runs the workflow on the OSCAR cluster(s) 
3. **Download**: Downloads results back to your local machine
4. **Cleanup**: Removes temporary files

## How It Works

1. **CWL Parsing**: Uses cwltool to parse CWL workflows and job orders
2. **Command Generation**: Generates bash scripts containing the CWL commands
3. **OSCAR Submission**: Submits jobs to the specified OSCAR service
4. **Path Mapping**: Maps local file paths to mount paths for OSCAR execution
5. **Result Collection**: Collects results from OSCAR execution

## Architecture

### Key Components

- **OSCARCommandLineTool**: CWL CommandLineTool implementation for OSCAR
- **OSCARTask**: Job execution handler that interfaces with OSCAR
- **OSCARExecutor**: Modular executor interface for command execution
- **OSCARPathMapper**: Path mapping for mount-based file access

### Execution Flow

```
CWL Workflow → cwltool parsing → OSCARCommandLineTool → OSCARTask → OSCARExecutor → OSCAR Service
```

## OSCAR Service Requirements

The OSCAR service used for execution should:

1. Have the required mount path configured
2. Accept JSON input with script content
3. Execute bash scripts
4. Return appropriate exit codes

### Example OSCAR Service Configuration

```yaml
functions:
  oscar:
  - your-service-name:
      name: your-service-name
      memory: 1Gi
      cpu: '1.0'
      image: opensourcefoundries/minideb:jessie
      script: script.sh
      environment:
        variables:
          MOUNT_PATH: "/mnt/cwl2o-data/mount"
      mount:
        storage_provider: minio.default
        path: /cwl2o-data/mount
```

## Environment Variables

The executor sets the following environment variables for OSCAR jobs:

- `CWL_JOB_NAME`: Name of the CWL job
- `CWL_MOUNT_PATH`: Mount path for shared data
- Plus all CWL-specific environment variables

## Limitations

- Currently supports only synchronous execution
- Assumes OSCAR service exists and is properly configured
- Limited error handling for OSCAR service failures
- Path mapping assumes files are available in mount path

## Development

### Setting up Development Environment

1. **Navigate to the project root and activate virtual environment:**
   ```bash
   cd /path/to/cwl-oscar
   
   # Create virtual environment (if not exists)
   python -m venv .venv
   
   # Activate virtual environment
   # On Linux/macOS:
   source .venv/bin/activate
   
   # On Windows:
   .venv\Scripts\activate
   ```

2. **Install development dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Test the installation:**
   ```bash
   python cwl-oscar --version
   python test_oscar.py  # Run test suite
   ```

4. **When finished developing:**
   ```bash
   deactivate  # Exit virtual environment
   ```

### Extending the Executor

To customize the execution behavior, modify the `OSCARExecutor` class:

```python
class CustomOSCARExecutor(OSCARExecutor):
    def execute_command(self, command, environment, working_directory, job_name):
        # Your custom execution logic here
        return super().execute_command(command, environment, working_directory, job_name)
```

### Adding New Service Types

To support different OSCAR service types, extend the `make_oscar_tool` function:

```python
def make_oscar_tool(spec, loading_context, oscar_endpoint, oscar_token, mount_path, service_name):
    if spec["class"] == "CommandLineTool":
        return OSCARCommandLineTool(spec, loading_context, oscar_endpoint, oscar_token, mount_path, service_name)
    elif spec["class"] == "CustomTool":
        return CustomOSCARTool(spec, loading_context, oscar_endpoint, oscar_token, mount_path, service_name)
    else:
        return default_make_tool(spec, loading_context)
```

## Troubleshooting

### Common Issues

1. **Authentication Error**: Verify your OSCAR token is valid and has proper permissions
2. **Service Not Found**: Ensure the OSCAR service exists and is accessible
3. **Mount Path Issues**: Check that the mount path is properly configured in both the executor and OSCAR service
4. **Command Execution Failures**: Review OSCAR service logs for detailed error information

### Debug Mode

Enable debug logging to see detailed execution information:

```bash
./cwl-oscar --debug --cluster-endpoint ... --cluster-token ... workflow.cwl inputs.json
```


## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review OSCAR service logs
3. Open an issue in the repository 
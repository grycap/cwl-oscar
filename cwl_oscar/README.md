# CWL OSCAR Executor

A CWL executor that runs Common Workflow Language (CWL) workflows on OSCAR clusters using the OSCAR Python client.

## Features

- **OSCAR Integration**: Executes CWL workflows on OSCAR clusters
- **Mounted Storage**: Uses shared mount paths for efficient data access
- **Modular Execution Interface**: Clean separation between CWL integration and OSCAR execution
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
   ./docker-run.sh run --oscar-endpoint YOUR_ENDPOINT --oscar-token YOUR_TOKEN workflow.cwl input.json
   
   # Using docker directly
   docker run --rm -v $(pwd):/workspace cwl-oscar \
     --oscar-endpoint YOUR_ENDPOINT --oscar-token YOUR_TOKEN \
     workflow.cwl input.json
   ```

3. **Run tests:**
   ```bash
   ./docker-run.sh test
   ```

## Configuration

### Required Parameters

- `--oscar-endpoint`: OSCAR cluster endpoint URL

**Authentication (choose one):**
- `--oscar-token`: OSCAR OIDC authentication token
- `--oscar-username` + `--oscar-password`: OSCAR username and password for basic authentication

### Optional Parameters

- `--mount-path`: Mount path for shared data (default: `/mnt/cwl2o-data/mount`)
- `--service-name`: OSCAR service name to use (default: `run-script-event2`)

## Usage

### Basic Usage

**Local execution with OIDC token:**
```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-token YOUR_TOKEN \
           workflow.cwl inputs.json
```

**Local execution with username/password:**
```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-username YOUR_USERNAME \
           --oscar-password YOUR_PASSWORD \
           workflow.cwl inputs.json
```

**Docker execution:**
```bash
# Using OIDC token
./docker-run.sh run --oscar-endpoint https://oscar.test.fedcloud.eu \
                    --oscar-token YOUR_TOKEN \
                    workflow.cwl inputs.json

# Using username/password
./docker-run.sh run --oscar-endpoint https://oscar.test.fedcloud.eu \
                    --oscar-username YOUR_USERNAME \
                    --oscar-password YOUR_PASSWORD \
                    workflow.cwl inputs.json

# Or with environment variables
export OSCAR_ENDPOINT=https://oscar.test.fedcloud.eu
export OSCAR_TOKEN=YOUR_TOKEN  # OR set OSCAR_USERNAME and OSCAR_PASSWORD
./docker-run.sh run workflow.cwl inputs.json
```

### With Custom Mount Path

```bash
# With OIDC token
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-token YOUR_TOKEN \
           --mount-path /mnt/custom/mount \
           workflow.cwl inputs.json

# With username/password
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-username YOUR_USERNAME \
           --oscar-password YOUR_PASSWORD \
           --mount-path /mnt/custom/mount \
           workflow.cwl inputs.json
```

### With Custom Service Name

```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-username YOUR_USERNAME \
           --oscar-password YOUR_PASSWORD \
           --service-name my-custom-service \
           workflow.cwl inputs.json
```

### Debug Mode

```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-username YOUR_USERNAME \
           --oscar-password YOUR_PASSWORD \
           --debug \
           workflow.cwl inputs.json
```

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
   cd /path/to/cwl2oscar
   
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
./cwl-oscar --debug --oscar-endpoint ... --oscar-token ... workflow.cwl inputs.json
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
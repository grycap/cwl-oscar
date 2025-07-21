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

1. Install dependencies:
```bash
pip install cwltool oscar-python
```

2. Make the entry point executable:
```bash
chmod +x cwl-oscar
```

## Configuration

### Required Parameters

- `--oscar-endpoint`: OSCAR cluster endpoint URL
- `--oscar-token`: OSCAR authentication token

### Optional Parameters

- `--mount-path`: Mount path for shared data (default: `/mnt/cwl2o-data/mount`)
- `--service-name`: OSCAR service name to use (default: `run-script-event2`)

## Usage

### Basic Usage

```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-token YOUR_TOKEN \
           workflow.cwl inputs.json
```

### With Custom Mount Path

```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-token YOUR_TOKEN \
           --mount-path /mnt/custom/mount \
           workflow.cwl inputs.json
```

### With Custom Service Name

```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-token YOUR_TOKEN \
           --service-name my-custom-service \
           workflow.cwl inputs.json
```

### Debug Mode

```bash
./cwl-oscar --oscar-endpoint https://oscar.test.fedcloud.eu \
           --oscar-token YOUR_TOKEN \
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

## License

This project follows the same license as the parent cwl-tes project.

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
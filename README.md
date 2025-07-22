# cwl2oscar

A repository containing tools for converting and executing Common Workflow Language (CWL) workflows with OSCAR clusters.

## Projects

- [cwl-oscar](./cwl_oscar) - CWL executor for OSCAR clusters
- [cwl-example](./cwl-example) - Example CWL workflows
- [fdl-example](./fdl-example) - FDL workflow examples
- [cwl2fdl script](./cwl2fdl.md) - CWL to FDL conversion utility

## Development Setup

### Prerequisites

- Python 3.6+
- pip

### Setting up the Development Environment

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd cwl2oscar
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
python cwl-oscar --oscar-endpoint https://test.example.com \
                 --oscar-token dummy-token \
                 cwl_oscar/example/hello.cwl \
                 cwl_oscar/example/input.json
```

### Deactivating the Virtual Environment

When you're done developing:
```bash
deactivate
```

## Quick Start

1. Activate the virtual environment (see setup above)
2. Run a simple test:
   ```bash
   python cwl-oscar --version
   ```
3. Execute a workflow on OSCAR:
   ```bash
   # With OIDC token
   python cwl-oscar --oscar-endpoint YOUR_OSCAR_ENDPOINT \
                    --oscar-token YOUR_TOKEN \
                    cwl_oscar/example/hello.cwl \
                    cwl_oscar/example/input.json
   
   # With username/password  
   python cwl-oscar --oscar-endpoint YOUR_OSCAR_ENDPOINT \
                    --oscar-username YOUR_USERNAME \
                    --oscar-password YOUR_PASSWORD \
                    cwl_oscar/example/hello.cwl \
                    cwl_oscar/example/input.json
   ```

For detailed usage instructions, see the [cwl-oscar documentation](./cwl_oscar/README.md).

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
   ./docker-run.sh run --oscar-endpoint YOUR_OSCAR_ENDPOINT \
                       --oscar-token YOUR_TOKEN \
                       examples/hello.cwl examples/input.json
   
   # Run a workflow with username/password
   ./docker-run.sh run --oscar-endpoint YOUR_OSCAR_ENDPOINT \
                       --oscar-username YOUR_USERNAME \
                       --oscar-password YOUR_PASSWORD \
                       examples/hello.cwl examples/input.json
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
export OSCAR_ENDPOINT=https://oscar.fedcloud.eu
export OSCAR_TOKEN=your_token_here
export SERVICE_NAME=run-script-event2
export MOUNT_PATH=/mnt/cwl2o-data/mount

# Now you can run without specifying these parameters
./docker-run.sh run examples/hello.cwl examples/input.json
```

### Using docker-compose

Alternatively, you can use docker-compose:

```bash
# Build and run with docker-compose
docker-compose run cwl-oscar --version

# Run tests
docker-compose run cwl-oscar-test

# Development mode with live code editing
docker-compose run cwl-oscar-dev --help
```
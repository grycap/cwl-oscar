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
   python cwl-oscar --oscar-endpoint YOUR_OSCAR_ENDPOINT \
                    --oscar-token YOUR_TOKEN \
                    cwl_oscar/example/hello.cwl \
                    cwl_oscar/example/input.json
   ```

For detailed usage instructions, see the [cwl-oscar documentation](./cwl_oscar/README.md).
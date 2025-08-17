"""OSCAR-specific implementation for CWL execution."""
from __future__ import absolute_import, print_function, unicode_literals

import functools
import logging
import os
import json
import time
from typing import Any, Dict, List, MutableMapping, MutableSequence, Optional, Union
from typing_extensions import Text

from cwltool.builder import Builder
from cwltool.command_line_tool import CommandLineTool
from cwltool.context import RuntimeContext
from cwltool.errors import WorkflowException
from cwltool.job import JobBase
from cwltool.pathmapper import PathMapper, MapperEnt
from cwltool.workflow import default_make_tool

# Import OSCAR Python client
try:
    from oscar_python.client import Client
except ImportError:
    raise ImportError("oscar-python package is required. Install with: pip install oscar-python")

import uuid
import tempfile
import shutil
import shlex
import hashlib
import yaml
import contextlib

log = logging.getLogger("oscar-backend")


@contextlib.contextmanager
def suppress_stdout_to_stderr():
    """Context manager to redirect stdout to stderr during oscar-python operations.
    
    This prevents oscar-python library messages from contaminating the JSON output.
    """
    import sys
    original_stdout = sys.stdout
    try:
        sys.stdout = sys.stderr
        yield
    finally:
        sys.stdout = original_stdout


class OSCARServiceManager:
    """Manages dynamic OSCAR service creation based on CommandLineTool requirements."""
    
    def __init__(self, oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, ssl=True, shared_minio_config=None):
        log.debug("OSCARServiceManager: Initializing service manager")
        log.debug("OSCARServiceManager: OSCAR endpoint: %s", oscar_endpoint)
        log.debug("OSCARServiceManager: Mount path: %s", mount_path)
        log.debug("OSCARServiceManager: Using token auth: %s", bool(oscar_token))
        log.debug("OSCARServiceManager: Using username/password auth: %s", bool(oscar_username and oscar_password))
        
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.mount_path = mount_path
        self.ssl = ssl
        self.client = None
        self._service_cache = {}  # Cache created services
        self.shared_minio_config = shared_minio_config
        
        log.debug("OSCARServiceManager: Service manager initialized successfully")
        
    def get_client(self):
        """Get or create OSCAR client."""
        if self.client is None:
            log.debug("OSCARServiceManager: Creating new OSCAR client")
            
            if self.oscar_token:
                # Use OIDC token authentication
                log.debug("OSCARServiceManager: Using OIDC token authentication")
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'oidc_token': self.oscar_token,
                    'ssl': str(self.ssl)
                }
            else:
                # Use basic username/password authentication
                log.debug("OSCARServiceManager: Using username/password authentication")
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'user': self.oscar_username,
                    'password': self.oscar_password,
                    'ssl': str(self.ssl)
                }
            
            log.debug("OSCARServiceManager: Client options: %s", {k: '***' if k in ['oidc_token', 'password'] else v for k, v in options.items()})
            self.client = Client(options=options)
            log.debug("OSCARServiceManager: OSCAR client created successfully")
        else:
            log.debug("OSCARServiceManager: Reusing existing OSCAR client")
            
        return self.client
        
    def extract_service_requirements(self, tool_spec):
        """Extract service requirements from CommandLineTool specification."""
        log.debug("OSCARServiceManager: Extracting service requirements from tool spec")
        log.debug("OSCARServiceManager: Tool ID: %s", tool_spec.get('id', 'unknown'))
        log.debug("OSCARServiceManager: Tool baseCommand: %s", tool_spec.get('baseCommand', 'unknown'))
        
        requirements = {
            'memory': '1Gi',  # Default
            'cpu': '1.0',     # Default  
            'image': 'opensourcefoundries/minideb:jessie',  # Default
            'environment': {}
        }
        log.debug("OSCARServiceManager: Default requirements: %s", requirements)
        
        # Check for DockerRequirement
        if 'requirements' in tool_spec:
            log.debug("OSCARServiceManager: Processing %d requirements", len(tool_spec['requirements']))
            for req in tool_spec['requirements']:
                req_class = req.get('class')
                log.debug("OSCARServiceManager: Processing requirement class: %s", req_class)
                
                if req_class == 'DockerRequirement':
                    if 'dockerPull' in req:
                        old_image = requirements['image']
                        requirements['image'] = req['dockerPull']
                        log.debug("OSCARServiceManager: Updated Docker image from '%s' to '%s'", old_image, requirements['image'])
                        
                elif req_class == 'ResourceRequirement':
                    if 'ramMin' in req:
                        ram_mb = req['ramMin']
                        old_memory = requirements['memory']
                        requirements['memory'] = f"{ram_mb}Mi"
                        log.debug("OSCARServiceManager: Updated memory from '%s' to '%s'", old_memory, requirements['memory'])
                    if 'coresMin' in req:
                        old_cpu = requirements['cpu']
                        requirements['cpu'] = str(req['coresMin'])
                        log.debug("OSCARServiceManager: Updated CPU from '%s' to '%s'", old_cpu, requirements['cpu'])
                        
                elif req_class == 'EnvVarRequirement':
                    if 'envDef' in req:
                        # envDef is a dictionary in CWL spec
                        if isinstance(req['envDef'], dict):
                            log.debug("OSCARServiceManager: Adding %d environment variables", len(req['envDef']))
                            requirements['environment'].update(req['envDef'])
                        else:
                            # Handle legacy format if it's a list
                            log.debug("OSCARServiceManager: Processing legacy envDef list format")
                            for env_def in req['envDef']:
                                if isinstance(env_def, dict):
                                    requirements['environment'][env_def['envName']] = env_def['envValue']
                                    log.debug("OSCARServiceManager: Added env var: %s=%s", env_def['envName'], env_def['envValue'])
        
        # Check hints as well
        if 'hints' in tool_spec:
            log.debug("OSCARServiceManager: Processing %d hints", len(tool_spec['hints']))
            for hint in tool_spec['hints']:
                hint_class = hint.get('class')
                log.debug("OSCARServiceManager: Processing hint class: %s", hint_class)
                
                if hint_class == 'DockerRequirement':
                    if 'dockerPull' in hint:
                        old_image = requirements['image']
                        requirements['image'] = hint['dockerPull']
                        log.debug("OSCARServiceManager: Updated Docker image from hint: '%s' to '%s'", old_image, requirements['image'])
        
        log.debug("OSCARServiceManager: Final extracted requirements: %s", requirements)
        return requirements
        
    def generate_service_name(self, tool_spec, requirements, job_name=None):
        """Generate a unique service name based on tool and requirements."""
        log.debug("OSCARServiceManager: Generating service name for tool")
        
                # Use job_name if provided, otherwise use "tool"
        if job_name:
            tool_id = job_name
            log.debug("OSCARServiceManager: Using provided job_name as tool ID: '%s'", tool_id)
        else:
            tool_id = "tool"
            log.debug("OSCARServiceManager: No job_name provided, using default tool ID: '%s'", tool_id)
            
        # Create a hash based on tool content and requirements
        tool_content = json.dumps({
            'baseCommand': tool_spec.get('baseCommand'),
            'class': tool_spec.get('class'),
            'requirements': requirements
        }, sort_keys=True)
        log.debug("OSCARServiceManager: Tool content for hashing: %s", tool_content)
        
        service_hash = hashlib.md5(tool_content.encode()).hexdigest()[:8]
        log.debug("OSCARServiceManager: Generated service hash: %s", service_hash)
        
        # Use tool_id directly without cleaning
        if not tool_id:
            tool_id = 'tool'
            log.debug("OSCARServiceManager: Empty tool ID, using default: '%s'", tool_id)
        
        # Ensure the service name follows Kubernetes naming rules (RFC 1123 subdomain)
        # Replace underscores with hyphens and ensure only lowercase alphanumeric + hyphens
        clean_tool_id = tool_id.lower().replace('_', '-')
        # Remove any other invalid characters, keep only a-z, 0-9, and hyphens
        import re
        clean_tool_id = re.sub(r'[^a-z0-9-]', '', clean_tool_id)
        # Ensure it doesn't start or end with a hyphen
        clean_tool_id = clean_tool_id.strip('-')
        # Ensure it's not empty
        if not clean_tool_id:
            clean_tool_id = 'tool'
        
        final_service_name = f"clt-{clean_tool_id}-{service_hash}"
        log.debug("OSCARServiceManager: Final generated service name: '%s'", final_service_name)
        return final_service_name
        

        
    def create_service_definition(self, service_name, requirements, mount_path, shared_minio_config=None):
        """Create OSCAR service definition."""
        log.debug("OSCARServiceManager: Creating service definition for service: %s", service_name)
        log.debug("OSCARServiceManager: Requirements: %s", requirements)
        log.debug("OSCARServiceManager: Mount path: %s", mount_path)
        
        # Extract mount path components for the mount configuration
        mount_parts = mount_path.strip('/').split('/')
        log.debug("OSCARServiceManager: Mount path parts: %s", mount_parts)
        
        # Remove 'mnt' prefix if present to get the actual mount path
        if mount_parts[0] == 'mnt':
            mount_base = '/'.join(mount_parts[1:])
            log.debug("OSCARServiceManager: Removed 'mnt' prefix, mount base: %s", mount_base)
        else:
            mount_base = '/'.join(mount_parts)
            log.debug("OSCARServiceManager: No 'mnt' prefix, mount base: %s", mount_base)
        
        # Embed the script content directly
        script_content = '''#!/bin/bash

# Debug: Show environment variables
echo "=== Environment Variables ==="
echo "INPUT_FILE_PATH: $INPUT_FILE_PATH"
echo "TMP_OUTPUT_DIR: $TMP_OUTPUT_DIR"
echo "MOUNT_PATH: $MOUNT_PATH"
echo "=============================="

# Sleep for 5 seconds
sleep 5

FILE_NAME=$(basename "$INPUT_FILE_PATH")

# Check if required environment variables are set
if [ -z "$INPUT_FILE_PATH" ]; then
    echo "ERROR: INPUT_FILE_PATH environment variable not set"
    exit 1
fi

if [ -z "$MOUNT_PATH" ]; then
    echo "ERROR: MOUNT_PATH environment variable not set"
    exit 1
fi

# Check if the mount path is available
echo "[script.sh] Checking if the mount path is available"
ls -lah /mnt


# Check if the input command script exists
if [ ! -f "$INPUT_FILE_PATH" ]; then
    echo "ERROR: Command script not found at $INPUT_FILE_PATH"
    exit 1
fi

echo "SCRIPT: Executing command script: $INPUT_FILE_PATH"


# Execute the command script with bash
# The command script will handle its own working directory and environment setup
# Redirect stdout to out.log and stderr to err.log
bash "$INPUT_FILE_PATH" > "$TMP_OUTPUT_DIR/$FILE_NAME.out.log" 2> "$TMP_OUTPUT_DIR/$FILE_NAME.err.log"
exit_code=$?


echo "SCRIPT: Command completed with exit code: $exit_code"

# Create output file in TMP_OUTPUT_DIR for OSCAR to detect completion
if [ -n "$TMP_OUTPUT_DIR" ]; then
    OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME.exit_code"
    echo "$exit_code" > "$OUTPUT_FILE"
    echo "SCRIPT: Exit code written to: $OUTPUT_FILE"
fi

echo "Script completed."
exit $exit_code
'''
        
        log.debug("OSCARServiceManager: Using embedded script content (%d characters)", len(script_content))
        
        service_def = {
            'name': service_name,
            'memory': requirements['memory'],
            'cpu': requirements['cpu'],
            'image': requirements['image'],
            'script': script_content,
            'environment': {
                'variables': {
                    'MOUNT_PATH': mount_path,
                    **requirements['environment']
                }
            },
            'input': [{
                'storage_provider': 'minio.default',
                'path': f'{service_name}/in'
            }],
            'output': [{
                'storage_provider': 'minio.default', 
                'path': f'{service_name}/out'
            }],
            'mount': {
                'storage_provider': 'minio.default',
                'path': f'/{mount_base}'
            }
        }
        
        # Add storage_providers if shared MinIO is configured
        if shared_minio_config:
            service_def["storage_providers"] = {
                "minio": {
                    "shared": {
                        "endpoint": shared_minio_config["endpoint"],
                        "verify": shared_minio_config.get("verify_ssl", False),
                        "access_key": shared_minio_config["access_key"],
                        "secret_key": shared_minio_config["secret_key"],
                        "region": shared_minio_config.get("region") or "us-east-1"  # Default to us-east-1 if region is None/null
                    }
                }
            }
            
            # Update only mount to use shared MinIO, keep input/output as minio.default
            service_def["mount"]["storage_provider"] = "minio.shared"
        
        log.debug("OSCARServiceManager: Created service definition: %s", json.dumps(service_def, indent=2))
        return service_def
        
    def get_or_create_service(self, tool_spec, job_name=None):
        """Get existing service or create new one for the CommandLineTool."""
        log.debug("OSCARServiceManager: Starting get_or_create_service for tool: %s", tool_spec.get('id', 'unknown'))
        
        requirements = self.extract_service_requirements(tool_spec)
        service_name = self.generate_service_name(tool_spec, requirements, job_name)
        
        log.info("OSCARServiceManager: Generated service name '%s' for tool '%s'", service_name, tool_spec.get('id', 'unknown'))
        
        # Check cache first
        if service_name in self._service_cache:
            log.debug("OSCARServiceManager: Using cached service: %s", service_name)
            log.info("OSCARServiceManager: Service '%s' found in cache, reusing existing service", service_name)
            return service_name
            
        log.debug("OSCARServiceManager: Service not in cache, checking OSCAR cluster")
        client = self.get_client()
        
        # Check if service already exists
        def check_service_exists(name):
            log.debug("OSCARServiceManager: Checking if service '%s' exists on OSCAR cluster", name)
            try:
                services_response = client.list_services()
                log.debug("OSCARServiceManager: List services response status: %d", services_response.status_code)
                
                if services_response.status_code == 200:
                    existing_services = json.loads(services_response.text)
                    log.debug("OSCARServiceManager: Found %d existing services on cluster", len(existing_services))
                    
                    for service in existing_services:
                        service_name_in_list = service.get('name')
                        log.debug("OSCARServiceManager: Checking service: %s", service_name_in_list)
                        if service_name_in_list == name:
                            log.info("OSCARServiceManager: Service already exists on cluster: %s", name)
                            self._service_cache[name] = service
                            return service
                    
                    log.debug("OSCARServiceManager: Service '%s' not found among existing services", name)
                else:
                    log.warning("OSCARServiceManager: Failed to list services, status code: %d", services_response.status_code)
                    
            except Exception as e:
                log.warning("OSCARServiceManager: Could not check existing services: %s", e)
            return None
        
        # First check if service exists
        existing_service = check_service_exists(service_name)
        if existing_service:
            log.info("OSCARServiceManager: Using existing service: %s", service_name)
            return service_name
            
        # Create new service with retry logic
        log.info("OSCARServiceManager: Creating new service for tool: %s -> %s", tool_spec.get('id', 'unknown'), service_name)
        service_def = self.create_service_definition(service_name, requirements, self.mount_path, self.shared_minio_config)
        
        max_retries = 3
        retry_delay = 2  # seconds
        last_exception = None
        import time  # Import time at the beginning
        
        for attempt in range(1, max_retries + 1):
            log.info("OSCARServiceManager: Attempt %d/%d to create service %s", attempt, max_retries, service_name)
            
            try:
                # Create service using OSCAR API - pass the complete service definition including storage_providers
                # According to OSCAR API spec, we need the full service definition, not just the inner grnet part
                log.debug("OSCARServiceManager: Sending service creation request to OSCAR API")
                log.debug("OSCARServiceManager: Complete service definition to create: %s", json.dumps(service_def, indent=2))
                
                response = client.create_service(service_def)
                log.debug("OSCARServiceManager: Service creation response status: %d", response.status_code)
                log.debug("OSCARServiceManager: Service creation response text: %s", response.text)
                
                # Wait a bit for service setup to complete regardless of response
                log.debug("OSCARServiceManager: Waiting 3 seconds for service setup to complete")
                time.sleep(3)
                
                # Always check if service was created, regardless of API response
                log.debug("OSCARServiceManager: Verifying service creation by checking if service exists")
                created_service = check_service_exists(service_name)
                if created_service:
                    log.info("OSCARServiceManager: Service successfully created and verified: %s", service_name)
                    self._service_cache[service_name] = service_def
                    return service_name
                
                if response.status_code in [200, 201]:
                    log.info("OSCARServiceManager: Service creation API succeeded (status %d): %s", response.status_code, service_name)
                    self._service_cache[service_name] = service_def
                    return service_name
                else:
                    # Include response text in error message for better debugging
                    error_msg = f"HTTP {response.status_code}"
                    if response.text:
                        error_msg += f": {response.text}"
                    log.error("OSCARServiceManager: Failed to create service %s (status %d): %s", service_name, response.status_code, response.text)
                    log.error("OSCARServiceManager: Service creation failed with error: %s", error_msg)
                    
            except Exception as e:
                last_exception = e
                # Try to extract more details from the exception if it's an HTTP error
                error_details = str(e)
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    error_details += f" - Response: {e.response.text}"
                log.error("OSCARServiceManager: Error creating service %s (attempt %d/%d): %s", service_name, attempt, max_retries, error_details)
                
                # Check if service exists despite exception
                log.debug("OSCARServiceManager: Checking if service exists despite exception")
                created_service = check_service_exists(service_name)
                if created_service:
                    log.info("OSCARServiceManager: Service exists despite exception: %s", service_name)
                    self._service_cache[service_name] = service_def
                    return service_name
                
                # If this isn't the last attempt, wait before retrying
                if attempt < max_retries:
                    log.debug("OSCARServiceManager: Waiting %d seconds before retry", retry_delay)
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        
        # If we get here, all retries failed - raise an exception instead of falling back
        log.error("OSCARServiceManager: Failed to create service %s after %d retry attempts", service_name, max_retries)
        raise RuntimeError(f"Failed to create OSCAR service '{service_name}' after {max_retries} attempts. Last error: {last_exception}")


def make_oscar_tool(spec, loading_context, cluster_manager, mount_path, service_name, shared_minio_config=None):
    """cwl-oscar specific factory for CWL Process generation."""
    if "class" in spec and spec["class"] == "CommandLineTool":
        # Pass None as service_name since it will be determined dynamically
        return OSCARCommandLineTool(spec, loading_context, cluster_manager, mount_path, None, shared_minio_config)
    else:
        return default_make_tool(spec, loading_context)


class OSCARPathMapper(PathMapper):
    """Path mapper for OSCAR execution - maps local paths to mount paths."""
    
    def __init__(self, referenced_files, basedir, stagedir, separateDirs, mount_path=None, **kwargs):
        # Extract mount_path from kwargs if provided, or use default
        self.mount_path = mount_path or '/mnt/cwl2o-data/mount'
        super(OSCARPathMapper, self).__init__(referenced_files, basedir, stagedir, separateDirs, **kwargs)
        
    def setup(self, referenced_files, basedir):
        """Set up path mappings for OSCAR execution."""
        # Call parent setup first to handle the standard path mapping
        super(OSCARPathMapper, self).setup(referenced_files, basedir)
        
        # Apply OSCAR-specific path mappings
        # For files already in mount path, use direct mount path access instead of staging
        for key in list(self._pathmap.keys()):
            entry = self._pathmap[key]
            if hasattr(entry, 'resolved') and entry.resolved:
                resolved_path = entry.resolved
                
                # If file is already in the mount path, use it directly without staging
                if self.mount_path in resolved_path:
                    log.debug("File already in mount path, using direct access: %s", resolved_path)
                    # Use the mount path directly - no staging needed
                    self._pathmap[key] = MapperEnt(
                        resolved=resolved_path,
                        target=resolved_path,  # Use same path as target
                        type=entry.type,
                        staged=False  # Don't stage - file is already accessible
                    )
                    log.debug("Direct mount path mapping: %s -> %s", resolved_path, resolved_path)


class OSCARExecutor:
    """Modular executor interface for OSCAR command execution."""
    
    def __init__(self, oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, service_manager=None, ssl=True):
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.mount_path = mount_path
        self.service_manager = service_manager
        self.ssl = ssl
        self.client = None
        self.service_config = None
        
    def get_client(self):
        """Get or create OSCAR client."""
        if self.client is None:
            # Choose authentication method based on provided credentials
            if self.oscar_token:
                # Use OIDC token authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'oidc_token': self.oscar_token,
                    'ssl': str(self.ssl)
                }
                log.debug("Using OIDC token authentication for OSCAR client")
            elif self.oscar_username and self.oscar_password:
                # Use basic username/password authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'user': self.oscar_username,
                    'password': self.oscar_password,
                    'ssl': str(self.ssl)
                }
                log.debug("Using username/password authentication for OSCAR client")
            else:
                raise ValueError("Either OIDC token or username/password must be provided for OSCAR authentication")
            
            self.client = Client(options=options)
        return self.client
        
    def get_service_config(self):
        """Get service configuration from OSCAR."""
        if self.service_config is None:
            client = self.get_client()
            services = client.list_services()
            service_json = json.loads(services.text)
            
            # Find the target service
            for svc in service_json:
                if svc['name'] == self.service_name:
                    self.service_config = svc
                    break
            
            if not self.service_config:
                raise Exception(f"Service {self.service_name} not found")
                
        return self.service_config
        
    def create_command_script(self, command, environment, working_directory, stdout_file=None, output_dir=".", job_id=None):
        """Create a simplified script file with just the CWL command."""
        random_uuid = str(uuid.uuid4())
        job_id = job_id or random_uuid
        script_name = f"cwl_command_{job_id}.sh"
        script_path = os.path.join(output_dir, script_name)
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Create simple bash script content
        script_content = "#!/bin/bash\n\n"
        script_content += "# CWL Command Script Generated by cwl-oscar\n\n"
        
        # Set environment variables including CWL_JOB_ID
        script_content += "# Set environment variables\n"
        script_content += f'export CWL_JOB_ID="{job_id}"\n'
        
        for key, value in environment.items():
            # Escape quotes and handle special characters
            escaped_value = str(value).replace('"', '\\"').replace('$', '\\$')
            script_content += f'export {key}="{escaped_value}"\n'
        
        # Use TMP_OUTPUT_DIR as workspace (available by default in OSCAR execution)
        script_content += "\n# Use TMP_OUTPUT_DIR as workspace\n"
        script_content += "cd \"$TMP_OUTPUT_DIR\"\n"
        script_content += "echo \"Working in: $TMP_OUTPUT_DIR\"\n\n"
        
        # Execute the main command (cwltool handles input/output file management)
        script_content += "# Execute CWL command\n"
        
        # Quote command arguments properly
        quoted_command = [shlex.quote(arg) for arg in command]
        command_line = ' '.join(quoted_command)
        
        # Handle stdout redirection if specified
        if stdout_file:
            script_content += f"{command_line} > {shlex.quote(stdout_file)} 2>&1\n"
        else:
            script_content += f"{command_line}\n"
        
        # Store the exit code
        script_content += "exit_code=$?\n\n"
        
        # Copy all output from TMP_OUTPUT_DIR to the mount path
        script_content += "# Copy output files to mount path\n"
        script_content += "OUTPUT_DIR=\"$CWL_MOUNT_PATH/$CWL_JOB_ID\"\n"
        script_content += "mkdir -p \"$OUTPUT_DIR\"\n"
        script_content += "cp -r \"$TMP_OUTPUT_DIR\"/* \"$OUTPUT_DIR\"/ 2>/dev/null || true\n"
        script_content += "echo \"SCRIPT: Files copied to $OUTPUT_DIR\"\n"
        script_content += "echo \"SCRIPT: Command completed with exit code: $exit_code\"\n"
        script_content += "exit $exit_code\n"
        
        # Write the script to file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        log.debug("[job] Created command script: %s with command: %s", script_path, command_line)
        return script_path
        
    def upload_and_wait_for_output(self, local_file_path, timeout_seconds=300, check_interval=5):
        """Upload a file to OSCAR service and wait for the corresponding output file."""
        
        # Get service configuration
        service = self.get_service_config()
        storage_service = self.get_client().create_storage_client()
        
        # Extract service configuration
        in_provider = service['input'][0]['storage_provider']
        in_path = service['input'][0]['path']
        out_provider = service['output'][0]['storage_provider']
        out_path = service['output'][0]['path']
        
        file_name = os.path.basename(local_file_path)
        expected_output_name = file_name + '.exit_code'
        expected_output_path = out_path + "/" + expected_output_name
        
        log.info("Uploading %s to OSCAR service...", file_name)
        
        # Upload the input file
        try:
            with suppress_stdout_to_stderr():
                storage_service.upload_file(in_provider, local_file_path, in_path)
        except Exception as e:
            log.error("Upload failed: %s", e)
            return None
        
        log.info("Waiting for output file (max %ds)...", timeout_seconds)
        
        # Wait for the output file
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                with suppress_stdout_to_stderr():
                    files = storage_service.list_files_from_path(out_provider, expected_output_path)
                
                if 'Contents' in files:
                    for file_entry in files['Contents']:
                        if file_entry['Key'] == expected_output_path or file_entry['Key'].endswith(expected_output_name):
                            log.info("Output file found: %s (%s bytes)", file_entry['Key'], file_entry['Size'])
                            return file_entry
                
                time.sleep(check_interval)
                
            except Exception as e:
                log.debug("Error checking for output: %s", e)
                time.sleep(check_interval)
        
        log.error("Timeout: Output file not found after %d seconds", timeout_seconds)
        return None
        
    def download_output_file(self, remote_output_path, local_download_path):
        """Download an output file from OSCAR service."""
        
        try:
            # Get service configuration
            service = self.get_service_config()
            storage_service = self.get_client().create_storage_client()
            out_provider = service['output'][0]['storage_provider']
            service_out_path = service['output'][0]['path']
            
            # Extract filename and prepare paths
            filename = os.path.basename(remote_output_path)
            temp_dir = os.path.dirname(local_download_path)
            
            # Create directories
            os.makedirs(temp_dir, exist_ok=True)
            
            # Construct full remote path (service_out_path + filename)
            if service_out_path and remote_output_path.startswith('out/'):
                file_only = remote_output_path[4:]  # Remove 'out/' prefix
                full_remote_path = service_out_path + '/' + file_only
            else:
                full_remote_path = service_out_path + '/' + remote_output_path
            
            log.debug("Downloading %s...", filename)
            
            # Download using correct parameter order: provider, local_directory, remote_path
            with suppress_stdout_to_stderr():
                storage_service.download_file(out_provider, temp_dir, full_remote_path)
            
            # Find downloaded file
            possible_paths = [
                os.path.join(temp_dir, 'out', filename),  # remote structure recreated
                os.path.join(temp_dir, filename),  # filename in temp directory
            ]
            
            downloaded_file_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    downloaded_file_path = path
                    break
            
            if downloaded_file_path:
                # Move to desired location if needed
                if downloaded_file_path != local_download_path:
                    shutil.move(downloaded_file_path, local_download_path)
                
                log.debug("Download successful: %s", local_download_path)
                return True
            else:
                log.error("Downloaded file not found in expected locations")
                return False
                
        except Exception as e:
            log.error("Download failed: %s", e)
            return False

        
    def execute_command(self, command, environment, working_directory, job_name, tool_spec=None, stdout_file=None, job_id=None):
        """
        Execute a command using OSCAR service via file upload/download and return the exit code.
        
        Args:
            command: List of command and arguments to execute
            environment: Dictionary of environment variables
            working_directory: Directory to execute the command in
            job_name: Name of the job (for logging)
            tool_spec: CommandLineTool specification for dynamic service selection
            stdout_file: Optional stdout redirection file
            job_id: Optional job ID to use (if not provided, one will be generated)
            
        Returns:
            Exit code of the command (0 for success, non-zero for failure)
        """
        log.info("[job %s] Executing command via OSCAR: %s", job_name, " ".join(command))
        log.debug("[job %s] Working directory: %s", job_name, working_directory)
        log.debug("[job %s] Environment variables: %s", job_name, environment)
        
        # Determine service name dynamically
        if self.service_manager and tool_spec:
            log.debug("OSCARExecutor: [job %s] Using service manager to determine service for tool", job_name)
            service_name = self.service_manager.get_or_create_service(tool_spec, job_name)
            log.info("OSCARExecutor: [job %s] Service manager selected service: %s", job_name, service_name)
        else:
            # Fall back to default service
            service_name = "run-script-event2"
            log.warning("OSCARExecutor: [job %s] No service manager or tool spec, using default service: %s", job_name, service_name)
        
        # Temporarily set service_name for this execution
        original_service_name = getattr(self, 'service_name', None)
        self.service_name = service_name
        
        script_path = None
        output_path = None
        
        try:
            # Create a temporary directory for scripts
            temp_dir = tempfile.mkdtemp(prefix="cwl_oscar_")
            
            # Use provided job_id or create command script file with job-specific ID
            if job_id is None:
                job_id = f"{job_name}_{int(time.time())}"
            
            log.debug("[job %s] Using job_id: %s", job_name, job_id)
            script_path = self.create_command_script(
                command, environment, working_directory, stdout_file=stdout_file, output_dir=temp_dir, job_id=job_id
            )
            
            # Upload script and wait for output
            log.info("[job %s] Submitting job to OSCAR service: %s", job_name, self.service_name)
            output_file = self.upload_and_wait_for_output(script_path)
            
            if output_file is None:
                log.error("[job %s] Failed to get output file from OSCAR", job_name)
                return 1
            
            # Download the output file
            output_filename = os.path.basename(script_path) + '.output'
            output_path = os.path.join(temp_dir, output_filename)
            
            success = self.download_output_file(output_file['Key'], output_path)
            if not success:
                log.error("[job %s] Failed to download output file", job_name)
                return 1
            
            # Read the exit code from the output file
            try:
                with open(output_path, 'r') as f:
                    output_content = f.read().strip()
                
                log.debug("[job %s] Exit code file content: '%s' (length: %d)", job_name, repr(output_content), len(output_content))
                log.debug("[job %s] Exit code isdigit(): %s", job_name, output_content.isdigit())
                
                # The output file should contain the exit code
                # For OSCAR script execution, it typically contains the exit code
                if output_content.isdigit():
                    exit_code = int(output_content)
                    log.debug("[job %s] Parsed exit code as integer: %d", job_name, exit_code)
                else:
                    log.warning("[job %s] Exit code content is not a digit, defaulting to 0. Content: '%s'", job_name, repr(output_content))
                    exit_code = 0
                
                if exit_code == 0:
                    log.info("[job %s] OSCAR job completed successfully", job_name)
                else:
                    log.warning("[job %s] OSCAR job completed with exit code %d", job_name, exit_code)
                
                # Note: All outputs (including stdout) are now available in mount_path/job_id
                # No need to download them separately as they're copied by the script
                log.info("[job %s] All outputs have been copied to mount path by the script", job_name)
                
                return exit_code
                
            except (ValueError, IOError) as e:
                log.warning("[job %s] Could not parse exit code from output: %s, assuming success", job_name, e)
                return 0
                
        except Exception as e:
            log.error("[job %s] Error executing command via OSCAR: %s", job_name, str(e))
            return 1
            
        finally:
            # Restore original service_name
            if original_service_name:
                self.service_name = original_service_name
            # Clean up temporary files
            try:
                if script_path and os.path.exists(script_path):
                    os.remove(script_path)
                if output_path and os.path.exists(output_path):
                    os.remove(output_path)
                # Clean up temp directory
                temp_dir_path = os.path.dirname(script_path) if script_path else None
                if temp_dir_path and os.path.exists(temp_dir_path):
                    shutil.rmtree(temp_dir_path, ignore_errors=True)
            except Exception as e:
                log.debug("[job %s] Error cleaning up temporary files: %s", job_name, e)


class OSCARTask(JobBase):
    """OSCAR-specific task implementation."""
    
    def __init__(self, builder, joborder, make_path_mapper, requirements, hints, name,
                 cluster_manager, mount_path, service_name, runtime_context,
                 tool_spec=None, shared_minio_config=None):
        super(OSCARTask, self).__init__(builder, joborder, make_path_mapper, requirements, hints, name)
        self.cluster_manager = cluster_manager
        self.mount_path = mount_path
        self.service_name = service_name
        self.runtime_context = runtime_context
        self.tool_spec = tool_spec # Store tool specification
        self.shared_minio_config = shared_minio_config
        
        # We'll create executors dynamically for each cluster as needed
        
    def run(self, runtimeContext, tmpdir_lock=None):
        """Execute the job using OSCAR with run-specific workspace."""
        try:
            log.info("[job %s] Starting OSCAR execution", self.name)
            
            # Generate job ID for this run
            job_id = f"{self.name}_{int(time.time())}"
            log.debug("[job %s] Generated job_id: %s", self.name, job_id)
            
            # Build the command line
            cmd = self.build_command_line()
            
            # Set up environment 
            env = self.build_environment()
            
            # Set working directory - the command script will create its own run-specific directory
            workdir = self.mount_path
            
            # Get the next available cluster using round-robin scheduling
            cluster_config = self.cluster_manager.get_next_cluster()
            if not cluster_config:
                raise RuntimeError("No available clusters for task execution")
            
            log.info("[job %s] Executing on cluster: %s", self.name, cluster_config.name)
            
            # Create service manager and executor for this specific cluster
            service_manager = OSCARServiceManager(
                cluster_config.endpoint,
                cluster_config.token,
                cluster_config.username,
                cluster_config.password,
                self.mount_path,
                cluster_config.ssl,
                self.shared_minio_config
            )
            
            executor = OSCARExecutor(
                cluster_config.endpoint,
                cluster_config.token,
                cluster_config.username,
                cluster_config.password,
                self.mount_path,
                service_manager,
                cluster_config.ssl
            )
            
            # Execute the command using OSCAR
            stdout_file = getattr(self, 'stdout', None)
            
            exit_code = executor.execute_command(
                command=cmd,
                environment=env,
                working_directory=workdir,
                job_name=self.name,
                tool_spec=self.tool_spec,  # Pass tool specification for dynamic service selection
                stdout_file=stdout_file,
                job_id=job_id  # Pass the job_id to ensure consistency
            )
            
            # Determine process status
            if exit_code == 0:
                log.info("[job %s] completed successfully", self.name)
                process_status = "success"
            else:
                log.error("[job %s] failed with exit code %d", self.name, exit_code)
                process_status = "permanentFail"
            
            # Collect outputs from the mount path where they were copied
            try:
                # Outputs are now copied to mount_path/job_id by the script
                output_dir = os.path.join(self.mount_path, job_id)
                log.info("[job %s] Looking for outputs in: %s (job_id: %s)", self.name, output_dir, job_id)
                
                # Check if output directory exists
                if os.path.exists(output_dir):
                    # Update builder's outdir to point to the correct location
                    original_outdir = self.builder.outdir
                    self.builder.outdir = output_dir
                    
                    # Use cwltool's standard output collection from the mount path
                    outputs = self.collect_outputs(output_dir, exit_code)
                    self.outputs = outputs
                    
                    # Restore original outdir
                    self.builder.outdir = original_outdir
                    
                    log.info("[job %s] Collected outputs: %s", self.name, outputs)
                else:
                    log.warning("[job %s] Output directory not found: %s", self.name, output_dir)
                    self.outputs = {}
                    process_status = "permanentFail"
                
            except Exception as e:
                log.error("[job %s] Error collecting outputs: %s", self.name, e)
                self.outputs = {}
                process_status = "permanentFail"
                
        except Exception as err:
            log.error("[job %s] job error:\n%s", self.name, err)
            if log.isEnabledFor(logging.DEBUG):
                log.exception(err)
            process_status = "permanentFail"
            self.outputs = {}
        
        finally:
            # Ensure outputs is set
            if self.outputs is None:
                self.outputs = {}
            
            # Notify cwltool about completion using the callback pattern
            with self.runtime_context.workflow_eval_lock:
                self.output_callback(self.outputs, process_status)
            
            log.info("[job %s] OUTPUTS: %s", self.name, self.outputs)
            
            # Don't return a status - let cwltool handle cleanup
            return
            
    def build_command_line(self):
        """Build the command line to execute."""
        # The command line is already built and available as self.command_line
        # from the parent JobBase class
        log.debug("[job %s] Command line: %s", self.name, self.command_line)
        return self.command_line
        
    def build_environment(self):
        """Build environment variables for the job - only CWL-specific variables from the CWL specification."""
        env = {}
        
        # Add CWL-specific environment variables needed by cwl-oscar
        env["CWL_JOB_NAME"] = self.name
        env["CWL_MOUNT_PATH"] = self.mount_path
        
        # Add environment variables from CWL EnvVarRequirement if present
        cwl_env_vars = self._get_cwl_environment_variables()
        if cwl_env_vars:
            env.update(cwl_env_vars)
            
        # Add any additional environment variables from cwltool (if provided by the job)
        if hasattr(self, 'environment'):
            env.update(self.environment)
            
        return env
    
    def _get_cwl_environment_variables(self):
        """Extract environment variables defined in CWL EnvVarRequirement."""
        env_vars = {}
        
        # Check if the tool has requirements with EnvVarRequirement
        if hasattr(self, 'requirements') and self.requirements:
            for req in self.requirements:
                if req.get('class') == 'EnvVarRequirement':
                    env_def = req.get('envDef', {})
                    for var_name, var_value in env_def.items():
                        env_vars[var_name] = var_value
        
        # Also check hints for environment variables
        if hasattr(self, 'hints') and self.hints:
            for hint in self.hints:
                if hint.get('class') == 'EnvVarRequirement':
                    env_def = hint.get('envDef', {})
                    for var_name, var_value in env_def.items():
                        env_vars[var_name] = var_value
        
        return env_vars
        
    def _required_env(self):
        """Return environment variables required for the job (abstract method from JobBase)."""
        return {}
        
    def _preserve_environment(self, env):
        """Preserve environment variables (abstract method from JobBase)."""
        return env


class OSCARCommandLineTool(CommandLineTool):
    """OSCAR-specific CommandLineTool implementation."""
    
    def __init__(self, toolpath_object, loading_context, cluster_manager, mount_path, service_name, shared_minio_config=None):
        super(OSCARCommandLineTool, self).__init__(toolpath_object, loading_context)
        self.cluster_manager = cluster_manager
        self.mount_path = mount_path
        self.service_name = service_name
        self.shared_minio_config = shared_minio_config
        
        # We'll create service managers dynamically for each cluster as needed
        
    def make_path_mapper(self, reffiles, stagedir, runtimeContext, separateDirs):
        """Create a path mapper for OSCAR execution."""
        return OSCARPathMapper(
            reffiles, runtimeContext.basedir, stagedir, separateDirs, mount_path=self.mount_path)
            
    def make_job_runner(self, runtimeContext):
        """Create an OSCAR job runner."""
        def create_oscar_task(builder, joborder, make_path_mapper, requirements, hints, name):
            return OSCARTask(
                builder,
                joborder,
                make_path_mapper,
                requirements,
                hints,
                name,
                self.cluster_manager,
                self.mount_path,
                self.service_name,
                runtimeContext,
                tool_spec=self.tool,  # Pass tool specification
                shared_minio_config=self.shared_minio_config
            )
        return create_oscar_task 
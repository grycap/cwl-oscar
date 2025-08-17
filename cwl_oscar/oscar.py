"""OSCAR-specific implementation for CWL execution."""
from __future__ import absolute_import, print_function, unicode_literals

# Standard library imports
import contextlib
import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
import uuid
from typing import Any, Dict, List, MutableMapping, MutableSequence, Optional, Union

# Third-party imports
from cwltool.builder import Builder
from cwltool.command_line_tool import CommandLineTool
from cwltool.context import RuntimeContext
from cwltool.errors import WorkflowException
from cwltool.job import JobBase
from cwltool.pathmapper import PathMapper, MapperEnt
from cwltool.workflow import default_make_tool

# Local imports
try:
    from .constants import *
    from .scripts.oscar_service_script import OSCAR_SERVICE_SCRIPT_TEMPLATE
    from .utils import create_oscar_client, sanitize_service_name
except ImportError:
    # Fallback for standalone execution
    from constants import *
    from scripts.oscar_service_script import OSCAR_SERVICE_SCRIPT_TEMPLATE
    from utils import create_oscar_client, sanitize_service_name

# Import OSCAR Python client
try:
    from oscar_python.client import Client
except ImportError:
    raise ImportError("oscar-python package is required. Install with: pip install oscar-python")

log = logging.getLogger("oscar-backend")


@contextlib.contextmanager
def suppress_stdout_to_stderr():
    """Context manager to redirect stdout to stderr during oscar-python operations.
    
    This prevents oscar-python library messages from contaminating the JSON output.
    """
    original_stdout = sys.stdout
    try:
        sys.stdout = sys.stderr
        yield
    finally:
        sys.stdout = original_stdout


class OSCARServiceManager:
    """Manages dynamic OSCAR service creation based on CommandLineTool requirements."""
    
    def __init__(self, oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, ssl=True, shared_minio_config=None):
        log.debug("%s: Initializing service manager", LOG_PREFIX_SERVICE_MANAGER)
        log.debug("%s: OSCAR endpoint: %s", LOG_PREFIX_SERVICE_MANAGER, oscar_endpoint)
        log.debug("%s: Mount path: %s", LOG_PREFIX_SERVICE_MANAGER, mount_path)
        log.debug("%s: Using token auth: %s", LOG_PREFIX_SERVICE_MANAGER, bool(oscar_token))
        log.debug("%s: Using username/password auth: %s", LOG_PREFIX_SERVICE_MANAGER, bool(oscar_username and oscar_password))
        
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.mount_path = mount_path
        self.ssl = ssl
        self.client = None
        self._service_cache = {}  # Cache created services
        self.shared_minio_config = shared_minio_config
        
        log.debug("%s: Service manager initialized successfully", LOG_PREFIX_SERVICE_MANAGER)
        
    def get_client(self):
        """Get or create OSCAR client."""
        if self.client is None:
            log.debug("%s: Creating new OSCAR client", LOG_PREFIX_SERVICE_MANAGER)
            self.client = create_oscar_client(
                self.oscar_endpoint,
                self.oscar_token,
                self.oscar_username,
                self.oscar_password,
                self.ssl
            )
        else:
            log.debug("%s: Reusing existing OSCAR client", LOG_PREFIX_SERVICE_MANAGER)
            
        return self.client
        
    def _extract_docker_requirements(self, tool_spec, requirements):
        """Extract Docker requirements from tool specification."""
        if 'requirements' in tool_spec:
            for req in tool_spec['requirements']:
                if req.get('class') == 'DockerRequirement' and 'dockerPull' in req:
                    old_image = requirements['image']
                    requirements['image'] = req['dockerPull']
                    log.debug("%s: Updated Docker image from '%s' to '%s'", 
                             LOG_PREFIX_SERVICE_MANAGER, old_image, requirements['image'])
        
        # Check hints as well
        if 'hints' in tool_spec:
            for hint in tool_spec['hints']:
                if hint.get('class') == 'DockerRequirement' and 'dockerPull' in hint:
                    old_image = requirements['image']
                    requirements['image'] = hint['dockerPull']
                    log.debug("%s: Updated Docker image from hint: '%s' to '%s'", 
                             LOG_PREFIX_SERVICE_MANAGER, old_image, requirements['image'])
    
    def _extract_resource_requirements(self, tool_spec, requirements):
        """Extract resource requirements from tool specification."""
        if 'requirements' in tool_spec:
            for req in tool_spec['requirements']:
                if req.get('class') == 'ResourceRequirement':
                    if 'ramMin' in req:
                        ram_mb = req['ramMin']
                        old_memory = requirements['memory']
                        requirements['memory'] = f"{ram_mb}Mi"
                        log.debug("%s: Updated memory from '%s' to '%s'", 
                                 LOG_PREFIX_SERVICE_MANAGER, old_memory, requirements['memory'])
                    if 'coresMin' in req:
                        old_cpu = requirements['cpu']
                        requirements['cpu'] = str(req['coresMin'])
                        log.debug("%s: Updated CPU from '%s' to '%s'", 
                                 LOG_PREFIX_SERVICE_MANAGER, old_cpu, requirements['cpu'])
    
    def _extract_environment_requirements(self, tool_spec, requirements):
        """Extract environment variable requirements from tool specification."""
        if 'requirements' in tool_spec:
            for req in tool_spec['requirements']:
                if req.get('class') == 'EnvVarRequirement' and 'envDef' in req:
                    # envDef is a dictionary in CWL spec
                    if isinstance(req['envDef'], dict):
                        log.debug("%s: Adding %d environment variables", 
                                 LOG_PREFIX_SERVICE_MANAGER, len(req['envDef']))
                        requirements['environment'].update(req['envDef'])
                    else:
                        # Handle legacy format if it's a list
                        log.debug("%s: Processing legacy envDef list format", LOG_PREFIX_SERVICE_MANAGER)
                        for env_def in req['envDef']:
                            if isinstance(env_def, dict):
                                requirements['environment'][env_def['envName']] = env_def['envValue']
                                log.debug("%s: Added env var: %s=%s", 
                                         LOG_PREFIX_SERVICE_MANAGER, env_def['envName'], env_def['envValue'])
        
        # Also check hints for environment variables
        if 'hints' in tool_spec:
            for hint in tool_spec['hints']:
                if hint.get('class') == 'EnvVarRequirement' and 'envDef' in hint:
                    env_def = hint.get('envDef', {})
                    for var_name, var_value in env_def.items():
                        requirements['environment'][var_name] = var_value
        
    def extract_service_requirements(self, tool_spec):
        """Extract service requirements from CommandLineTool specification."""
        log.debug("%s: Extracting service requirements from tool spec", LOG_PREFIX_SERVICE_MANAGER)
        log.debug("%s: Tool ID: %s", LOG_PREFIX_SERVICE_MANAGER, tool_spec.get('id', 'unknown'))
        log.debug("%s: Tool baseCommand: %s", LOG_PREFIX_SERVICE_MANAGER, tool_spec.get('baseCommand', 'unknown'))
        
        requirements = {
            'memory': DEFAULT_MEMORY,
            'cpu': DEFAULT_CPU,
            'image': DEFAULT_DOCKER_IMAGE,
            'environment': {}
        }
        log.debug("%s: Default requirements: %s", LOG_PREFIX_SERVICE_MANAGER, requirements)
        
        # Extract different types of requirements
        self._extract_docker_requirements(tool_spec, requirements)
        self._extract_resource_requirements(tool_spec, requirements)
        self._extract_environment_requirements(tool_spec, requirements)
        
        log.debug("%s: Final extracted requirements: %s", LOG_PREFIX_SERVICE_MANAGER, requirements)
        return requirements
        
    def generate_service_name(self, tool_spec, requirements, job_name=None):
        """Generate a unique service name based on tool and requirements."""
        log.debug("%s: Generating service name for tool", LOG_PREFIX_SERVICE_MANAGER)
        
        # Use job_name if provided, otherwise use "tool"
        if job_name:
            tool_id = job_name
            log.debug("%s: Using provided job_name as tool ID: '%s'", LOG_PREFIX_SERVICE_MANAGER, tool_id)
        else:
            tool_id = "tool"
            log.debug("%s: No job_name provided, using default tool ID: '%s'", LOG_PREFIX_SERVICE_MANAGER, tool_id)
            
        # Create a hash based on tool content and requirements
        tool_content = json.dumps({
            'baseCommand': tool_spec.get('baseCommand'),
            'class': tool_spec.get('class'),
            'requirements': requirements
        }, sort_keys=True)
        log.debug("%s: Tool content for hashing: %s", LOG_PREFIX_SERVICE_MANAGER, tool_content)
        
        service_hash = hashlib.md5(tool_content.encode()).hexdigest()[:SERVICE_HASH_LENGTH]
        log.debug("%s: Generated service hash: %s", LOG_PREFIX_SERVICE_MANAGER, service_hash)
        
        # Use tool_id directly without cleaning
        if not tool_id:
            tool_id = 'tool'
            log.debug("%s: Empty tool ID, using default: '%s'", LOG_PREFIX_SERVICE_MANAGER, tool_id)
        
        # Sanitize the tool ID for Kubernetes naming rules
        clean_tool_id = sanitize_service_name(tool_id)
        
        final_service_name = f"{SERVICE_NAME_PREFIX}{clean_tool_id}-{service_hash}"
        log.debug("%s: Final generated service name: '%s'", LOG_PREFIX_SERVICE_MANAGER, final_service_name)
        return final_service_name
        

        
    def create_service_definition(self, service_name, requirements, mount_path, shared_minio_config=None):
        """Create OSCAR service definition."""
        log.debug("%s: Creating service definition for service: %s", LOG_PREFIX_SERVICE_MANAGER, service_name)
        log.debug("%s: Requirements: %s", LOG_PREFIX_SERVICE_MANAGER, requirements)
        log.debug("%s: Mount path: %s", LOG_PREFIX_SERVICE_MANAGER, mount_path)
        
        # Extract mount path components for the mount configuration
        mount_parts = mount_path.strip('/').split('/')
        log.debug("%s: Mount path parts: %s", LOG_PREFIX_SERVICE_MANAGER, mount_parts)
        
        # Remove 'mnt' prefix if present to get the actual mount path
        if mount_parts[0] == 'mnt':
            mount_base = '/'.join(mount_parts[1:])
            log.debug("%s: Removed 'mnt' prefix, mount base: %s", LOG_PREFIX_SERVICE_MANAGER, mount_base)
        else:
            mount_base = '/'.join(mount_parts)
            log.debug("%s: No 'mnt' prefix, mount base: %s", LOG_PREFIX_SERVICE_MANAGER, mount_base)
        
        log.debug("%s: Using script template (%d characters)", LOG_PREFIX_SERVICE_MANAGER, len(OSCAR_SERVICE_SCRIPT_TEMPLATE))
        
        service_def = {
            'name': service_name,
            'memory': requirements['memory'],
            'cpu': requirements['cpu'],
            'image': requirements['image'],
            'script': OSCAR_SERVICE_SCRIPT_TEMPLATE,
            'environment': {
                'variables': {
                    'MOUNT_PATH': mount_path,
                    **requirements['environment']
                }
            },
            'input': [{
                'storage_provider': DEFAULT_STORAGE_PROVIDER,
                'path': f'{service_name}/in'
            }],
            'output': [{
                'storage_provider': DEFAULT_STORAGE_PROVIDER, 
                'path': f'{service_name}/out'
            }],
            'mount': {
                'storage_provider': DEFAULT_STORAGE_PROVIDER,
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
                        "region": shared_minio_config.get("region") or DEFAULT_REGION
                    }
                }
            }
            
            # Update only mount to use shared MinIO, keep input/output as minio.default
            service_def["mount"]["storage_provider"] = SHARED_STORAGE_PROVIDER
        
        log.debug("%s: Created service definition: %s", LOG_PREFIX_SERVICE_MANAGER, json.dumps(service_def, indent=2))
        return service_def
        
    def _check_service_exists(self, client, name):
        """Check if a service exists on the OSCAR cluster."""
        log.debug("%s: Checking if service '%s' exists on OSCAR cluster", LOG_PREFIX_SERVICE_MANAGER, name)
        try:
            services_response = client.list_services()
            log.debug("%s: List services response status: %d", LOG_PREFIX_SERVICE_MANAGER, services_response.status_code)
            
            if services_response.status_code == 200:
                existing_services = json.loads(services_response.text)
                log.debug("%s: Found %d existing services on cluster", LOG_PREFIX_SERVICE_MANAGER, len(existing_services))
                
                for service in existing_services:
                    service_name_in_list = service.get('name')
                    log.debug("%s: Checking service: %s", LOG_PREFIX_SERVICE_MANAGER, service_name_in_list)
                    if service_name_in_list == name:
                        log.info("%s: Service already exists on cluster: %s", LOG_PREFIX_SERVICE_MANAGER, name)
                        self._service_cache[name] = service
                        return service
                
                log.debug("%s: Service '%s' not found among existing services", LOG_PREFIX_SERVICE_MANAGER, name)
            else:
                log.warning("%s: Failed to list services, status code: %d", LOG_PREFIX_SERVICE_MANAGER, services_response.status_code)
                
        except Exception as e:
            log.warning("%s: Could not check existing services: %s", LOG_PREFIX_SERVICE_MANAGER, e)
        return None
    
    def _create_service_with_retry(self, client, service_name, service_def):
        """Create service with retry logic."""
        max_retries = DEFAULT_MAX_RETRIES
        retry_delay = DEFAULT_RETRY_DELAY
        last_exception = None
        
        for attempt in range(1, max_retries + 1):
            log.info("%s: Attempt %d/%d to create service %s", LOG_PREFIX_SERVICE_MANAGER, attempt, max_retries, service_name)
            
            try:
                # Create service using OSCAR API
                log.debug("%s: Sending service creation request to OSCAR API", LOG_PREFIX_SERVICE_MANAGER)
                log.debug("%s: Complete service definition to create: %s", LOG_PREFIX_SERVICE_MANAGER, json.dumps(service_def, indent=2))
                
                response = client.create_service(service_def)
                log.debug("%s: Service creation response status: %d", LOG_PREFIX_SERVICE_MANAGER, response.status_code)
                log.debug("%s: Service creation response text: %s", LOG_PREFIX_SERVICE_MANAGER, response.text)
                
                # Wait for service setup to complete
                log.debug("%s: Waiting %d seconds for service setup to complete", LOG_PREFIX_SERVICE_MANAGER, DEFAULT_SERVICE_SETUP_WAIT)
                time.sleep(DEFAULT_SERVICE_SETUP_WAIT)
                
                # Always check if service was created, regardless of API response
                log.debug("%s: Verifying service creation by checking if service exists", LOG_PREFIX_SERVICE_MANAGER)
                created_service = self._check_service_exists(client, service_name)
                if created_service:
                    log.info("%s: Service successfully created and verified: %s", LOG_PREFIX_SERVICE_MANAGER, service_name)
                    self._service_cache[service_name] = service_def
                    return service_name
                
                if response.status_code in [200, 201]:
                    log.info("%s: Service creation API succeeded (status %d): %s", LOG_PREFIX_SERVICE_MANAGER, response.status_code, service_name)
                    self._service_cache[service_name] = service_def
                    return service_name
                else:
                    # Include response text in error message for better debugging
                    error_msg = f"HTTP {response.status_code}"
                    if response.text:
                        error_msg += f": {response.text}"
                    log.error("%s: Failed to create service %s (status %d): %s", LOG_PREFIX_SERVICE_MANAGER, service_name, response.status_code, response.text)
                    log.error("%s: Service creation failed with error: %s", LOG_PREFIX_SERVICE_MANAGER, error_msg)
                    
            except Exception as e:
                last_exception = e
                # Try to extract more details from the exception if it's an HTTP error
                error_details = str(e)
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    error_details += f" - Response: {e.response.text}"
                log.error("%s: Error creating service %s (attempt %d/%d): %s", LOG_PREFIX_SERVICE_MANAGER, service_name, attempt, max_retries, error_details)
                
                # Check if service exists despite exception
                log.debug("%s: Checking if service exists despite exception", LOG_PREFIX_SERVICE_MANAGER)
                created_service = self._check_service_exists(client, service_name)
                if created_service:
                    log.info("%s: Service exists despite exception: %s", LOG_PREFIX_SERVICE_MANAGER, service_name)
                    self._service_cache[service_name] = service_def
                    return service_name
                
                # If this isn't the last attempt, wait before retrying
                if attempt < max_retries:
                    log.debug("%s: Waiting %d seconds before retry", LOG_PREFIX_SERVICE_MANAGER, retry_delay)
                    time.sleep(retry_delay)
                    retry_delay *= DEFAULT_RETRY_MULTIPLIER  # Exponential backoff
        
        # If we get here, all retries failed - raise an exception
        log.error("%s: Failed to create service %s after %d retry attempts", LOG_PREFIX_SERVICE_MANAGER, service_name, max_retries)
        raise RuntimeError(f"Failed to create OSCAR service '{service_name}' after {max_retries} attempts. Last error: {last_exception}")
        
    def get_or_create_service(self, tool_spec, job_name=None):
        """Get existing service or create new one for the CommandLineTool."""
        log.debug("%s: Starting get_or_create_service for tool: %s", LOG_PREFIX_SERVICE_MANAGER, tool_spec.get('id', 'unknown'))
        
        requirements = self.extract_service_requirements(tool_spec)
        service_name = self.generate_service_name(tool_spec, requirements, job_name)
        
        log.info("%s: Generated service name '%s' for tool '%s'", LOG_PREFIX_SERVICE_MANAGER, service_name, tool_spec.get('id', 'unknown'))
        
        # Check cache first
        if service_name in self._service_cache:
            log.debug("%s: Using cached service: %s", LOG_PREFIX_SERVICE_MANAGER, service_name)
            log.info("%s: Service '%s' found in cache, reusing existing service", LOG_PREFIX_SERVICE_MANAGER, service_name)
            return service_name
            
        log.debug("%s: Service not in cache, checking OSCAR cluster", LOG_PREFIX_SERVICE_MANAGER)
        client = self.get_client()
        
        # First check if service exists
        existing_service = self._check_service_exists(client, service_name)
        if existing_service:
            log.info("%s: Using existing service: %s", LOG_PREFIX_SERVICE_MANAGER, service_name)
            return service_name
            
        # Create new service
        log.info("%s: Creating new service for tool: %s -> %s", LOG_PREFIX_SERVICE_MANAGER, tool_spec.get('id', 'unknown'), service_name)
        service_def = self.create_service_definition(service_name, requirements, self.mount_path, self.shared_minio_config)
        
        return self._create_service_with_retry(client, service_name, service_def)


class OSCARPathMapper(PathMapper):
    """Path mapper for OSCAR execution - maps local paths to mount paths."""
    
    def __init__(self, referenced_files, basedir, stagedir, separateDirs, mount_path=None, **kwargs):
        # Extract mount_path from kwargs if provided, or use default
        self.mount_path = mount_path or DEFAULT_MOUNT_PATH
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
            self.client = create_oscar_client(
                self.oscar_endpoint,
                self.oscar_token,
                self.oscar_username,
                self.oscar_password,
                self.ssl
            )
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
        
    def upload_and_wait_for_output(self, local_file_path, timeout_seconds=DEFAULT_UPLOAD_TIMEOUT, check_interval=DEFAULT_CHECK_INTERVAL):
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
        expected_output_name = file_name + EXIT_CODE_EXTENSION
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
        log.info(LOG_PREFIX_JOB + " Executing command via OSCAR: %s", job_name, " ".join(command))
        log.debug(LOG_PREFIX_JOB + " Working directory: %s", job_name, working_directory)
        log.debug(LOG_PREFIX_JOB + " Environment variables: %s", job_name, environment)
        
        # Determine service name dynamically
        if self.service_manager and tool_spec:
            log.debug("%s: " + LOG_PREFIX_JOB + " Using service manager to determine service for tool", LOG_PREFIX_EXECUTOR, job_name)
            service_name = self.service_manager.get_or_create_service(tool_spec, job_name)
            log.info("%s: " + LOG_PREFIX_JOB + " Service manager selected service: %s", LOG_PREFIX_EXECUTOR, job_name, service_name)
        else:
            # Fall back to default service
            service_name = "run-script-event2"
            log.warning("%s: " + LOG_PREFIX_JOB + " No service manager or tool spec, using default service: %s", LOG_PREFIX_EXECUTOR, job_name, service_name)
        
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
            
            log.debug(LOG_PREFIX_JOB + " Using job_id: %s", job_name, job_id)
            script_path = self.create_command_script(
                command, environment, working_directory, stdout_file=stdout_file, output_dir=temp_dir, job_id=job_id
            )
            
            # Upload script and wait for output
            log.info(LOG_PREFIX_JOB + " Submitting job to OSCAR service: %s", job_name, self.service_name)
            output_file = self.upload_and_wait_for_output(script_path)
            
            if output_file is None:
                log.error(LOG_PREFIX_JOB + " Failed to get output file from OSCAR", job_name)
                return 1
            
            # Download the output file
            output_filename = os.path.basename(script_path) + OUTPUT_EXTENSION
            output_path = os.path.join(temp_dir, output_filename)
            
            success = self.download_output_file(output_file['Key'], output_path)
            if not success:
                log.error(LOG_PREFIX_JOB + " Failed to download output file", job_name)
                return 1
            
            # Read the exit code from the output file
            try:
                with open(output_path, 'r') as f:
                    output_content = f.read().strip()
                
                log.debug(LOG_PREFIX_JOB + " Exit code file content: '%s' (length: %d)", job_name, repr(output_content), len(output_content))
                log.debug(LOG_PREFIX_JOB + " Exit code isdigit(): %s", job_name, output_content.isdigit())
                
                # The output file should contain the exit code
                # For OSCAR script execution, it typically contains the exit code
                if output_content.isdigit():
                    exit_code = int(output_content)
                    log.debug(LOG_PREFIX_JOB + " Parsed exit code as integer: %d", job_name, exit_code)
                else:
                    log.warning(LOG_PREFIX_JOB + " Exit code content is not a digit, defaulting to 0. Content: '%s'", job_name, repr(output_content))
                    exit_code = 0
                
                if exit_code == 0:
                    log.info(LOG_PREFIX_JOB + " OSCAR job completed successfully", job_name)
                else:
                    log.warning(LOG_PREFIX_JOB + " OSCAR job completed with exit code %d", job_name, exit_code)
                
                # Note: All outputs (including stdout) are now available in mount_path/job_id
                # No need to download them separately as they're copied by the script
                log.info(LOG_PREFIX_JOB + " All outputs have been copied to mount path by the script", job_name)
                
                return exit_code
                
            except (ValueError, IOError) as e:
                log.warning(LOG_PREFIX_JOB + " Could not parse exit code from output: %s, assuming success", job_name, e)
                return 0
                
        except Exception as e:
            log.error(LOG_PREFIX_JOB + " Error executing command via OSCAR: %s", job_name, str(e))
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
                log.debug(LOG_PREFIX_JOB + " Error cleaning up temporary files: %s", job_name, e)


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
            log.info(LOG_PREFIX_JOB + " Starting OSCAR execution", self.name)
            
            # Generate job ID for this run
            job_id = f"{self.name}_{int(time.time())}"
            log.debug(LOG_PREFIX_JOB + " Generated job_id: %s", self.name, job_id)
            
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
            
            log.info(LOG_PREFIX_JOB + " Executing on cluster: %s", self.name, cluster_config.name)
            
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
                log.info(LOG_PREFIX_JOB + " completed successfully", self.name)
                process_status = "success"
            else:
                log.error(LOG_PREFIX_JOB + " failed with exit code %d", self.name, exit_code)
                process_status = "permanentFail"
            
            # Collect outputs from the mount path where they were copied
            try:
                # Outputs are now copied to mount_path/job_id by the script
                output_dir = os.path.join(self.mount_path, job_id)
                log.info(LOG_PREFIX_JOB + " Looking for outputs in: %s (job_id: %s)", self.name, output_dir, job_id)
                
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
                    
                    log.info(LOG_PREFIX_JOB + " Collected outputs: %s", self.name, outputs)
                else:
                    log.warning(LOG_PREFIX_JOB + " Output directory not found: %s", self.name, output_dir)
                    self.outputs = {}
                    process_status = "permanentFail"
                
            except Exception as e:
                log.error(LOG_PREFIX_JOB + " Error collecting outputs: %s", self.name, e)
                self.outputs = {}
                process_status = "permanentFail"
                
        except Exception as err:
            log.error(LOG_PREFIX_JOB + " job error:\n%s", self.name, err)
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
            
            log.info(LOG_PREFIX_JOB + " OUTPUTS: %s", self.name, self.outputs)
            
            # Don't return a status - let cwltool handle cleanup
            return
            
    def build_command_line(self):
        """Build the command line to execute."""
        # The command line is already built and available as self.command_line
        # from the parent JobBase class
        log.debug(LOG_PREFIX_JOB + " Command line: %s", self.name, self.command_line)
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


def make_oscar_tool(spec, loading_context, cluster_manager, mount_path, service_name, shared_minio_config=None):
    """cwl-oscar specific factory for CWL Process generation."""
    if "class" in spec and spec["class"] == "CommandLineTool":
        # Pass None as service_name since it will be determined dynamically
        return OSCARCommandLineTool(spec, loading_context, cluster_manager, mount_path, None, shared_minio_config)
    else:
        return default_make_tool(spec, loading_context) 
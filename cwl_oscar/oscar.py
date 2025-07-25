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

log = logging.getLogger("oscar-backend")


def make_oscar_tool(spec, loading_context, oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, service_name):
    """cwl-oscar specific factory for CWL Process generation."""
    if "class" in spec and spec["class"] == "CommandLineTool":
        return OSCARCommandLineTool(spec, loading_context, oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, service_name)
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
    
    def __init__(self, oscar_endpoint, oscar_token, oscar_username, oscar_password, service_name):
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.service_name = service_name
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
                    'ssl': 'True'
                }
                log.debug("Using OIDC token authentication for OSCAR client")
            elif self.oscar_username and self.oscar_password:
                # Use basic username/password authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'user': self.oscar_username,
                    'password': self.oscar_password,
                    'ssl': 'True'
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
            storage_service.upload_file(in_provider, local_file_path, in_path)
        except Exception as e:
            log.error("Upload failed: %s", e)
            return None
        
        log.info("Waiting for output file (max %ds)...", timeout_seconds)
        
        # Wait for the output file
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
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

        
    def execute_command(self, command, environment, working_directory, job_name, stdout_file=None):
        """
        Execute a command using OSCAR service via file upload/download and return the exit code.
        
        Args:
            command: List of command and arguments to execute
            environment: Dictionary of environment variables
            working_directory: Directory to execute the command in
            job_name: Name of the job (for logging)
            stdout_file: Optional stdout redirection file
            
        Returns:
            Exit code of the command (0 for success, non-zero for failure)
        """
        log.info("[job %s] Executing command via OSCAR: %s", job_name, " ".join(command))
        log.debug("[job %s] Working directory: %s", job_name, working_directory)
        log.debug("[job %s] Environment variables: %s", job_name, environment)
        
        script_path = None
        output_path = None
        
        try:
            # Create a temporary directory for scripts
            temp_dir = tempfile.mkdtemp(prefix="cwl_oscar_")
            
            # Create command script file with job-specific ID
            job_id = f"{job_name}_{int(time.time())}"
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
                
                # The output file should contain the exit code
                # For OSCAR script execution, it typically contains the exit code
                exit_code = int(output_content) if output_content.isdigit() else 0
                
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
                 oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, service_name, runtime_context):
        super(OSCARTask, self).__init__(builder, joborder, make_path_mapper, requirements, hints, name)
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.mount_path = mount_path
        self.service_name = service_name
        self.runtime_context = runtime_context
        
        # Create OSCAR executor
        self.executor = OSCARExecutor(oscar_endpoint, oscar_token, oscar_username, oscar_password, service_name)
        
    def run(self, runtimeContext, tmpdir_lock=None):
        """Execute the job using OSCAR with run-specific workspace."""
        try:
            log.info("[job %s] Starting OSCAR execution", self.name)
            
            # Generate job ID for this run
            job_id = f"{self.name}_{int(time.time())}"
            
            # Build the command line
            cmd = self.build_command_line()
            
            # Set up environment 
            env = self.build_environment()
            
            # Set working directory - the command script will create its own run-specific directory
            workdir = self.mount_path
            
            # Execute the command using OSCAR
            stdout_file = getattr(self, 'stdout', None)
            
            exit_code = self.executor.execute_command(
                command=cmd,
                environment=env,
                working_directory=workdir,
                job_name=self.name,
                stdout_file=stdout_file
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
                log.info("[job %s] Looking for outputs in: %s", self.name, output_dir)
                
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
    
    def __init__(self, toolpath_object, loading_context, oscar_endpoint, oscar_token, oscar_username, oscar_password, mount_path, service_name):
        super(OSCARCommandLineTool, self).__init__(toolpath_object, loading_context)
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.mount_path = mount_path
        self.service_name = service_name
        
    def make_path_mapper(self, reffiles, stagedir, runtimeContext, separateDirs):
        """Create a path mapper for OSCAR execution."""
        return OSCARPathMapper(
            reffiles, runtimeContext.basedir, stagedir, separateDirs, mount_path=self.mount_path)
            
    def make_job_runner(self, runtimeContext):
        """Create a job runner for OSCAR execution."""
        def job_runner(builder, joborder, make_path_mapper, requirements, hints, name):
            return OSCARTask(
                builder=builder,
                joborder=joborder,
                make_path_mapper=make_path_mapper,
                requirements=requirements,
                hints=hints,
                name=name,
                oscar_endpoint=self.oscar_endpoint,
                oscar_token=self.oscar_token,
                oscar_username=self.oscar_username,
                oscar_password=self.oscar_password,
                mount_path=self.mount_path,
                service_name=self.service_name,
                runtime_context=runtimeContext
            )
        return job_runner 
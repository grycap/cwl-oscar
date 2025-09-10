#!/usr/bin/env python3
# Copyright 2025 Universitat Politècnica de València and contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""OSCAR Task for job execution."""

import logging
import os
import re
import time
from typing import Dict, Any, Optional

from cwltool.job import JobBase

try:
    from constants import *
    from service_manager import OSCARServiceManager
    from executor import OSCARExecutor
except ImportError:
    # Fallback for package import
    from .constants import *
    from .service_manager import OSCARServiceManager
    from .executor import OSCARExecutor

log = logging.getLogger("oscar-backend")


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
    
    def _wait_for_output_directory(self, output_dir, max_retries=5, retry_delay=3):
        """
        Wait for output directory to appear with retry logic for shared mount sync.
        
        Args:
            output_dir: Path to the output directory to check
            max_retries: Maximum number of retry attempts
            retry_delay: Delay in seconds between retries
            
        Returns:
            bool: True if directory exists, False if not found after all retries
        """
        for attempt in range(max_retries + 1):  # +1 to include initial attempt
            if os.path.exists(output_dir):
                if attempt > 0:
                    log.info(LOG_PREFIX_JOB + " Output directory found after %d retries: %s", self.name, attempt, output_dir)
                return True
            
            if attempt < max_retries:  # Don't sleep after the last attempt
                log.debug(LOG_PREFIX_JOB + " Output directory not found (attempt %d/%d), waiting %d seconds: %s", 
                         self.name, attempt + 1, max_retries + 1, retry_delay, output_dir)
                time.sleep(retry_delay)
        
        return False
        
    def run(self, runtimeContext, tmpdir_lock=None):
        """Execute the job using OSCAR with run-specific workspace."""
        try:
            log.info(LOG_PREFIX_JOB + " Starting OSCAR execution", self.name)
            
            # Generate job ID for this run using base step name (strip scatter suffixes)
            base_step_name = re.sub(r'_\d+$', '', self.name)
            job_id = f"{base_step_name}_{int(time.time())}"
            log.debug(LOG_PREFIX_JOB + " Generated job_id: %s (from step: %s)", self.name, job_id, self.name)
            
            # Build the command line
            cmd = self.build_command_line()
            
            # Set up environment 
            env = self.build_environment()
            
            # Set working directory - the command script will create its own run-specific directory
            workdir = self.mount_path
            
            # Get cluster for this specific step (uses step mapping if available, otherwise round-robin)
            cluster_config = self.cluster_manager.get_cluster_for_step(base_step_name)
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
                
                # * Check if output directory exists with retry logic for shared mount sync
                output_dir_found = self._wait_for_output_directory(output_dir)
                
                if output_dir_found:
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
                    log.warning(LOG_PREFIX_JOB + " Output directory not found after retries: %s", self.name, output_dir)
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

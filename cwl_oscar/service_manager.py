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

"""OSCAR Service Manager for dynamic service creation."""

import hashlib
import json
import logging
import re
import time
from typing import Dict, Any, Optional

try:
    from constants import *
    from scripts.oscar_service_script import OSCAR_SERVICE_SCRIPT_TEMPLATE
    from utils import create_oscar_client, sanitize_service_name
except ImportError:
    # Fallback for package import
    from .constants import *
    from .scripts.oscar_service_script import OSCAR_SERVICE_SCRIPT_TEMPLATE
    from .utils import create_oscar_client, sanitize_service_name

log = logging.getLogger("oscar-backend")


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
        
        # Extract base step name by removing CWL scatter suffixes (_2, _3, etc.)
        if job_name:
            # Remove CWL scatter suffixes like _2, _3, etc. to enable service reuse
            base_step_name = re.sub(r'_\d+$', '', job_name)
            tool_id = base_step_name
            log.debug("%s: Using base step name as tool ID: '%s' (from job_name: '%s')", LOG_PREFIX_SERVICE_MANAGER, tool_id, job_name)
        else:
            tool_id = "tool"
            log.debug("%s: No job_name provided, using default tool ID: '%s'", LOG_PREFIX_SERVICE_MANAGER, tool_id)
            
        # Create a hash based on tool content, requirements, mount path, and MinIO endpoint
        # ! Include mount_path and MinIO endpoint to ensure different configurations get unique services
        hash_content = {
            'baseCommand': tool_spec.get('baseCommand'),
            'class': tool_spec.get('class'),
            'requirements': requirements,
            'mount_path': self.mount_path  # * Include mount path in hash for service isolation
        }
        
        # Add MinIO endpoint to hash if present to prevent service reuse across different endpoints
        if self.shared_minio_config:
            hash_content['minio_endpoint'] = self.shared_minio_config.get('endpoint')
            log.debug("%s: Including MinIO endpoint in hash: %s", 
                     LOG_PREFIX_SERVICE_MANAGER, self.shared_minio_config.get('endpoint'))
        
        tool_content = json.dumps(hash_content, sort_keys=True)
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
                        "verify": shared_minio_config.get("verify_ssl", True),
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

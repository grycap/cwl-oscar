#!/usr/bin/env python3
"""
Local CWL-OSCAR Runner

Allows running local CWL workflows with local input files on OSCAR infrastructure.
Handles file uploads, workflow execution, and result downloads.
"""

import os
import json
import yaml
import time
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

try:
    from oscar_python.client import Client
except ImportError:
    raise ImportError("oscar-python package is required. Install with: pip install oscar-python")

log = logging.getLogger("cwl-oscar-local")


class OSCARLocalRunner:
    """Local runner for CWL workflows on OSCAR infrastructure."""
    
    def __init__(self, oscar_endpoint, oscar_token=None, oscar_username=None, oscar_password=None, 
                 mount_path="/mnt/cwl-oscar4/mount", cwl_oscar_service="cwl-oscar4"):
        """
        Initialize the local runner.
        
        Args:
            oscar_endpoint: OSCAR cluster endpoint URL
            oscar_token: OSCAR OIDC authentication token
            oscar_username: OSCAR username for basic authentication  
            oscar_password: OSCAR password for basic authentication
            mount_path: Mount path for shared data
            cwl_oscar_service: Name of the cwl-oscar service
        """
        self.oscar_endpoint = oscar_endpoint
        self.oscar_token = oscar_token
        self.oscar_username = oscar_username
        self.oscar_password = oscar_password
        self.mount_path = mount_path
        self.cwl_oscar_service = cwl_oscar_service
        self.client = None
        self.storage_service = None
        
    def get_client(self):
        """Get or create OSCAR client."""
        if self.client is None:
            if self.oscar_token:
                # Use OIDC token authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'oidc_token': self.oscar_token,
                    'ssl': 'True'
                }
            else:
                # Use basic username/password authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'user': self.oscar_username,
                    'password': self.oscar_password,
                    'ssl': 'True'
                }
            
            self.client = Client(options=options)
        return self.client
        
    def get_storage_service(self):
        """Get or create storage service."""
        if self.storage_service is None:
            self.storage_service = self.get_client().create_storage_client()
        return self.storage_service
        
    def get_service_config(self, service_name):
        """Get configuration for a specific service."""
        client = self.get_client()
        services_response = client.list_services()
        
        if services_response.status_code != 200:
            raise Exception(f"Failed to list services: {services_response.text}")
            
        services = json.loads(services_response.text)
        for service in services:
            if service.get('name') == service_name:
                return service
                
        raise Exception(f"Service {service_name} not found")
        
    def upload_file_to_mount(self, local_path, remote_filename=None):
        """
        Upload a local file to the OSCAR mount storage.
        
        Args:
            local_path: Path to local file
            remote_filename: Optional remote filename (default: use local filename)
            
        Returns:
            Remote path in mount storage
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
            
        if remote_filename is None:
            remote_filename = os.path.basename(local_path)
            
        # Remove leading slash and 'mnt' from mount_path to get storage path
        mount_parts = self.mount_path.strip('/').split('/')
        if mount_parts[0] == 'mnt':
            mount_parts = mount_parts[1:]
        storage_path = '/'.join(mount_parts)
        
        log.info("Uploading %s to %s/%s", local_path, storage_path, remote_filename)
        
        storage_service = self.get_storage_service()
        # upload_file expects: provider, local_file_path, remote_directory_path
        # It automatically uses the original filename
        storage_service.upload_file("minio.default", local_path, storage_path)
        
        return f"{self.mount_path}/{remote_filename}"
        
    def upload_workflow_files(self, workflow_path, input_path, additional_files=None):
        """
        Upload workflow, input file, and any additional files to mount storage.
        
        Args:
            workflow_path: Path to CWL workflow file
            input_path: Path to input YAML/JSON file  
            additional_files: Optional list of additional files to upload
            
        Returns:
            Dict with remote paths for uploaded files
        """
        uploaded_files = {}
        
        # Upload workflow file
        uploaded_files['workflow'] = self.upload_file_to_mount(workflow_path)
        
        # Upload input file
        uploaded_files['input'] = self.upload_file_to_mount(input_path)
        
        # Upload additional files if provided
        if additional_files:
            uploaded_files['additional'] = []
            for file_path in additional_files:
                remote_path = self.upload_file_to_mount(file_path)
                uploaded_files['additional'].append(remote_path)
                
        return uploaded_files
        
    def create_run_script(self, workflow_remote_path, input_remote_path, additional_args=None):
        """
        Create a run script for executing the workflow on OSCAR.
        
        Args:
            workflow_remote_path: Remote path to workflow file
            input_remote_path: Remote path to input file
            additional_args: Optional additional arguments for cwl-oscar
            
        Returns:
            Path to created run script
        """
        script_content = "#!/bin/bash\n\n"
        script_content += "/usr/local/bin/python /app/cwl-oscar \\\n"
        script_content += f"  --oscar-endpoint {self.oscar_endpoint} \\\n"
        
        if self.oscar_token:
            script_content += f"  --oscar-token {self.oscar_token} \\\n"
        else:
            script_content += f"  --oscar-username {self.oscar_username} \\\n"
            script_content += f"  --oscar-password {self.oscar_password} \\\n"
            
        script_content += f"  --mount-path {self.mount_path} \\\n"
        script_content += f"  --quiet \\\n"
        # script_content += f"  --service-name clt4 \\\n"
        
        if additional_args:
            for arg in additional_args:
                script_content += f"  {arg} \\\n"
                
        script_content += f"  {workflow_remote_path} \\\n"
        script_content += f"  {input_remote_path}\n"
        
        # Create temporary script file
        script_fd, script_path = tempfile.mkstemp(suffix='.sh', prefix='cwl_oscar_run_')
        with os.fdopen(script_fd, 'w') as f:
            f.write(script_content)
            
        os.chmod(script_path, 0o755)
        
        log.debug("Created run script: %s", script_path)
        log.debug("Script content:\n%s", script_content)
        
        return script_path
        
    def submit_and_wait(self, script_path, timeout_seconds=600, check_interval=10):
        """
        Submit run script to cwl-oscar service and wait for completion.
        
        Args:
            script_path: Path to run script
            timeout_seconds: Maximum wait time
            check_interval: How often to check for completion
            
        Returns:
            True if successful, False otherwise
        """
        service_config = self.get_service_config(self.cwl_oscar_service)
        storage_service = self.get_storage_service()
        
        # Extract service paths
        in_provider = service_config['input'][0]['storage_provider']
        in_path = service_config['input'][0]['path']
        out_provider = service_config['output'][0]['storage_provider']
        out_path = service_config['output'][0]['path']
        
        script_name = os.path.basename(script_path)
        expected_output = f"{script_name}.exit_code"
        
        log.info("Submitting workflow to OSCAR service: %s", self.cwl_oscar_service)
        log.info("Expected completion file: %s", expected_output)
        
        # Check if exit code file already exists and remove it
        try:
            existing_files = storage_service.list_files_from_path(out_provider, out_path + "/")
            if isinstance(existing_files, dict) and 'Contents' in existing_files:
                for file_info in existing_files['Contents']:
                    if file_info['Key'].endswith(expected_output):
                        log.info("Removing old exit code file: %s", file_info['Key'])
                        storage_service.delete_file(out_provider, file_info['Key'])
            elif isinstance(existing_files, list):
                for file_info in existing_files:
                    file_key = file_info.get('Key', file_info) if isinstance(file_info, dict) else file_info
                    if file_key.endswith(expected_output):
                        log.info("Removing old exit code file: %s", file_key)
                        storage_service.delete_file(out_provider, file_key)
        except Exception as e:
            log.debug("Could not check/clean old exit code files: %s", e)
        
        # Upload run script
        storage_service.upload_file(in_provider, script_path, in_path)
        
        log.info("Waiting for workflow completion (max %ds)...", timeout_seconds)
        
        # Wait for completion
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                files = storage_service.list_files_from_path(out_provider, out_path + "/")
                completion_found = False
                
                if isinstance(files, dict) and 'Contents' in files:
                    # Handle AWS S3-style response
                    for file_info in files['Contents']:
                        if file_info['Key'].endswith(expected_output):
                            log.info("Found completion file: %s", file_info['Key'])
                            completion_found = True
                            break
                elif isinstance(files, list):
                    # Handle list response
                    for file_info in files:
                        file_key = file_info.get('Key', file_info) if isinstance(file_info, dict) else file_info
                        if file_key.endswith(expected_output):
                            log.info("Found completion file: %s", file_key)
                            completion_found = True
                            break
                
                if completion_found:
                    log.info("Workflow completed, checking exit code...")
                    # Download and check the exit code
                    try:
                        # Find the actual exit code file path
                        exit_code_file_key = None
                        if isinstance(files, dict) and 'Contents' in files:
                            for file_info in files['Contents']:
                                if file_info['Key'].endswith(expected_output):
                                    exit_code_file_key = file_info['Key']
                                    break
                        elif isinstance(files, list):
                            for file_info in files:
                                file_key = file_info.get('Key', file_info) if isinstance(file_info, dict) else file_info
                                if file_key.endswith(expected_output):
                                    exit_code_file_key = file_key
                                    break
                        
                        if exit_code_file_key:
                            # Download the exit code file to a temporary location
                            temp_dir = tempfile.mkdtemp()
                            try:
                                # Construct full remote path like download_results does
                                if exit_code_file_key.startswith('out/'):
                                    # Remove 'out/' prefix and combine with service out path
                                    file_only = exit_code_file_key[4:]  # Remove 'out/' prefix
                                    full_remote_path = out_path + '/' + file_only
                                else:
                                    # Use as-is if it doesn't start with 'out/'
                                    full_remote_path = out_path + '/' + exit_code_file_key
                                
                                log.debug("Downloading exit code file: provider=%s, path=%s", out_provider, full_remote_path)
                                storage_service.download_file(out_provider, temp_dir, full_remote_path)
                                
                                # Find the downloaded file
                                downloaded_file = None
                                for root, dirs, files in os.walk(temp_dir):
                                    for f in files:
                                        if f.endswith('.exit_code'):
                                            downloaded_file = os.path.join(root, f)
                                            break
                                    if downloaded_file:
                                        break
                                
                                if downloaded_file and os.path.exists(downloaded_file):
                                    # Read the exit code
                                    with open(downloaded_file, 'r') as f:
                                        exit_code_content = f.read().strip()
                                    
                                    log.info("Exit code file content: '%s'", exit_code_content)
                                    
                                    if exit_code_content.isdigit():
                                        exit_code = int(exit_code_content)
                                        if exit_code == 0:
                                            log.info("Workflow completed successfully (exit code: 0)")
                                            return True
                                        else:
                                            log.error("Workflow failed with exit code: %d", exit_code)
                                            return False
                                    else:
                                        log.warning("Invalid exit code format: '%s', treating as failure", exit_code_content)
                                        return False
                                else:
                                    log.warning("Could not find downloaded exit code file, treating as failure")
                                    return False
                            finally:
                                # Clean up temp directory
                                shutil.rmtree(temp_dir, ignore_errors=True)
                        else:
                            log.warning("Could not determine exit code file path, treating as failure")
                            return False
                            
                    except Exception as e:
                        log.error("Error checking exit code: %s, treating as failure", e)
                        return False
                    
            except Exception as e:
                log.debug("Error checking for completion: %s", e)
                
            log.debug("Waiting for completion... (%ds elapsed)", int(time.time() - start_time))
            time.sleep(check_interval)
            
        log.error("Workflow timed out after %d seconds", timeout_seconds)
        return False
        
    def download_results(self, output_dir="./results"):
        """
        Download workflow results from OSCAR output storage.
        
        Args:
            output_dir: Local directory to download results to
            
        Returns:
            Path to downloaded results directory
        """
        os.makedirs(output_dir, exist_ok=True)
        
        service_config = self.get_service_config(self.cwl_oscar_service)
        storage_service = self.get_storage_service()
        
        out_provider = service_config['output'][0]['storage_provider']
        out_path = service_config['output'][0]['path']  # e.g., "cwl-oscar4/out"
        
        log.info("Downloading results to: %s", output_dir)
        
        try:
            # List all files in output path
            files = storage_service.list_files_from_path(out_provider, out_path + "/")
            
            if isinstance(files, dict) and 'Contents' in files:
                # Handle AWS S3-style response with Contents key
                for file_info in files['Contents']:
                    if isinstance(file_info, dict) and 'Key' in file_info:
                        file_key = file_info['Key']  # e.g., "out/cwl_oscar_run_xxx.sh.exit_code"
                        # Skip directory entries
                        if file_key.endswith('/'):
                            continue
                            
                        filename = os.path.basename(file_key)
                        
                        # Construct full download path
                        # The file_key is relative to bucket root, but we need the full path
                        # out_path is like "cwl-oscar4/out", file_key is like "out/filename"
                        # We need to combine them properly to get "cwl-oscar4/out/filename"
                        if file_key.startswith('out/'):
                            # Remove 'out/' prefix and combine with service out path
                            file_only = file_key[4:]  # Remove 'out/' prefix
                            full_remote_path = out_path + '/' + file_only
                        else:
                            # Use as-is if it doesn't start with 'out/'
                            full_remote_path = out_path + '/' + file_key
                        
                        log.info("Downloading: %s -> %s", full_remote_path, filename)
                        # download_file expects: provider, local_directory, remote_full_path
                        storage_service.download_file(out_provider, output_dir, full_remote_path)
                        
                        # Check if file was downloaded to a nested structure and move if needed
                        for possible_subdir in ['out', os.path.dirname(file_key)]:
                            if possible_subdir:
                                nested_path = os.path.join(output_dir, possible_subdir, filename)
                                final_path = os.path.join(output_dir, filename)
                                if os.path.exists(nested_path) and nested_path != final_path:
                                    log.debug("Moving %s -> %s", nested_path, final_path)
                                    shutil.move(nested_path, final_path)
                                    # Try to clean up empty directory
                                    try:
                                        os.rmdir(os.path.join(output_dir, possible_subdir))
                                    except OSError:
                                        pass
                                    break
            elif isinstance(files, list):
                # Handle list of file objects
                for file_info in files:
                    if isinstance(file_info, dict) and 'Key' in file_info:
                        file_key = file_info['Key']
                        # Skip directory entries
                        if file_key.endswith('/'):
                            continue
                            
                        filename = os.path.basename(file_key)
                        local_path = os.path.join(output_dir, filename)
                        
                        log.info("Downloading: %s -> %s", file_key, filename)
                        # download_file expects: provider, local_directory, remote_full_path
                        storage_service.download_file(out_provider, output_dir, file_key)
                        
                        # Check if file was downloaded to a nested structure
                        nested_path = os.path.join(output_dir, file_key)
                        if os.path.exists(nested_path) and nested_path != local_path:
                            shutil.move(nested_path, local_path)
                    elif isinstance(file_info, str):
                        # Handle string file paths
                        if file_info.endswith('/'):
                            continue
                            
                        filename = os.path.basename(file_info)
                        local_path = os.path.join(output_dir, filename)
                        
                        log.info("Downloading: %s -> %s", file_info, filename)
                        # download_file expects: provider, local_directory, remote_full_path
                        storage_service.download_file(out_provider, output_dir, file_info)
            else:
                log.warning("Unknown files list format: %s", type(files))
                log.debug("Files content: %s", files)
                        
        except Exception as e:
            log.error("Error downloading results: %s", e)
            log.debug("Exception details:", exc_info=True)
            
        return output_dir
        
    def run_workflow(self, workflow_path, input_path, additional_files=None, 
                    additional_args=None, output_dir="./results", timeout_seconds=600):
        """
        Complete workflow execution: upload, run, and download results.
        
        Args:
            workflow_path: Path to CWL workflow file
            input_path: Path to input YAML/JSON file
            additional_files: Optional list of additional files
            additional_args: Optional additional cwl-oscar arguments
            output_dir: Local directory for results
            timeout_seconds: Maximum execution time
            
        Returns:
            Tuple of (success: bool, results_dir: str)
        """
        log.info("Starting local workflow execution")
        log.info("Workflow: %s", workflow_path)
        log.info("Input: %s", input_path)
        
        try:
            # Step 1: Upload files
            log.info("Step 1: Uploading files to OSCAR")
            uploaded_files = self.upload_workflow_files(workflow_path, input_path, additional_files)
            log.info("Uploaded files: %s", uploaded_files)
            
            # Step 2: Create and submit run script
            log.info("Step 2: Creating and submitting run script")
            script_path = self.create_run_script(
                uploaded_files['workflow'], 
                uploaded_files['input'], 
                additional_args
            )
            
            success = self.submit_and_wait(script_path, timeout_seconds)
            
            if not success:
                log.error("Workflow execution failed or timed out")
                return False, None
                
            # Step 3: Download results
            log.info("Step 3: Downloading results")
            results_dir = self.download_results(output_dir)
            
            log.info("Workflow execution completed successfully")
            log.info("Results available in: %s", results_dir)
            
            return True, results_dir
            
        except Exception as e:
            log.error("Workflow execution failed: %s", e)
            return False, None
            
        finally:
            # Clean up temporary script
            if 'script_path' in locals() and os.path.exists(script_path):
                os.remove(script_path)


def main():
    """Command line interface for local runner."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run local CWL workflows on OSCAR')
    parser.add_argument('workflow', help='Path to CWL workflow file')
    parser.add_argument('input', help='Path to input YAML/JSON file')
    parser.add_argument('--oscar-endpoint', required=True, help='OSCAR endpoint URL')
    parser.add_argument('--oscar-token', help='OSCAR OIDC token')
    parser.add_argument('--oscar-username', help='OSCAR username')
    parser.add_argument('--oscar-password', help='OSCAR password')
    parser.add_argument('--mount-path', default='/mnt/cwl-oscar4/mount', help='Mount path')
    parser.add_argument('--service-name', default='cwl-oscar4', help='CWL-OSCAR service name')
    parser.add_argument('--output-dir', default='./results', help='Output directory')
    parser.add_argument('--timeout', type=int, default=600, help='Timeout in seconds')
    parser.add_argument('--additional-files', nargs='*', help='Additional files to upload')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set up logging
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create runner
    runner = OSCARLocalRunner(
        oscar_endpoint=args.oscar_endpoint,
        oscar_token=args.oscar_token,
        oscar_username=args.oscar_username,
        oscar_password=args.oscar_password,
        mount_path=args.mount_path,
        cwl_oscar_service=args.service_name
    )
    
    # Run workflow
    success, results_dir = runner.run_workflow(
        workflow_path=args.workflow,
        input_path=args.input,
        additional_files=args.additional_files,
        output_dir=args.output_dir,
        timeout_seconds=args.timeout
    )
    
    if success:
        print(f"✅ Workflow completed successfully")
        print(f"📁 Results in: {results_dir}")
        exit(0)
    else:
        print("❌ Workflow execution failed")
        exit(1)


if __name__ == "__main__":
    main() 
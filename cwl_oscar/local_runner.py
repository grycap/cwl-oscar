#!/usr/bin/env python3

"""
Local CWL-OSCAR Runner

A tool for running CWL workflows from your local machine on remote OSCAR clusters.

Key Features:
- Upload local workflow files to OSCAR cluster storage
- Execute workflows on remote OSCAR infrastructure  
- Download results back to local machine
- Support for multiple OSCAR clusters
- Shared MinIO storage for multi-cluster workflows

Usage:
    python local_runner.py --cluster-endpoint https://oscar.example.com \
                           --cluster-token your-token \
                           workflow.cwl input.json
"""

import os
import json
import time
import tempfile
import shutil
import logging

try:
    from oscar_python.client import Client
except ImportError:
    raise ImportError("oscar-python package is required. Install with: pip install oscar-python")

log = logging.getLogger("cwl-oscar-local")


class OSCARLocalRunner:
    """Local runner for CWL workflows on OSCAR infrastructure."""

    def __init__(self, clusters, mount_path="/mnt/cwl-oscar/mount", cwl_oscar_service="cwl-oscar", 
                 shared_minio_config=None):
        """
        Initialize the local runner.

        Args:
            clusters: List of cluster configurations for multi-cluster support
            mount_path: Mount path for shared data
            cwl_oscar_service: Name of the cwl-oscar service
            shared_minio_config: Shared MinIO configuration for multi-cluster support
        """
        self.clusters = clusters
        self.primary_cluster = clusters[0]  # Use first cluster for service operations
        self.mount_path = mount_path
        self.cwl_oscar_service = cwl_oscar_service
        self.shared_minio_config = shared_minio_config
        self.client = None
        self.storage_service = None

        # Expose primary cluster properties for compatibility
        self.oscar_endpoint = self.primary_cluster['endpoint']
        self.oscar_token = self.primary_cluster.get('token')
        self.oscar_username = self.primary_cluster.get('username')
        self.oscar_password = self.primary_cluster.get('password')
        self.ssl = self.primary_cluster.get('ssl', True)

    def get_client(self):
        """Get or create OSCAR client."""
        if self.client is None:
            if self.oscar_token:
                # Use OIDC token authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'oidc_token': self.oscar_token,
                    'ssl': str(self.ssl)
                }
            else:
                # Use basic username/password authentication
                options = {
                    'cluster_id': 'oscar-cluster',
                    'endpoint': self.oscar_endpoint,
                    'user': self.oscar_username,
                    'password': self.oscar_password,
                    'ssl': str(self.ssl)
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
        
        # Add cluster configurations
        for cluster in self.clusters:
            script_content += f"  --cluster-endpoint {cluster['endpoint']} \\\n"
            if cluster.get('token'):
                script_content += f"  --cluster-token {cluster['token']} \\\n"
            else:
                script_content += f"  --cluster-username {cluster['username']} \\\n"
                script_content += f"  --cluster-password {cluster['password']} \\\n"
            if not cluster.get('ssl', True):
                script_content += f"  --cluster-disable-ssl \\\n"
            if cluster.get('steps'):
                steps_str = ','.join(cluster['steps'])
                script_content += f"  --cluster-steps {steps_str} \\\n"
        
        # Add shared MinIO configuration for multi-cluster
        if len(self.clusters) > 1 and self.shared_minio_config:
            script_content += f"  --shared-minio-endpoint {self.shared_minio_config['endpoint']} \\\n"
            script_content += f"  --shared-minio-access-key {self.shared_minio_config['access_key']} \\\n"
            script_content += f"  --shared-minio-secret-key {self.shared_minio_config['secret_key']} \\\n"
            if self.shared_minio_config.get('region'):
                script_content += f"  --shared-minio-region {self.shared_minio_config['region']} \\\n"
            if not self.shared_minio_config.get('verify_ssl', True):
                script_content += f"  --shared-minio-disable-ssl \\\n"
            
        script_content += f"  --mount-path {self.mount_path} \\\n"
        script_content += f"  --service-name {self.cwl_oscar_service} \\\n"
        
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
        out_path = service_config['output'][0]['path']  # e.g., "cwl-oscar/out"
        
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
                        # out_path is like "cwl-oscar/out", file_key is like "out/filename"
                        # We need to combine them properly to get "cwl-oscar/out/filename"
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
        
    def create_cwl_oscar_service(self, service_name="cwl-oscar"):
        """
        Create a cwl-oscar service on the OSCAR cluster.
        
        Args:
            service_name: Name of the service to create
            
        Returns:
            True if service was created successfully, False otherwise
        """
        log.info("Creating cwl-oscar service: %s", service_name)
        
        try:
            client = self.get_client()
            
            # Check if service already exists
            services_response = client.list_services()
            if services_response.status_code == 200:
                existing_services = json.loads(services_response.text)
                for service in existing_services:
                    if service.get('name') == service_name:
                        log.info("Service '%s' already exists on cluster", service_name)
                        return True
            
            # Create service definition based on cwl-oscar.yaml template
            service_def = self._create_cwl_oscar_service_definition(service_name)
            
            # Create the service
            log.info("Creating service '%s' on OSCAR cluster", service_name)
            response = client.create_service(service_def)
            
            if response.status_code in [200, 201]:
                log.info("✅ Service '%s' created successfully", service_name)
                return True
            else:
                log.error("❌ Failed to create service '%s': HTTP %d - %s", 
                         service_name, response.status_code, response.text)
                return False
                
        except Exception as e:
            log.error("❌ Error creating service '%s': %s", service_name, e)
            return False
    
    def _create_cwl_oscar_service_definition(self, service_name):
        """
        Create service definition for cwl-oscar service based on the FDL template.
        
        Args:
            service_name: Name of the service
            
        Returns:
            Service definition dictionary
        """
        # * Script template based on fdl-orchestrator/script.sh
        script_template = '''#!/bin/bash

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
bash "$INPUT_FILE_PATH" 1> "$TMP_OUTPUT_DIR/$FILE_NAME.out.log" # 2> "$TMP_OUTPUT_DIR/$FILE_NAME.err.log"
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
        
        # * Service definition based on cwl-oscar.yaml template
        service_def = {
            'name': service_name,
            'memory': '500Mi',
            'cpu': '0.5',
            'image': 'robertbio/cwl-oscar:latest',
            'script': script_template,
            'environment': {
                'variables': {
                    'MOUNT_PATH': self.mount_path
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
                'path': f'{service_name}/mount'
            }
        }
        
        # Add shared MinIO configuration if available
        if self.shared_minio_config:
            service_def["storage_providers"] = {
                "minio": {
                    "shared": {
                        "endpoint": self.shared_minio_config["endpoint"],
                        "verify": self.shared_minio_config.get("verify_ssl", True),
                        "access_key": self.shared_minio_config["access_key"],
                        "secret_key": self.shared_minio_config["secret_key"],
                        "region": self.shared_minio_config.get("region", "us-east-1")
                    }
                }
            }
            
            # Update mount to use shared MinIO
            service_def["mount"]["storage_provider"] = "minio.shared"
        
        log.debug("Created service definition: %s", json.dumps(service_def, indent=2))
        return service_def
        
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
    
    # Positional arguments (optional when using --init)
    parser.add_argument('workflow', nargs='?', help='Path to CWL workflow file')
    parser.add_argument('input', nargs='?', help='Path to input YAML/JSON file')
    
    # Service initialization
    parser.add_argument('--init', '--create', action='store_true', 
                        help='Initialize/create cwl-oscar service on OSCAR cluster')
    
    # Multi-cluster support
    parser.add_argument("--cluster-endpoint", type=str, action='append',
                        help="OSCAR cluster endpoint URL (can be specified multiple times for multiple clusters)")
    parser.add_argument("--cluster-token", type=str, action='append',
                        help="OSCAR OIDC authentication token for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-username", type=str, action='append',
                        help="OSCAR username for basic authentication for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-password", type=str, action='append',
                        help="OSCAR password for basic authentication for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-disable-ssl", action='append_const', const=True,
                        help="Disable SSL verification for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-steps", type=str, action='append',
                        help="Comma-separated list of workflow steps to execute on corresponding cluster (can be specified multiple times)")
    
    # Shared MinIO bucket configuration for multi-cluster support
    parser.add_argument("--shared-minio-endpoint", type=str,
                        help="Shared MinIO endpoint URL for multi-cluster support (all clusters will use this bucket)")
    parser.add_argument("--shared-minio-access-key", type=str,
                        help="Shared MinIO access key for multi-cluster support")
    parser.add_argument("--shared-minio-secret-key", type=str,
                        help="Shared MinIO secret key for multi-cluster support")
    parser.add_argument("--shared-minio-region", type=str,
                        help="Shared MinIO region for multi-cluster support")
    parser.add_argument("--shared-minio-disable-ssl", action="store_true", default=False,
                        help="Disable SSL certificate verification for shared MinIO")
    
    # Execution configuration
    parser.add_argument('--mount-path', default='/mnt/cwl-oscar/mount', help='Mount path for shared data')
    parser.add_argument('--service-name', default='cwl-oscar', help='CWL-OSCAR service name (used for both execution and initialization)')
    parser.add_argument('--output-dir', default='./results', help='Output directory')
    parser.add_argument('--timeout', type=int, default=600, help='Timeout in seconds')
    parser.add_argument('--additional-files', nargs='*', help='Additional files to upload')
    
    # Execution options
    parser.add_argument('--parallel', action='store_true', help='Enable parallel execution')
    parser.add_argument('--on-error', choices=['stop', 'continue'], default='stop',
                        help='Desired workflow behavior when a step fails')
    parser.add_argument('--compute-checksum', action='store_true', default=True,
                        help='Compute checksum of contents while collecting outputs')
    parser.add_argument('--no-compute-checksum', action='store_false', dest='compute_checksum',
                        help='Do not compute checksum of contents while collecting outputs')
    parser.add_argument('--default-container', help='Specify a default docker container')
    parser.add_argument('--timestamps', action='store_true', help='Add timestamps to the errors, warnings, and notifications')
    
    # Logging options
    logging_group = parser.add_mutually_exclusive_group()
    logging_group.add_argument('--verbose', action='store_true', help='Default logging')
    logging_group.add_argument('--quiet', action='store_true', help='Only print warnings and errors')
    logging_group.add_argument('--debug', action='store_true', help='Print even more logging')
    
    args = parser.parse_args()
    
    # Parse multi-cluster configuration
    clusters = []
    if args.cluster_endpoint:
        endpoint_count = len(args.cluster_endpoint)
        
        # Ensure we have matching counts for authentication
        if args.cluster_token:
            if len(args.cluster_token) != endpoint_count:
                print("Error: Number of --cluster-token arguments must match --cluster-endpoint arguments")
                return 1
        if args.cluster_username:
            if len(args.cluster_username) != endpoint_count:
                print("Error: Number of --cluster-username arguments must match --cluster-endpoint arguments")
                return 1
        if args.cluster_password:
            if len(args.cluster_password) != endpoint_count:
                print("Error: Number of --cluster-password arguments must match --cluster-endpoint arguments")
                return 1
        if args.cluster_disable_ssl:
            if len(args.cluster_disable_ssl) != endpoint_count:
                print("Error: Number of --cluster-disable-ssl arguments must match --cluster-endpoint arguments")
                return 1
        if args.cluster_steps:
            if len(args.cluster_steps) != endpoint_count:
                print("Error: Number of --cluster-steps arguments must match --cluster-endpoint arguments")
                return 1
        
        # Build cluster configurations
        for i in range(endpoint_count):
            cluster = {
                'endpoint': args.cluster_endpoint[i],
                'token': args.cluster_token[i] if args.cluster_token else None,
                'username': args.cluster_username[i] if args.cluster_username else None,
                'password': args.cluster_password[i] if args.cluster_password else None,
                'ssl': not (args.cluster_disable_ssl and args.cluster_disable_ssl[i]),
                'steps': [step.strip() for step in args.cluster_steps[i].split(',') if step.strip()] if args.cluster_steps else []
            }
            
            # Validate authentication for this cluster
            if not cluster['token'] and not cluster['username']:
                print(f"Error: cluster {i+1} requires either --cluster-token or --cluster-username")
                return 1
            if cluster['username'] and not cluster['password']:
                print(f"Error: cluster {i+1} needs --cluster-password when using --cluster-username")
                return 1
            
            clusters.append(cluster)
    
    # Handle shared MinIO configuration
    shared_minio_config = None
    if len(clusters) > 1:
        # Multi-cluster mode requires shared MinIO bucket
        if not args.shared_minio_endpoint:
            print("Error: --shared-minio-endpoint is required for multi-cluster mode")
            return 1
        if not args.shared_minio_access_key or not args.shared_minio_secret_key:
            print("Error: --shared-minio-access-key and --shared-minio-secret-key are required for multi-cluster mode")
            return 1
        
        shared_minio_config = {
            'endpoint': args.shared_minio_endpoint,
            'access_key': args.shared_minio_access_key,
            'secret_key': args.shared_minio_secret_key,
            'region': args.shared_minio_region,
            'verify_ssl': not args.shared_minio_disable_ssl
        }
    elif args.cluster_endpoint:
        print("Single cluster mode - using default cluster MinIO bucket")
    
    # Require cluster configuration
    if not clusters:
        print("Error: --cluster-endpoint is required (at least one cluster must be specified)")
        return 1
    
    # Validate arguments based on mode
    if args.init:
        # Initialization mode - workflow and input are optional
        log.info("Running in initialization mode")
    else:
        # Normal execution mode - workflow and input are required
        if not args.workflow or not args.input:
            print("Error: workflow and input arguments are required for normal execution")
            print("Use --init to create a service without running a workflow")
            return 1
    
    # Set up logging
    if args.quiet:
        level = logging.WARNING
    elif args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Prepare additional arguments to pass to cwl-oscar
    additional_args = []
    if args.quiet:
        additional_args.append('--quiet')
    elif args.debug:
        additional_args.append('--debug')
    if args.parallel:
        additional_args.append('--parallel')
    if args.on_error != 'stop':
        additional_args.extend(['--on-error', args.on_error])
    if not args.compute_checksum:
        additional_args.append('--no-compute-checksum')
    if args.default_container:
        additional_args.extend(['--default-container', args.default_container])
    if args.timestamps:
        additional_args.append('--timestamps')
    
    # Create runner
    runner = OSCARLocalRunner(
        clusters=clusters,
        mount_path=args.mount_path,
        cwl_oscar_service=args.service_name,
        shared_minio_config=shared_minio_config
    )
    
    # Handle initialization mode
    if args.init:
        print(f"🚀 Initializing cwl-oscar service: {args.service_name}")
        success = runner.create_cwl_oscar_service(args.service_name)
        
        if success:
            print(f"✅ Service '{args.service_name}' initialized successfully")
            print(f"🔗 Cluster: {runner.oscar_endpoint}")
            print(f"📁 Mount path: {args.mount_path}")
            return 0
        else:
            print(f"❌ Failed to initialize service '{args.service_name}'")
            return 1
    
    # Run workflow
    success, results_dir = runner.run_workflow(
        workflow_path=args.workflow,
        input_path=args.input,
        additional_files=args.additional_files,
        additional_args=additional_args,
        output_dir=args.output_dir,
        timeout_seconds=args.timeout
    )
    
    if success:
        print(f"✅ Workflow completed successfully")
        print(f"📁 Results in: {results_dir}")
        return 0
    else:
        print("❌ Workflow execution failed")
        return 1


if __name__ == "__main__":
    main() 


### This is only a test to connect to an OSCAR service via python
### It is not used in the cwl-oscar project

from oscar_python.client import Client
import json
import os
import time
import uuid
from typing import Optional

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, continue with os.environ

# OSCAR service configuration - now using environment variables
TEST_OSCAR_ENDPOINT = os.getenv("TEST_OSCAR_ENDPOINT")
TEST_OSCAR_USERNAME = os.getenv("TEST_OSCAR_USERNAME")
TEST_OSCAR_PASSWORD = os.getenv("TEST_OSCAR_PASSWORD")
TEST_SERVICE_NAME = os.getenv("TEST_SERVICE_NAME")

if not TEST_OSCAR_PASSWORD:
    raise ValueError("TEST_OSCAR_PASSWORD environment variable is required")

options_basic_auth = {
    'cluster_id': 'grnet',
    'endpoint': TEST_OSCAR_ENDPOINT,
    'user': TEST_OSCAR_USERNAME,
    'password': TEST_OSCAR_PASSWORD,
    'ssl': 'True'
}


def create_command_script(cmd: list[str], output_dir: str = ".") -> str:
    """Create a script file with commands from a list of strings."""
    random_uuid = str(uuid.uuid4())
    script_name = f"command_{random_uuid}.txt"
    script_path = os.path.join(output_dir, script_name)
    
    os.makedirs(output_dir, exist_ok=True)
    
    with open(script_path, 'w') as f:
        for command in cmd:
            f.write(command + '\n')
    
    print(f"Created script: {script_path}")
    return script_path


def upload_and_wait_for_output(
    storage_service, 
    local_file_path: str, 
    in_provider: str, 
    in_path: str, 
    out_provider: str, 
    out_path: str,
    timeout_seconds: int = 300,
    check_interval: int = 5
) -> Optional[dict]:
    """Upload a file to OSCAR service input and wait for the corresponding output file."""
    
    file_name = os.path.basename(local_file_path)
    expected_output_name = file_name + '.output'
    expected_output_path = out_path + "/" + expected_output_name
    
    print(f"Uploading {file_name}...")
    
    # Upload the input file
    try:
        storage_service.upload_file(in_provider, local_file_path, in_path)
    except Exception as e:
        print(f"Upload failed: {e}")
        return None
    
    print(f"Waiting for output file (max {timeout_seconds}s)...")
    
    # Wait for the output file
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            files = storage_service.list_files_from_path(out_provider, expected_output_path)
            
            if 'Contents' in files:
                for file_entry in files['Contents']:
                    if file_entry['Key'] == expected_output_path or file_entry['Key'].endswith(expected_output_name):
                        print(f"Output file found: {file_entry['Key']} ({file_entry['Size']} bytes)")
                        return file_entry
            
            time.sleep(check_interval)
            
        except Exception as e:
            print(f"Error checking for output: {e}")
            time.sleep(check_interval)
    
    print(f"Timeout: Output file not found after {timeout_seconds} seconds")
    return None


def download_output_file(
    storage_service,
    out_provider: str,
    remote_output_path: str,
    local_download_path: str,
    service_out_path: str
) -> bool:
    """Download an output file from OSCAR service."""
    
    try:
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
        
        print(f"Downloading {filename}...")
        
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
                import shutil
                shutil.move(downloaded_file_path, local_download_path)
            
            print(f"Download successful: {local_download_path}")
            return True
        else:
            print("Downloaded file not found in expected locations")
            return False
            
    except Exception as e:
        print(f"Download failed: {e}")
        return False


def execute_commands(commands: list[str]) -> str:
    """Execute a list of commands via OSCAR and return the output content."""
    
    # Initialize client and get service configuration
    client = Client(options=options_basic_auth)
    storage_service = client.create_storage_client()
    
    services = client.list_services()
    service_json = json.loads(services.text)
    
    # Find the target service
    service = None
    for svc in service_json:
        if svc['name'] == TEST_SERVICE_NAME:
            service = svc
            break
    
    if not service:
        raise Exception(f"Service {TEST_SERVICE_NAME} not found")
    
    # Extract service configuration
    in_provider = service['input'][0]['storage_provider']
    in_path = service['input'][0]['path']
    out_provider = service['output'][0]['storage_provider']
    out_path = service['output'][0]['path']
    
    # Create and upload script
    script_path = create_command_script(commands, output_dir="./scripts")
    
    # Upload and wait for output
    output_file = upload_and_wait_for_output(
        storage_service=storage_service,
        local_file_path=script_path,
        in_provider=in_provider,
        in_path=in_path,
        out_provider=out_provider,
        out_path=out_path
    )
    
    if not output_file:
        raise Exception("Failed to get output file")
    
    # Download result
    output_filename = os.path.basename(script_path) + '.output'
    local_output_path = os.path.join(os.getcwd(), output_filename)
    
    success = download_output_file(
        storage_service=storage_service,
        out_provider=out_provider,
        remote_output_path=output_file['Key'],
        local_download_path=local_output_path,
        service_out_path=out_path
    )
    
    if not success:
        raise Exception("Failed to download output file")
    
    # Read and return output content
    with open(local_output_path, 'r') as f:
        content = f.read().strip()
    
    # Clean up files
    os.remove(script_path)
    os.remove(local_output_path)
    
    return content


# Example usage
if __name__ == "__main__":
    try:
        # Example 1: Simple command
        print("=== Example 1: Simple ls command ===")
        result1 = execute_commands(["ls -lah /"])
        print(f"Result: {result1}")
        
        # Example 2: Multiple commands
        print("\n=== Example 2: Multiple commands ===")
        result2 = execute_commands([
            "#!/bin/bash",
            "echo 'System Information:'",
            "uname -a",
            "echo 'Current directory:'", 
            "pwd",
            "echo 'Available space:'",
            "df -h /"
        ])
        print(f"Result:\n{result2}")
        
    except Exception as e:
        print(f"Error: {e}")


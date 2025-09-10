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

"""Test script for cwl-oscar implementation."""

import os
import tempfile
import json
import sys
import subprocess

# Test configuration
TEST_OSCAR_ENDPOINT = ""
TEST_OSCAR_TOKEN = ""
TEST_SERVICE_NAME = ""

def test_oscar_client():
    """Test OSCAR client connectivity."""
    print("Testing OSCAR client connectivity...")
    
    try:
        from oscar_python.client import Client
        
        options = {
            'cluster_id': 'test-cluster',
            'endpoint': TEST_OSCAR_ENDPOINT,
            'oidc_token': TEST_OSCAR_TOKEN,
            'ssl': 'True'
        }
        
        client = Client(options=options)
        
        # Test cluster info
        print("Getting cluster info...")
        info = client.get_cluster_info()
        print(f"Cluster info status: {info.status_code}")
        
        # Test service list
        print("Getting service list...")
        services = client.list_services()
        print(f"Services list status: {services.status_code}")
        
        if services.status_code == 200:
            services_data = services.json()
            print(f"Found {len(services_data)} services")
            
            # Check if our test service exists
            service_names = [s['name'] for s in services_data]
            if TEST_SERVICE_NAME in service_names:
                print(f"✓ Test service '{TEST_SERVICE_NAME}' found")
            else:
                print(f"✗ Test service '{TEST_SERVICE_NAME}' not found")
                print(f"Available services: {service_names}")
        
        return True
        
    except Exception as e:
        print(f"✗ OSCAR client test failed: {e}")
        return False

def test_cwl_oscar_basic():
    """Test basic cwl-oscar functionality."""
    print("\nTesting basic cwl-oscar functionality...")
    
    # Test version
    print("Testing version...")
    try:
        result = subprocess.run([
            "python", "../../cwl-oscar", "--version"
        ], capture_output=True, text=True, cwd="example")
        
        if result.returncode == 0:
            print(f"✓ Version: {result.stdout.strip()}")
        else:
            print(f"✗ Version failed: {result.stderr}")
            
    except Exception as e:
        print(f"✗ Version test failed: {e}")
        
    # Test help
    print("Testing help...")
    try:
        result = subprocess.run([
            "python", "../../cwl-oscar", "--help"
        ], capture_output=True, text=True, cwd="example")
        
        if result.returncode == 0:
            print("✓ Help command works")
        else:
            print(f"✗ Help failed: {result.stderr}")
            
    except Exception as e:
        print(f"✗ Help test failed: {e}")

def test_cwl_oscar_execution():
    """Test CWL workflow execution with OSCAR."""
    print("\nTesting CWL workflow execution...")
    
    try:
        # Test with dry run first (if available)
        print("Testing workflow execution...")
        result = subprocess.run([
            "python", "../../cwl-oscar",
            "--oscar-endpoint", TEST_OSCAR_ENDPOINT,
            "--oscar-token", TEST_OSCAR_TOKEN,
            "--service-name", TEST_SERVICE_NAME,
            "--debug",
            "hello.cwl", "input.json"
        ], capture_output=True, text=True, cwd="example", timeout=300)
        
        print(f"Return code: {result.returncode}")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
        
        if result.returncode == 0:
            print("✓ CWL workflow execution completed successfully")
            
            # Check for output file
            if os.path.exists("example/hello.txt"):
                with open("example/hello.txt", "r") as f:
                    content = f.read()
                print(f"✓ Output file created: {content.strip()}")
            else:
                print("✗ Output file not found")
                
        else:
            print(f"✗ CWL workflow execution failed with code {result.returncode}")
            
    except subprocess.TimeoutExpired:
        print("✗ CWL workflow execution timed out")
    except Exception as e:
        print(f"✗ CWL workflow execution failed: {e}")

def test_output_directory_retry():
    """Test the output directory retry logic."""
    print("\nTesting output directory retry logic...")
    
    try:
        import tempfile
        import time
        import threading
        from task import OSCARTask
        
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            test_output_dir = os.path.join(temp_dir, "test_output")
            
            # Create a mock OSCARTask instance
            class MockOSCARTask:
                def __init__(self):
                    self.name = "test_job"
                
                def _wait_for_output_directory(self, output_dir, max_retries=3, retry_delay=1):
                    """Copy the method from OSCARTask for testing."""
                    import logging
                    log = logging.getLogger("oscar-backend")
                    LOG_PREFIX_JOB = "[job %s]"
                    
                    for attempt in range(max_retries + 1):
                        if os.path.exists(output_dir):
                            if attempt > 0:
                                log.info(LOG_PREFIX_JOB + " Output directory found after %d retries: %s", self.name, attempt, output_dir)
                            return True
                        
                        if attempt < max_retries:
                            log.debug(LOG_PREFIX_JOB + " Output directory not found (attempt %d/%d), waiting %d seconds: %s", 
                                     self.name, attempt + 1, max_retries + 1, retry_delay, output_dir)
                            time.sleep(retry_delay)
                    
                    return False
            
            mock_task = MockOSCARTask()
            
            # Test 1: Directory doesn't exist - should return False
            print("Test 1: Directory doesn't exist")
            result = mock_task._wait_for_output_directory(test_output_dir, max_retries=2, retry_delay=0.5)
            if not result:
                print("✓ Correctly returned False when directory doesn't exist")
            else:
                print("✗ Should have returned False when directory doesn't exist")
                return False
            
            # Test 2: Directory exists immediately - should return True
            print("Test 2: Directory exists immediately")
            os.makedirs(test_output_dir)
            result = mock_task._wait_for_output_directory(test_output_dir, max_retries=2, retry_delay=0.5)
            if result:
                print("✓ Correctly returned True when directory exists immediately")
            else:
                print("✗ Should have returned True when directory exists immediately")
                return False
            
            # Clean up for next test
            os.rmdir(test_output_dir)
            
            # Test 3: Directory appears after delay - should return True
            print("Test 3: Directory appears after delay")
            
            def create_directory_after_delay():
                time.sleep(1.5)  # Wait 1.5 seconds then create directory
                os.makedirs(test_output_dir)
            
            # Start thread to create directory after delay
            thread = threading.Thread(target=create_directory_after_delay)
            thread.start()
            
            # This should find the directory after retries
            result = mock_task._wait_for_output_directory(test_output_dir, max_retries=3, retry_delay=1)
            thread.join()  # Wait for thread to complete
            
            if result:
                print("✓ Correctly found directory after retry delay")
            else:
                print("✗ Should have found directory after retry delay")
                return False
            
            print("✓ All output directory retry tests passed!")
            return True
            
    except Exception as e:
        print(f"✗ Output directory retry test failed: {e}")
        return False

def test_service_name_uniqueness():
    """Test that different MinIO configurations produce unique service names."""
    print("\nTesting service name uniqueness with different MinIO configs...")
    
    try:
        from service_manager import OSCARServiceManager
        
        # Mock tool spec and requirements
        tool_spec = {
            'baseCommand': ['echo', 'hello'],
            'class': 'CommandLineTool'
        }
        
        requirements = {
            'memory': '1Gi',
            'cpu': '1.0',
            'image': 'ubuntu:latest',
            'environment': {}
        }
        
        # Test 1: No shared MinIO config
        service_manager1 = OSCARServiceManager(
            oscar_endpoint="https://test1.example.com",
            oscar_token="token1",
            oscar_username=None,
            oscar_password=None,
            mount_path="/mnt/test",
            shared_minio_config=None
        )
        
        service_name1 = service_manager1.generate_service_name(tool_spec, requirements, "test-job")
        print(f"Service name without MinIO config: {service_name1}")
        
        # Test 2: With shared MinIO config - endpoint A
        shared_minio_config_a = {
            'endpoint': 'https://minio-a.example.com',
            'access_key': 'access_key_a',
            'secret_key': 'secret_key_a',
            'region': 'us-east-1'
        }
        
        service_manager2 = OSCARServiceManager(
            oscar_endpoint="https://test1.example.com",
            oscar_token="token1",
            oscar_username=None,
            oscar_password=None,
            mount_path="/mnt/test",
            shared_minio_config=shared_minio_config_a
        )
        
        service_name2 = service_manager2.generate_service_name(tool_spec, requirements, "test-job")
        print(f"Service name with MinIO config A: {service_name2}")
        
        # Test 3: With shared MinIO config - endpoint B (different endpoint)
        shared_minio_config_b = {
            'endpoint': 'https://minio-b.example.com',
            'access_key': 'access_key_a',  # Same access key (should not affect hash)
            'secret_key': 'secret_key_a',  # Same secret key (should not affect hash)
            'region': 'us-east-1'          # Same region (should not affect hash)
        }
        
        service_manager3 = OSCARServiceManager(
            oscar_endpoint="https://test1.example.com",
            oscar_token="token1",
            oscar_username=None,
            oscar_password=None,
            mount_path="/mnt/test",
            shared_minio_config=shared_minio_config_b
        )
        
        service_name3 = service_manager3.generate_service_name(tool_spec, requirements, "test-job")
        print(f"Service name with MinIO config B: {service_name3}")
        
        # Test 4: Different mount path (should create different service)
        service_manager4 = OSCARServiceManager(
            oscar_endpoint="https://test1.example.com",
            oscar_token="token1",
            oscar_username=None,
            oscar_password=None,
            mount_path="/mnt/different-path",  # Different mount path
            shared_minio_config=shared_minio_config_a  # Same MinIO config as test 2
        )
        
        service_name4 = service_manager4.generate_service_name(tool_spec, requirements, "test-job")
        print(f"Service name with different mount path: {service_name4}")
        
        # Test 5: Same endpoint as A but different access key (should create same service since only endpoint is hashed)
        shared_minio_config_c = {
            'endpoint': 'https://minio-a.example.com',  # Same endpoint as A
            'access_key': 'different_access_key',       # Different access key (should not affect hash)
            'secret_key': 'different_secret_key',
            'region': 'eu-west-1'                       # Different region (should not affect hash)
        }
        
        service_manager5 = OSCARServiceManager(
            oscar_endpoint="https://test1.example.com",
            oscar_token="token1",
            oscar_username=None,
            oscar_password=None,
            mount_path="/mnt/test",  # Same mount path as test 2
            shared_minio_config=shared_minio_config_c
        )
        
        service_name5 = service_manager5.generate_service_name(tool_spec, requirements, "test-job")
        print(f"Service name with same endpoint, different credentials: {service_name5}")
        
        # Verify service names: 1,2,3,4 should be unique, 2 and 5 should be the same
        service_names = [service_name1, service_name2, service_name3, service_name4, service_name5]
        print(f"All service names: {service_names}")
        
        # Check that services 2 and 5 are the same (same endpoint + mount path)
        if service_name2 == service_name5:
            print("✓ Services with same endpoint and mount path have same name (correct)")
        else:
            print("✗ Services with same endpoint and mount path should have same name")
            return False
        
        # Check that services 1, 2, 3, 4 are unique
        unique_services = [service_name1, service_name2, service_name3, service_name4]
        unique_names_subset = set(unique_services)
        
        if len(unique_names_subset) == len(unique_services):
            print("✓ Services with different configs have unique names - MinIO config isolation working correctly!")
            print(f"  Generated {len(unique_names_subset)} unique service names for different configurations")
            return True
        else:
            print("✗ Service names are not unique - MinIO config isolation failed!")
            print(f"  Expected {len(unique_services)} unique names, got {len(unique_names_subset)}")
            print(f"  Service names: {unique_services}")
            return False
            
    except Exception as e:
        print(f"✗ Service name uniqueness test failed: {e}")
        return False

def test_oscar_service_direct():
    """Test OSCAR service execution directly."""
    print("\nTesting OSCAR service execution directly...")
    
    try:
        from oscar_python.client import Client
        
        options = {
            'cluster_id': 'test-cluster',
            'endpoint': TEST_OSCAR_ENDPOINT,
            'oidc_token': TEST_OSCAR_TOKEN,
            'ssl': 'True'
        }
        
        client = Client(options=options)
        
        # Create a simple test script
        test_script = '''#!/bin/bash
echo "Hello from OSCAR service!"
echo "Current directory: $(pwd)"
echo "Mount path: $MOUNT_PATH"
echo "Test execution completed"
'''
        
        test_input = {
            "script": test_script,
            "job_name": "test-job",
            "command": "echo Hello from OSCAR"
        }
        
        print(f"Submitting job to service: {TEST_SERVICE_NAME}")
        response = client.run_service(
            TEST_SERVICE_NAME,
            input=json.dumps(test_input),
            async_call=False
        )
        
        print(f"Response status: {response.status_code}")
        print(f"Response content: {response.text}")
        
        if response.status_code == 200:
            print("✓ OSCAR service execution successful")
        else:
            print(f"✗ OSCAR service execution failed")
            
    except Exception as e:
        print(f"✗ OSCAR service direct test failed: {e}")

def main():
    """Run all tests."""
    print("CWL-OSCAR Test Suite")
    print("=" * 50)
    
    # Test 1: Output directory retry logic (unit test - doesn't require OSCAR connection)
    test_output_directory_retry()
    
    # Test 2: Service name uniqueness (unit test - doesn't require OSCAR connection)
    test_service_name_uniqueness()
    
    # Test 3: OSCAR client connectivity
    if not test_oscar_client():
        print("Skipping further tests due to OSCAR client failure")
        return 1
    
    # Test 4: Basic cwl-oscar functionality
    test_cwl_oscar_basic()
    
    # Test 5: Direct OSCAR service test
    test_oscar_service_direct()
    
    # Test 6: Full CWL workflow execution
    # test_cwl_oscar_execution()  # Commented out for now as it requires the service to be properly set up
    
    print("\n" + "=" * 50)
    print("Test suite completed!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 
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

"""
MinIO Bucket Cleanup Script

This script recursively deletes all objects in a MinIO bucket and then deletes the bucket itself.
It supports both synchronous and asynchronous deletion for better performance.

Usage:
    python minio_bucket_cleanup.py --endpoint <endpoint> --access-key <access-key> --secret-key <secret-key> --bucket <bucket-name> [--secure] [--region <region>] [--dry-run]

Example:
    python minio_bucket_cleanup.py --endpoint localhost:9000 --access-key minioadmin --secret-key minioadmin --bucket my-bucket --secure
"""

import argparse
import sys
import logging
from typing import List, Optional
from minio import Minio
from minio.error import S3Error
import concurrent.futures
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('minio_cleanup.log')
    ]
)
logger = logging.getLogger(__name__)


class MinIOBucketCleaner:
    """Class to handle MinIO bucket cleanup operations."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 secure: bool = True, region: Optional[str] = None):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint (host:port)
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            secure: Use HTTPS (True) or HTTP (False)
            region: AWS region (optional)
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.region = region
        
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region
            )
            logger.info(f"Successfully connected to MinIO at {endpoint}")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
    
    def list_all_objects(self, bucket_name: str) -> List[str]:
        """
        List all objects in the bucket recursively.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            List of object names
        """
        objects = []
        try:
            # List all objects in the bucket
            objects_iter = self.client.list_objects(bucket_name, recursive=True)
            for obj in objects_iter:
                objects.append(obj.object_name)
            
            logger.info(f"Found {len(objects)} objects in bucket '{bucket_name}'")
            return objects
            
        except S3Error as e:
            if e.code == 'NoSuchBucket':
                logger.warning(f"Bucket '{bucket_name}' does not exist")
                return []
            else:
                logger.error(f"Error listing objects in bucket '{bucket_name}': {e}")
                raise
    
    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete a single object from the bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            return True
        except S3Error as e:
            logger.error(f"Failed to delete object '{object_name}': {e}")
            return False
    
    def delete_objects_batch(self, bucket_name: str, object_names: List[str], 
                           max_workers: int = 10) -> int:
        """
        Delete multiple objects in parallel.
        
        Args:
            bucket_name: Name of the bucket
            object_names: List of object names to delete
            max_workers: Maximum number of parallel workers
            
        Returns:
            Number of successfully deleted objects
        """
        if not object_names:
            return 0
        
        deleted_count = 0
        failed_count = 0
        
        logger.info(f"Starting deletion of {len(object_names)} objects with {max_workers} workers")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all delete tasks
            future_to_object = {
                executor.submit(self.delete_object, bucket_name, obj_name): obj_name
                for obj_name in object_names
            }
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_object):
                obj_name = future_to_object[future]
                try:
                    if future.result():
                        deleted_count += 1
                        if deleted_count % 100 == 0:
                            logger.info(f"Deleted {deleted_count} objects...")
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Exception occurred while deleting '{obj_name}': {e}")
                    failed_count += 1
        
        logger.info(f"Deletion completed: {deleted_count} successful, {failed_count} failed")
        return deleted_count
    
    def delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete the bucket after ensuring it's empty.
        
        Args:
            bucket_name: Name of the bucket to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if bucket exists
            if not self.client.bucket_exists(bucket_name):
                logger.warning(f"Bucket '{bucket_name}' does not exist")
                return True
            
            # List any remaining objects
            remaining_objects = self.list_all_objects(bucket_name)
            if remaining_objects:
                logger.warning(f"Bucket '{bucket_name}' still contains {len(remaining_objects)} objects")
                return False
            
            # Delete the bucket
            self.client.remove_bucket(bucket_name)
            logger.info(f"Successfully deleted bucket '{bucket_name}'")
            return True
            
        except S3Error as e:
            logger.error(f"Failed to delete bucket '{bucket_name}': {e}")
            return False
    
    def cleanup_bucket(self, bucket_name: str, dry_run: bool = False, 
                      max_workers: int = 10) -> bool:
        """
        Complete bucket cleanup process.
        
        Args:
            bucket_name: Name of the bucket to cleanup
            dry_run: If True, only list objects without deleting
            max_workers: Maximum number of parallel workers for deletion
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting cleanup process for bucket '{bucket_name}'")
        
        try:
            # List all objects
            objects = self.list_all_objects(bucket_name)
            
            if not objects:
                logger.info(f"Bucket '{bucket_name}' is already empty")
                if not dry_run:
                    return self.delete_bucket(bucket_name)
                return True
            
            if dry_run:
                logger.info(f"DRY RUN: Would delete {len(objects)} objects from bucket '{bucket_name}'")
                logger.info(f"DRY RUN: Would delete bucket '{bucket_name}'")
                return True
            
            # Delete all objects
            deleted_count = self.delete_objects_batch(bucket_name, objects, max_workers)
            
            if deleted_count != len(objects):
                logger.error(f"Failed to delete all objects. Expected: {len(objects)}, Deleted: {deleted_count}")
                return False
            
            # Delete the bucket
            return self.delete_bucket(bucket_name)
            
        except Exception as e:
            logger.error(f"Cleanup process failed: {e}")
            return False


def main():
    """Main function to parse arguments and execute cleanup."""
    parser = argparse.ArgumentParser(
        description="Clean up MinIO bucket by deleting all objects and the bucket itself",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--endpoint',
        required=True,
        help='MinIO server endpoint (e.g., localhost:9000)'
    )
    
    parser.add_argument(
        '--access-key',
        required=True,
        help='MinIO access key'
    )
    
    parser.add_argument(
        '--secret-key',
        required=True,
        help='MinIO secret key'
    )
    
    parser.add_argument(
        '--bucket',
        required=True,
        nargs='+',
        action='append',
        help='Name(s) of the bucket(s) to cleanup (supports multiple --bucket groups)'
    )
    
    parser.add_argument(
        '--secure',
        action='store_true',
        default=True,
        help='Use HTTPS (default: True)'
    )
    
    parser.add_argument(
        '--insecure',
        dest='secure',
        action='store_false',
        help='Use HTTP instead of HTTPS'
    )
    
    parser.add_argument(
        '--region',
        help='AWS region (optional)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List objects without deleting (for testing)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=10,
        help='Maximum number of parallel workers for deletion (default: 10)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()

    # Flatten buckets in case multiple --bucket groups are provided
    # Supports both: --bucket a b c  and  --bucket a --bucket b c
    bucket_args = []
    for group in args.bucket:
        # Each group is a list from one --bucket occurrence
        bucket_args.extend(group if isinstance(group, list) else [group])
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Create cleaner instance
        cleaner = MinIOBucketCleaner(
            endpoint=args.endpoint,
            access_key=args.access_key,
            secret_key=args.secret_key,
            secure=args.secure,
            region=args.region
        )
        
        # Perform cleanup for each bucket
        start_time = time.time()
        all_success = True
        
        for bucket_name in bucket_args:
            logger.info(f"Processing bucket: {bucket_name}")
            success = cleaner.cleanup_bucket(
                bucket_name=bucket_name,
                dry_run=args.dry_run,
                max_workers=args.max_workers
            )
            
            if not success:
                all_success = False
                logger.error(f"Failed to cleanup bucket: {bucket_name}")
        
        end_time = time.time()
        
        if all_success:
            logger.info(f"All bucket cleanups completed successfully in {end_time - start_time:.2f} seconds")
            sys.exit(0)
        else:
            logger.error("Some bucket cleanups failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

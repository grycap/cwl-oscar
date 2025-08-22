# MinIO Bucket Cleanup Scripts

This repository contains scripts to recursively delete all objects in a MinIO bucket and then delete the bucket itself. The scripts provide both Python and shell script interfaces for maximum flexibility.

## ⚠️ WARNING

**These scripts will permanently delete ALL data in the specified bucket and then delete the bucket itself. This action cannot be undone. Use with extreme caution!**

## Features

- **Recursive deletion**: Deletes all objects in the bucket, including nested directories
- **Multiple bucket support**: Clean up multiple buckets in a single command
- **Parallel processing**: Uses multiple worker threads for faster deletion
- **Safety features**: Confirmation prompts and dry-run mode
- **Comprehensive logging**: Detailed logs saved to file and console
- **Error handling**: Robust error handling with detailed error messages
- **Flexible configuration**: Support for different endpoints, regions, and security settings

## Prerequisites

- Python 3.6 or higher
- MinIO Python client library
- Access to a MinIO service with appropriate permissions

## Installation

1. **Clone or download the scripts** to your local machine
2. **Install Python dependencies**:
   ```bash
   pip3 install -r minio_requirements.txt
   ```
   
   Or install manually:
   ```bash
   pip3 install minio
   ```

## Usage

### Multiple Bucket Support

The scripts now support cleaning up multiple buckets in a single command. This is useful for:
- Batch cleanup operations
- Cleaning up related buckets
- Development environment cleanup
- Testing multiple scenarios

**Examples:**
```bash
# Clean up multiple buckets
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b dev-bucket test-bucket staging-bucket

# Dry run with multiple buckets
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b bucket1 bucket2 -d
```

**Note:** When cleaning up multiple buckets, the script processes them sequentially and reports success/failure for each bucket individually.

### Option 1: Shell Script (Recommended)

The shell script provides a user-friendly interface with colored output and safety confirmations.

#### Basic Usage

```bash
./minio_cleanup.sh -e <endpoint> -a <access-key> -s <secret-key> -b <bucket-name>
```

#### Examples

**Local MinIO with default credentials:**
```bash
./minio_cleanup.sh -e localhost:9000 -a minioadmin -s minioadmin -b my-bucket
```

**Remote MinIO with HTTPS:**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a myuser -s mypassword -b production-data -r us-east-1
```

**Multiple buckets:**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b bucket1 bucket2 bucket3
```

**Use HTTP instead of HTTPS:**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket -i
```

**Dry run (see what would be deleted without actually deleting):**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket -d
```

**Dry run with multiple buckets:**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b bucket1 bucket2 -d
```

**Custom number of parallel workers:**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket -w 20
```

**Verbose logging:**
```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b my-bucket -v
```

#### Shell Script Options

| Option | Long Option | Description |
|--------|-------------|-------------|
| `-e` | `--endpoint` | MinIO server endpoint (e.g., localhost:9000 or https://minio.example.com:9000) |
| `-a` | `--access-key` | MinIO access key |
| `-s` | `--secret-key` | MinIO secret key |
| `-b` | `--bucket` | Name of the bucket to cleanup |
| `-r` | `--region` | AWS region (optional) |
| `-i` | `--insecure` | Use HTTP instead of HTTPS |
| `-d` | `--dry-run` | List objects without deleting (for testing) |
| `-w` | `--workers` | Maximum number of parallel workers (default: 10) |
| `-v` | `--verbose` | Enable verbose logging |
| `-h` | `--help` | Show help message |

### Option 2: Python Script Directly

You can also run the Python script directly for more control:

```bash
python3 minio_bucket_cleanup.py --endpoint <endpoint> --access-key <access-key> --secret-key <secret-key> --bucket <bucket-name>
```

#### Python Script Options

| Option | Description |
|--------|-------------|
| `--endpoint` | MinIO server endpoint (required) |
| `--access-key` | MinIO access key (required) |
| `--secret-key` | MinIO secret key (required) |
| `--bucket` | Name of the bucket to cleanup (required) |
| `--secure` | Use HTTPS (default: True) |
| `--insecure` | Use HTTP instead of HTTPS |
| `--region` | AWS region (optional) |
| `--dry-run` | List objects without deleting |
| `--max-workers` | Maximum number of parallel workers (default: 10) |
| `--verbose` | Enable verbose logging |
| `--help` | Show help message |

## Examples

### Example 1: Clean up a development bucket

```bash
./minio_cleanup.sh -e localhost:9000 -a minioadmin -s minioadmin -b dev-data
```

### Example 2: Clean up a production bucket with confirmation

```bash
./minio_cleanup.sh -e https://prod-minio.company.com:9000 -a produser -s prodpass -b user-uploads -r us-west-2
```

### Example 3: Test what would be deleted (dry run)

```bash
./minio_cleanup.sh -e https://minio.example.com:9000 -a minioadmin -s minioadmin -b test-bucket -d
```

### Example 4: Use HTTP for local development

```bash
./minio_cleanup.sh -e http://localhost:9000 -a minioadmin -s minioadmin -b local-data
```

## Safety Features

1. **Confirmation Prompt**: The script asks for confirmation before proceeding with deletion
2. **Dry Run Mode**: Use `-d` or `--dry-run` to see what would be deleted without actually deleting
3. **Detailed Logging**: All operations are logged to both console and file (`minio_cleanup.log`)
4. **Error Handling**: Comprehensive error handling with detailed error messages
5. **Progress Reporting**: Shows progress during deletion operations

## Logging

The script creates a log file `minio_cleanup.log` in the current directory with detailed information about all operations. This includes:

- Connection attempts
- Object discovery
- Deletion progress
- Error messages
- Completion status

## Performance

- **Parallel Processing**: Uses multiple worker threads for faster deletion
- **Configurable Workers**: Adjust the number of parallel workers with `-w` option
- **Batch Operations**: Efficiently handles large numbers of objects
- **Progress Reporting**: Shows progress every 100 deleted objects

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Check endpoint URL and port
   - Verify network connectivity
   - Check firewall settings

2. **Authentication Failed**
   - Verify access key and secret key
   - Check if credentials have appropriate permissions

3. **Bucket Not Found**
   - Verify bucket name spelling
   - Check if bucket exists
   - Verify user has access to the bucket

4. **Permission Denied**
   - Ensure user has delete permissions on objects
   - Ensure user has delete permissions on the bucket

### Debug Mode

Use the `-v` or `--verbose` flag for detailed logging:

```bash
./minio_cleanup.sh -e localhost:9000 -a minioadmin -s minioadmin -b my-bucket -v
```

## Security Considerations

1. **Credentials**: Never hardcode credentials in scripts
2. **Network**: Use HTTPS in production environments
3. **Permissions**: Use least-privilege access for cleanup operations
4. **Audit**: Review logs after cleanup operations

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve these scripts.

## License

This project is open source. Please check the repository for specific licensing information.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the logs for error details
3. Verify your MinIO configuration and permissions
4. Test with a dry run first using the `-d` flag

# Docker GitHub Action Setup

This document explains how to set up and use the GitHub Action for building and pushing the cwl-oscar Docker image to Docker Hub.

## Required Secrets

To use this GitHub Action, you need to add the following secrets to your GitHub repository:

### Setting up Docker Hub Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Add the following repository secrets:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `DOCKER_HUB_USERNAME` | Your Docker Hub username | `robertbio` |
| `DOCKER_HUB_ACCESS_TOKEN` | Docker Hub access token (not password!) | `dckr_pat_...` |

### Creating a Docker Hub Access Token

1. Log in to [Docker Hub](https://hub.docker.com/)
2. Go to **Account Settings** → **Security**
3. Click **New Access Token**
4. Give it a descriptive name (e.g., "GitHub Actions cwl-oscar")
5. Select appropriate permissions (Read, Write, Delete for your repositories)
6. Copy the generated token and use it as `DOCKER_HUB_ACCESS_TOKEN`

## Workflow Triggers

The GitHub Action is triggered in the following scenarios:

### 1. Manual Trigger (workflow_dispatch)
- Can be triggered manually from the GitHub Actions tab
- Allows customization of:
  - **Registry**: Override the default registry (default: `robertbio`)
  - **Push to registry**: Choose whether to push after building (default: `true`)

### 2. Release Trigger
- Automatically triggered when a new release is published
- Always pushes to the default registry (`robertbio`)

### 3. Main Branch Push
- Automatically triggered on pushes to the `main` branch
- Only triggers if changes are made to:
  - `cwl_oscar/**` (source code)
  - `Dockerfile`
  - `requirements.txt`
  - `utils/docker-run.sh`

## How It Works

The workflow uses the existing `utils/docker-run.sh` script with the `build-push-multi` command, which:

1. **Updates build info** with current timestamp and git revision
2. **Creates multi-platform builder** using Docker Buildx
3. **Builds for multiple platforms**: `linux/amd64` and `linux/arm64`
4. **Pushes to registry** (if enabled)
5. **Runs tests** to verify the image works correctly

## Usage Examples

### Manual Build and Push
1. Go to the **Actions** tab in your GitHub repository
2. Select **Build and Push Docker Image**
3. Click **Run workflow**
4. Optionally customize:
   - Registry (default: `robertbio`)
   - Whether to push to registry (default: `true`)

### Build Only (No Push)
Useful for testing builds without publishing:
1. Trigger manually with **Push to registry** set to `false`

### Custom Registry
To push to a different registry:
1. Trigger manually with **Registry** set to your desired registry (e.g., `mycompany`)
2. Make sure you have the appropriate Docker Hub credentials

## Build Output

The workflow provides:
- **Multi-platform support**: Images for both AMD64 and ARM64 architectures
- **Testing**: Validates the built image works correctly
- **Build summary**: Shows registry, platforms, and pull command
- **Security scanning**: Optional security check (if script exists)

## Image Tags

The built images are tagged as:
- `{registry}/cwl-oscar:latest` (multi-platform manifest)
- `{registry}/cwl-oscar:amd64` (AMD64 specific)
- `{registry}/cwl-oscar:arm64` (ARM64 specific)

## Troubleshooting

### Authentication Issues
- Verify `DOCKER_HUB_USERNAME` matches your Docker Hub username exactly
- Ensure `DOCKER_HUB_ACCESS_TOKEN` is a valid access token (not password)
- Check that the access token has sufficient permissions

### Build Failures
- Check the workflow logs for specific error messages
- Verify the `utils/docker-run.sh` script is working locally
- Ensure all required files are present in the repository

### Registry Issues
- Verify you have push permissions to the specified registry
- For custom registries, ensure the registry name is correct
- Check Docker Hub rate limits if builds are failing frequently

## Local Testing

Before relying on the GitHub Action, test the build process locally:

```bash
# Test the build script (run from project root)
./utils/docker-run.sh build-push-multi

# Test with custom registry
./utils/docker-run.sh --registry myregistry build-push-multi

# Test build only (no push)
./utils/docker-run.sh build-multi
```

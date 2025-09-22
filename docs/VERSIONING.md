# Dynamic Versioning with setuptools-scm

This project uses [setuptools-scm](https://setuptools-scm.readthedocs.io/) to automatically determine the package version based on Git tags. This ensures consistency between Git releases and PyPI package versions.

## How It Works

### Version Generation
- **Release versions**: When you create a Git tag like `v1.2.3`, the package version becomes `1.2.3`
- **Development versions**: Between releases, versions like `1.2.4.dev5+g1234abcd.d20250915` are generated
  - `1.2.4` = next version after the latest tag
  - `dev5` = 5 commits since the last tag
  - `g1234abcd` = Git commit hash
  - `d20250915` = date of the commit

### Configuration
The versioning is configured in `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=80", "setuptools-scm>=8", "wheel"]
build-backend = "setuptools.build_meta"

[project]
dynamic = ["version"]  # Version is determined dynamically

[tool.setuptools_scm]
version_file = "cwl_oscar/_version.py"  # Generated version file (replaces deprecated write_to)
```

### Version Access in Code
The version is accessible in multiple ways:

```python
# Primary method (uses generated _version.py)
from cwl_oscar import __version__
print(__version__)

# Fallback method (uses package metadata)
from importlib.metadata import version
print(version("cwl-oscar"))

# Version info with build details
from cwl_oscar import get_version_info
info = get_version_info()
print(f"Version: {info['version']}")
```

## Release Process

### Creating a Release
1. **Commit all changes** to the repository
2. **Create and push a Git tag**:
   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```
3. **Create a GitHub release** from the tag
4. **GitHub Actions automatically**:
   - Builds the package with the correct version
   - Publishes to PyPI

### GitHub Actions Integration
The workflow in `.github/workflows/python-publish.yml` handles:
- Fetching sufficient Git history (`fetch-depth: 50`) for setuptools-scm to find tags
- Building packages with automatic version detection
- Publishing to PyPI

**Note**: 
- setuptools-scm handles versioning automatically
- Using `fetch-depth: 50` instead of `fetch-depth: 0` provides better performance while ensuring tag accessibility
- For repositories with very long histories, you may need to adjust the depth or use `fetch-depth: 0`

## Development Workflow

### Local Development
During development, the version will show as a development version:
```
0.2.0.dev3+g9f80fea7c.d20250915
```

This indicates:
- Based on tag `v0.1.0` (becomes `0.2.0` for next version)
- 3 commits since the tag
- Current commit hash `9f80fea7c`
- Date `20250915`

### Testing Version Generation
You can test what version would be generated:
```bash
python -c "import setuptools_scm; print(setuptools_scm.get_version())"
```

### Building Packages
When building packages, setuptools-scm automatically:
1. Generates `cwl_oscar/_version.py` with version info
2. Sets the package version in metadata
3. Includes version in wheel filename

## File Structure

### Generated Files (Git-ignored)
- `cwl_oscar/_version.py` - Auto-generated version file
- `build/` - Build artifacts
- `dist/` - Distribution packages
- `*.egg-info/` - Package metadata

### Version-Related Files
- `pyproject.toml` - Build configuration with setuptools-scm setup
- `cwl_oscar/__init__.py` - Version import logic with fallbacks
- `.gitignore` - Excludes generated version file
- `.github/workflows/python-publish.yml` - Release automation

## Benefits

1. **Automatic Versioning**: No manual version updates needed
2. **Consistency**: Git tags and package versions always match
3. **Development Tracking**: Clear development versions between releases
4. **PEP 440 Compliance**: Proper Python version strings
5. **Build Integration**: Works with modern Python build tools

The approach follows Python packaging best practices.

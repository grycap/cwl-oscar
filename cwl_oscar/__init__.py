"""CWL OSCAR Executor - Execute CWL workflows on OSCAR clusters."""

__version__ = "0.1.0"

# Build information - can be updated by build scripts
__build_time__ = "unknown"
__git_revision__ = "unknown"

def get_version_info():
    """Get version information."""
    import os
    import datetime
    
    # Try to read build info from file (created by build process)
    build_info_file = os.path.join(os.path.dirname(__file__), '.build_info')
    if os.path.exists(build_info_file):
        try:
            with open(build_info_file, 'r') as f:
                lines = f.read().strip().split('\n')
                info = {}
                for line in lines:
                    if '=' in line:
                        key, value = line.split('=', 1)
                        info[key.strip()] = value.strip()
                return {
                    'version': __version__,
                    'build_time': info.get('BUILD_TIME', __build_time__),
                    'git_revision': info.get('GIT_REVISION', __git_revision__)
                }
        except Exception:
            pass
    
    # Fallback to module constants
    return {
        'version': __version__,
        'build_time': __build_time__,
        'git_revision': __git_revision__
    } 
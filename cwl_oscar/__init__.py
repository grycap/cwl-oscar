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

"""CWL OSCAR Executor - Execute CWL workflows on OSCAR clusters."""

import os
from importlib.metadata import version, PackageNotFoundError

# * Try to get version from setuptools-scm generated file
try:
    from ._version import version as __version__
except ImportError:
    # ! Fallback: try to get version from package metadata
    try:
        __version__ = version("cwl-oscar")
    except PackageNotFoundError:
        # ! Final fallback for development
        __version__ = "0.1.0-dev"

# Build information - can be updated by build scripts
__build_time__ = "unknown"
__git_revision__ = "unknown"

def get_version_info():
    """Get version information."""
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

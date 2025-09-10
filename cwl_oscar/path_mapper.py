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

"""OSCAR Path Mapper for execution path mapping."""

import logging

from cwltool.pathmapper import PathMapper, MapperEnt

try:
    from constants import *
except ImportError:
    # Fallback for package import
    from .constants import *

log = logging.getLogger("oscar-backend")


class OSCARPathMapper(PathMapper):
    """Path mapper for OSCAR execution - maps local paths to mount paths."""
    
    def __init__(self, referenced_files, basedir, stagedir, separateDirs, mount_path=None, **kwargs):
        # Extract mount_path from kwargs if provided, or use default
        self.mount_path = mount_path or DEFAULT_MOUNT_PATH
        super(OSCARPathMapper, self).__init__(referenced_files, basedir, stagedir, separateDirs, **kwargs)
        
    def setup(self, referenced_files, basedir):
        """Set up path mappings for OSCAR execution."""
        # Call parent setup first to handle the standard path mapping
        super(OSCARPathMapper, self).setup(referenced_files, basedir)
        
        # Apply OSCAR-specific path mappings
        # For files already in mount path, use direct mount path access instead of staging
        for key in list(self._pathmap.keys()):
            entry = self._pathmap[key]
            if hasattr(entry, 'resolved') and entry.resolved:
                resolved_path = entry.resolved
                
                # If file is already in the mount path, use it directly without staging
                if self.mount_path in resolved_path:
                    log.debug("File already in mount path, using direct access: %s", resolved_path)
                    # Use the mount path directly - no staging needed
                    self._pathmap[key] = MapperEnt(
                        resolved=resolved_path,
                        target=resolved_path,  # Use same path as target
                        type=entry.type,
                        staged=False  # Don't stage - file is already accessible
                    )
                    log.debug("Direct mount path mapping: %s -> %s", resolved_path, resolved_path)

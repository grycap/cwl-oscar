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

"""OSCAR Command Line Tool implementation."""

from cwltool.command_line_tool import CommandLineTool

try:
    from path_mapper import OSCARPathMapper
    from task import OSCARTask
except ImportError:
    # Fallback for package import
    from .path_mapper import OSCARPathMapper
    from .task import OSCARTask


class OSCARCommandLineTool(CommandLineTool):
    """OSCAR-specific CommandLineTool implementation."""
    
    def __init__(self, toolpath_object, loading_context, cluster_manager, mount_path, service_name, shared_minio_config=None):
        super(OSCARCommandLineTool, self).__init__(toolpath_object, loading_context)
        self.cluster_manager = cluster_manager
        self.mount_path = mount_path
        self.service_name = service_name
        self.shared_minio_config = shared_minio_config
        
        # We'll create service managers dynamically for each cluster as needed
        
    def make_path_mapper(self, reffiles, stagedir, runtimeContext, separateDirs):
        """Create a path mapper for OSCAR execution."""
        return OSCARPathMapper(
            reffiles, runtimeContext.basedir, stagedir, separateDirs, mount_path=self.mount_path)
            
    def make_job_runner(self, runtimeContext):
        """Create an OSCAR job runner."""
        def create_oscar_task(builder, joborder, make_path_mapper, requirements, hints, name):
            return OSCARTask(
                builder,
                joborder,
                make_path_mapper,
                requirements,
                hints,
                name,
                self.cluster_manager,
                self.mount_path,
                self.service_name,
                runtimeContext,
                tool_spec=self.tool,  # Pass tool specification
                shared_minio_config=self.shared_minio_config
            )
        return create_oscar_task

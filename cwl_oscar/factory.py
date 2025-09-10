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

"""OSCAR Tool Factory for CWL Process generation."""

from cwltool.workflow import default_make_tool

try:
    from command_line_tool import OSCARCommandLineTool
except ImportError:
    # Fallback for package import
    from .command_line_tool import OSCARCommandLineTool


def make_oscar_tool(spec, loading_context, cluster_manager, mount_path, service_name, shared_minio_config=None):
    """cwl-oscar specific factory for CWL Process generation."""
    if "class" in spec and spec["class"] == "CommandLineTool":
        # Pass None as service_name since it will be determined dynamically
        return OSCARCommandLineTool(spec, loading_context, cluster_manager, mount_path, None, shared_minio_config)
    else:
        return default_make_tool(spec, loading_context)

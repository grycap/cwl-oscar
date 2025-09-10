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

"""OSCAR-specific implementation for CWL execution."""

# Import all the split modules
from .service_manager import OSCARServiceManager
from .executor import OSCARExecutor
from .task import OSCARTask
from .command_line_tool import OSCARCommandLineTool
from .path_mapper import OSCARPathMapper
from .factory import make_oscar_tool
from .context_utils import suppress_stdout_to_stderr

# Export public API for backward compatibility
__all__ = [
    'make_oscar_tool',
    'OSCARPathMapper',
    'OSCARServiceManager',
    'OSCARExecutor',
    'OSCARTask',
    'OSCARCommandLineTool',
    'suppress_stdout_to_stderr',
] 
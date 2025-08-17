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
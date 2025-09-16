"""Context utilities for OSCAR operations."""

import contextlib
import sys


@contextlib.contextmanager
def suppress_stdout_to_stderr():
    """Context manager to redirect stdout to stderr during oscar-python operations.
    
    This prevents oscar-python library messages from contaminating the JSON output.
    """
    original_stdout = sys.stdout
    try:
        sys.stdout = sys.stderr
        yield
    finally:
        sys.stdout = original_stdout

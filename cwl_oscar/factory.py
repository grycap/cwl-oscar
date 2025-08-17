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

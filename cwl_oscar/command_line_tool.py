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

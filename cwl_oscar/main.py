"""Main entrypoint for cwl-oscar."""
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import os
import functools
import signal
import sys
import logging
from typing import MutableMapping, MutableSequence
from typing_extensions import Text

import pkg_resources
from typing import Any, Dict, Tuple, Optional
import cwltool.main
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.executors import (MultithreadedJobExecutor, SingleJobExecutor,
                               JobExecutor)
from cwltool.process import Process

from .oscar import make_oscar_tool, OSCARPathMapper
from .cluster_manager import ClusterManager
from .__init__ import get_version_info

log = logging.getLogger("oscar-backend")
log.setLevel(logging.INFO)
# Always use stderr for logging to keep stdout clean for JSON output
console = logging.StreamHandler(sys.stderr)
log.addHandler(console)

DEFAULT_TMP_PREFIX = "tmp"
DEFAULT_MOUNT_PATH = "/mnt/cwl2o-data/mount"


def versionstring():
    """Determine our version."""
    pkg = pkg_resources.require("cwltool")
    if pkg:
        cwltool_ver = pkg[0].version
    else:
        cwltool_ver = "unknown"
    
    version_info = get_version_info()
    return "%s %s (built: %s) with cwltool %s" % (
        sys.argv[0], 
        version_info['version'], 
        version_info['build_time'], 
        cwltool_ver
    )


def main(args=None):
    """Main entrypoint for cwl-oscar."""
    if args is None:
        args = sys.argv[1:]

    parser = arg_parser()
    parsed_args = parser.parse_args(args)

    # Log version information at startup
    log.info("Starting %s", versionstring())

    if parsed_args.version:
        print(versionstring())
        return 0

    # Initialize cluster manager
    cluster_manager = ClusterManager()
    
    # Handle new multi-cluster arguments
    if parsed_args.cluster_endpoint:
        log.info("Processing multi-cluster configuration")
        
        endpoint_count = len(parsed_args.cluster_endpoint)
        
        # The current argparse approach with action='append' doesn't preserve the relationship
        # between endpoints and their authentication methods. We need to parse the raw arguments
        # to understand which authentication method goes with which endpoint.
        
        # Get the raw argument list to understand the grouping
        raw_args = sys.argv[1:] if args is None else args
        log.debug("Raw arguments: %s", raw_args)
        
        # Parse arguments manually to understand the grouping
        clusters = []
        current_cluster = None
        
        i = 0
        while i < len(raw_args):
            arg = raw_args[i]
            
            if arg == '--cluster-endpoint':
                if current_cluster:
                    clusters.append(current_cluster)
                current_cluster = {'endpoint': raw_args[i + 1], 'token': None, 'username': None, 'password': None, 'ssl': True, 'steps': []}
                i += 2
            elif arg == '--cluster-token':
                if current_cluster:
                    current_cluster['token'] = raw_args[i + 1]
                    i += 2
                else:
                    print(versionstring(), file=sys.stderr)
                    parser.print_usage(sys.stderr)
                    print("cwl-oscar: error: --cluster-token must follow --cluster-endpoint", file=sys.stderr)
                    return 1
            elif arg == '--cluster-username':
                if current_cluster:
                    current_cluster['username'] = raw_args[i + 1]
                    i += 2
                else:
                    print(versionstring(), file=sys.stderr)
                    parser.print_usage(sys.stderr)
                    print("cwl-oscar: error: --cluster-username must follow --cluster-endpoint", file=sys.stderr)
                    return 1
            elif arg == '--cluster-password':
                if current_cluster:
                    current_cluster['password'] = raw_args[i + 1]
                    i += 2
                else:
                    print(versionstring(), file=sys.stderr)
                    parser.print_usage(sys.stderr)
                    print("cwl-oscar: error: --cluster-password must follow --cluster-endpoint", file=sys.stderr)
                    return 1
            elif arg == '--cluster-disable-ssl':
                if current_cluster:
                    current_cluster['ssl'] = False
                    i += 1
                else:
                    print(versionstring(), file=sys.stderr)
                    parser.print_usage(sys.stderr)
                    print("cwl-oscar: error: --cluster-disable-ssl must follow --cluster-endpoint", file=sys.stderr)
                    return 1
            elif arg == '--cluster-steps':
                if current_cluster:
                    # Parse comma-separated steps and add to the cluster
                    steps_str = raw_args[i + 1]
                    steps = [step.strip() for step in steps_str.split(',') if step.strip()]
                    current_cluster['steps'].extend(steps)
                    i += 2
                else:
                    print(versionstring(), file=sys.stderr)
                    parser.print_usage(sys.stderr)
                    print("cwl-oscar: error: --cluster-steps must follow --cluster-endpoint", file=sys.stderr)
                    return 1
            elif arg.startswith('--shared-minio') or arg.startswith('--mount-path') or arg.startswith('--outdir') or arg.startswith('--') or not arg.startswith('-'):
                # Skip non-cluster arguments
                i += 1
            else:
                i += 1
        
        # Add the last cluster
        if current_cluster:
            clusters.append(current_cluster)
        
        log.debug("Parsed clusters: %s", clusters)
        
        # Validate and add each cluster
        for i, cluster in enumerate(clusters):
            endpoint = cluster['endpoint']
            token = cluster['token']
            username = cluster['username']
            password = cluster['password']
            ssl = cluster['ssl']
            steps = cluster['steps']
            
            # Validate authentication for this cluster
            if not token and not username:
                print(versionstring(), file=sys.stderr)
                parser.print_usage(sys.stderr)
                print(f"cwl-oscar: error: cluster {i+1} requires either --cluster-token or --cluster-username", file=sys.stderr)
                return 1
                
            if username and not password:
                print(versionstring(), file=sys.stderr)
                parser.print_usage(sys.stderr)
                print(f"cwl-oscar: error: cluster {i+1} needs --cluster-password when using --cluster-username", file=sys.stderr)
                return 1
            
            # Add cluster to manager with steps
            cluster_manager.add_cluster_from_args(endpoint, token, username, password, ssl, steps)
            auth_method = "token" if token else "username/password"
            steps_info = f" (steps: {', '.join(steps)})" if steps else ""
            log.info("Added cluster %d: %s (%s)%s", i+1, endpoint, auth_method, steps_info)
    
    else:
        print(versionstring(), file=sys.stderr)
        parser.print_usage(sys.stderr)
        print("cwl-oscar: error: --cluster-endpoint is required (at least one cluster must be specified)", file=sys.stderr)
        return 1
    
    # Validate cluster configurations
    if not cluster_manager.validate_clusters():
        return 1
    
    # Log cluster information
    cluster_info = cluster_manager.get_cluster_info()
    log.info("Configured %d clusters:", len(cluster_info))
    for info in cluster_info:
        log.info("  %s: %s (%s)", info['name'], info['endpoint'], info['auth_type'])

    # Check shared MinIO configuration for multi-cluster
    shared_minio_config = None
    if len(cluster_info) > 1:
        # Multi-cluster mode requires shared MinIO bucket
        if not parsed_args.shared_minio_endpoint:
            print(versionstring(), file=sys.stderr)
            parser.print_usage(sys.stderr)
            print("cwl-oscar: error: --shared-minio-endpoint is required for multi-cluster mode", file=sys.stderr)
            return 1
        
        if not parsed_args.shared_minio_access_key or not parsed_args.shared_minio_secret_key:
            print(versionstring(), file=sys.stderr)
            parser.print_usage(sys.stderr)
            print("cwl-oscar: error: --shared-minio-access-key and --shared-minio-secret-key are required for multi-cluster mode", file=sys.stderr)
            return 1
        
        shared_minio_config = {
            'endpoint': parsed_args.shared_minio_endpoint,
            'access_key': parsed_args.shared_minio_access_key,
            'secret_key': parsed_args.shared_minio_secret_key,
            'region': parsed_args.shared_minio_region,
            'verify_ssl': not parsed_args.shared_minio_disable_ssl
        }
        log.info("Shared MinIO bucket configured: %s", parsed_args.shared_minio_endpoint)
    else:
        log.info("Single cluster mode - using default cluster MinIO bucket")

    # Configure logging levels based on existing quiet/debug options
    if hasattr(parsed_args, 'quiet') and parsed_args.quiet:
        log.setLevel(logging.WARNING)
    elif hasattr(parsed_args, 'debug') and parsed_args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    def signal_handler(*args):  # pylint: disable=unused-argument
        """setup signal handler"""
        log.info("received control-c signal")
        log.info("terminating thread(s)...")
        log.warning("remote OSCAR task(s) will keep running")
        sys.exit(1)
    signal.signal(signal.SIGINT, signal_handler)

    loading_context = cwltool.main.LoadingContext(vars(parsed_args))
    loading_context.construct_tool_object = functools.partial(
        make_oscar_tool, 
        cluster_manager=cluster_manager,
        mount_path=parsed_args.mount_path,
        service_name=parsed_args.service_name,
        shared_minio_config=shared_minio_config
    )
    
    runtime_context = cwltool.main.RuntimeContext(vars(parsed_args))
    runtime_context.path_mapper = functools.partial(
        OSCARPathMapper, mount_path=parsed_args.mount_path
    )
    
    job_executor = MultithreadedJobExecutor() if parsed_args.parallel \
        else SingleJobExecutor()
    # Set reasonable limits for cores and RAM
    job_executor.max_ram = 8 * 1024 * 1024 * 1024  # 8GB in bytes
    job_executor.max_cores = 4  # 4 CPU cores
    
    executor = functools.partial(
        oscar_execute, 
        job_executor=job_executor,
        loading_context=loading_context,
        cluster_manager=cluster_manager,
        mount_path=parsed_args.mount_path,
        service_name=parsed_args.service_name,
        shared_minio_config=shared_minio_config
    )
    
    # * Disable ANSI color codes in cwltool logging to fix log output
    os.environ['NO_COLOR'] = '1'  # Standard environment variable to disable colors
    os.environ['FORCE_COLOR'] = '0'  # Disable forced colors
    
    # Configure cwltool to not use colors by setting up a plain formatter
    try:
        from cwltool import loghandler as cwltool_loghandler
        # Create a plain formatter without colors
        plain_formatter = logging.Formatter('%(levelname)s %(message)s')
        if hasattr(cwltool_loghandler, 'defaultStreamHandler'):
            cwltool_loghandler.defaultStreamHandler.setFormatter(plain_formatter)
    except (ImportError, AttributeError):
        # If cwltool structure is different, the environment variables should still work
        pass
    
    return cwltool.main.main(
        args=parsed_args,
        executor=executor,
        loadingContext=loading_context,
        runtimeContext=runtime_context,
        versionfunc=versionstring,
        logger_handler=console
    )


def oscar_execute(process,           # type: Process
                  job_order,         # type: Dict[Text, Any]
                  runtime_context,   # type: RuntimeContext
                  job_executor,      # type: JobExecutor
                  loading_context,   # type: LoadingContext
                  cluster_manager,
                  mount_path,
                  service_name,
                  shared_minio_config,
                  logger=log
                  ):  # type: (...) -> Tuple[Optional[Dict[Text, Any]], Text]
    """Execute using OSCAR backend."""
    if not job_executor:
        job_executor = MultithreadedJobExecutor()
    return job_executor(process, job_order, runtime_context, logger)


def arg_parser():  # type: () -> argparse.ArgumentParser
    """Create argument parser for cwl-oscar."""
    parser = argparse.ArgumentParser(
        description='OSCAR executor for Common Workflow Language.')
    
    # Multi-cluster support
    parser.add_argument("--cluster-endpoint", type=str, action='append',
                        help="OSCAR cluster endpoint URL (can be specified multiple times for multiple clusters)")
    parser.add_argument("--cluster-token", type=str, action='append',
                        help="OSCAR OIDC authentication token for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-username", type=str, action='append',
                        help="OSCAR username for basic authentication for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-password", type=str, action='append',
                        help="OSCAR password for basic authentication for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-disable-ssl", action='append', const=True, nargs='?',
                        help="Disable SSL verification for corresponding cluster (can be specified multiple times)")
    parser.add_argument("--cluster-steps", type=str, action='append',
                        help="Comma-separated list of workflow steps to execute on corresponding cluster (can be specified multiple times)")
    
    # Shared MinIO bucket configuration for multi-cluster support
    parser.add_argument("--shared-minio-endpoint", type=str,
                        help="Shared MinIO endpoint URL for multi-cluster support (all clusters will use this bucket)")
    parser.add_argument("--shared-minio-access-key", type=str,
                        help="Shared MinIO access key for multi-cluster support")
    parser.add_argument("--shared-minio-secret-key", type=str,
                        help="Shared MinIO secret key for multi-cluster support")
    parser.add_argument("--shared-minio-region", type=str,
                        help="Shared MinIO region for multi-cluster support")
    parser.add_argument("--shared-minio-disable-ssl", action="store_true", default=False,
                        help="Disable SSL certificate verification for shared MinIO")
    
    parser.add_argument("--mount-path", type=str,
                        default=DEFAULT_MOUNT_PATH,
                        help="Mount path for shared data")
    parser.add_argument("--service-name", type=str,
                        default="run-script-event2",
                        help="OSCAR service name to use for execution")
    
    # Standard cwltool arguments
    parser.add_argument("--basedir", type=Text)
    parser.add_argument("--outdir",
                        type=Text, default=os.path.abspath('.'),
                        help="Output directory, default current directory")
    
    envgroup = parser.add_mutually_exclusive_group()
    envgroup.add_argument(
        "--preserve-environment",
        type=Text,
        action="append",
        help="Preserve specific environment variable when "
        "running CommandLineTools.  May be provided multiple "
        "times.",
        metavar="ENVVAR",
        default=[],
        dest="preserve_environment")
    envgroup.add_argument(
        "--preserve-entire-environment",
        action="store_true",
        help="Preserve all environment variable when running "
        "CommandLineTools.",
        default=False,
        dest="preserve_entire_environment")

    parser.add_argument("--tmpdir-prefix", type=Text,
                        help="Path prefix for temporary directories",
                        default=DEFAULT_TMP_PREFIX)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--tmp-outdir-prefix",
        type=Text,
        help="Path prefix for intermediate output directories",
        default=DEFAULT_TMP_PREFIX)

    exgroup.add_argument(
        "--cachedir",
        type=Text,
        default="",
        help="Directory to cache intermediate workflow outputs to avoid "
        "recomputing steps."
    )

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--rm-tmpdir",
        action="store_true",
        default=True,
        help="Delete intermediate temporary directories (default)",
        dest="rm_tmpdir")

    exgroup.add_argument(
        "--leave-tmpdir",
        action="store_false",
        default=True,
        help="Do not delete intermediate temporary directories",
        dest="rm_tmpdir")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--move-outputs",
        action="store_const",
        const="move",
        default="move",
        help="Move output files to the workflow output directory and delete "
        "intermediate output directories (default).",
        dest="move_outputs")

    exgroup.add_argument(
        "--leave-outputs",
        action="store_const",
        const="leave",
        default="move",
        help="Leave output files in intermediate output directories.",
        dest="move_outputs")

    exgroup.add_argument(
        "--copy-outputs",
        action="store_const",
        const="copy",
        default="move",
        help="Copy output files to the workflow output directory, don't "
        "delete intermediate output directories.",
        dest="move_outputs")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--verbose", action="store_true", help="Default logging")
    exgroup.add_argument(
        "--quiet",
        action="store_true",
        help="Only print warnings and errors.")
    exgroup.add_argument(
        "--debug",
        action="store_true",
        help="Print even more logging")

    parser.add_argument(
        "--timestamps",
        action="store_true",
        help="Add timestamps to the errors, warnings, and notifications.")

    parser.add_argument(
        "--tool-help",
        action="store_true",
        help="Print command line help for tool")

    parser.add_argument(
        "--default-container",
        help="Specify a default docker container that will be used if the "
        "workflow fails to specify one.")
    parser.add_argument("--disable-validate", dest="do_validate",
                        action="store_false", default=True,
                        help=argparse.SUPPRESS)

    parser.add_argument(
        "--on-error",
        help="Desired workflow behavior when a step fails. "
        "One of 'stop' or 'continue'. Default is 'stop'.",
        default="stop",
        choices=("stop", "continue"))

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--compute-checksum",
        action="store_true",
        default=True,
        help="Compute checksum of contents while collecting outputs",
        dest="compute_checksum")
    exgroup.add_argument(
        "--no-compute-checksum",
        action="store_false",
        help="Do not compute checksum of contents while collecting outputs",
        dest="compute_checksum")

    parser.add_argument(
        "--relax-path-checks",
        action="store_true",
        default=False,
        help="Relax requirements on path names to permit "
        "spaces and hash characters.",
        dest="relax_path_checks")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--parallel", action="store_true", default=True,
        help="Run jobs in parallel (the default)")
    exgroup.add_argument(
        "--serial", action="store_false", dest="parallel",
        help="Run jobs serially")

    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit")



    parser.add_argument(
        "workflow",
        type=Text,
        nargs="?",
        default=None,
        metavar='cwl_document',
        help="path or URL to a CWL Workflow, "
        "CommandLineTool, or ExpressionTool. If the `inputs_object` has a "
        "`cwl:tool` field indicating the path or URL to the cwl_document, "
        " then the `workflow` argument is optional.")
    parser.add_argument(
        "job_order",
        nargs=argparse.REMAINDER,
        metavar='inputs_object',
        help="path or URL to a YAML or JSON "
        "formatted description of the required input values for the given "
        "`cwl_document`.")

    return parser


if __name__ == "__main__":
    sys.exit(main())
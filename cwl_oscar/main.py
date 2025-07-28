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
from .__init__ import __version__

# Build time constant - update this string as needed
BUILD_TIME = "2024-01-15 10:30:00 UTC"

log = logging.getLogger("oscar-backend")
log.setLevel(logging.INFO)
# Always use stderr for logging to keep stdout clean for JSON output
console = logging.StreamHandler(sys.stderr)
log.addHandler(console)

DEFAULT_TMP_PREFIX = "tmp"
DEFAULT_OSCAR_ENDPOINT = ""
DEFAULT_MOUNT_PATH = "/mnt/cwl2o-data/mount"


def versionstring():
    """Determine our version."""
    pkg = pkg_resources.require("cwltool")
    if pkg:
        cwltool_ver = pkg[0].version
    else:
        cwltool_ver = "unknown"
    return "%s %s (built: %s) with cwltool %s" % (sys.argv[0], __version__, BUILD_TIME, cwltool_ver)


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

    if parsed_args.oscar_endpoint is None:
        print(versionstring(), file=sys.stderr)
        parser.print_usage(sys.stderr)
        print("cwl-oscar: error: argument --oscar-endpoint is required", file=sys.stderr)
        return 1

    # Check authentication parameters
    if parsed_args.oscar_token is None and parsed_args.oscar_username is None:
        print(versionstring(), file=sys.stderr)
        parser.print_usage(sys.stderr)
        print("cwl-oscar: error: either --oscar-token or --oscar-username is required", file=sys.stderr)
        return 1
    
    if parsed_args.oscar_username is not None and parsed_args.oscar_password is None:
        print(versionstring(), file=sys.stderr)
        parser.print_usage(sys.stderr)
        print("cwl-oscar: error: --oscar-password is required when using --oscar-username", file=sys.stderr)
        return 1

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
        oscar_endpoint=parsed_args.oscar_endpoint,
        oscar_token=parsed_args.oscar_token,
        oscar_username=parsed_args.oscar_username,
        oscar_password=parsed_args.oscar_password,
        mount_path=parsed_args.mount_path,
        service_name=parsed_args.service_name,
        ssl=not parsed_args.disable_ssl
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
        oscar_endpoint=parsed_args.oscar_endpoint,
        oscar_token=parsed_args.oscar_token,
        oscar_username=parsed_args.oscar_username,
        oscar_password=parsed_args.oscar_password,
        mount_path=parsed_args.mount_path,
        service_name=parsed_args.service_name
    )
    
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
                  oscar_endpoint,
                  oscar_token,
                  oscar_username,
                  oscar_password,
                  mount_path,
                  service_name,
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
    
    # OSCAR-specific arguments
    parser.add_argument("--oscar-endpoint", type=str, 
                        default=DEFAULT_OSCAR_ENDPOINT,
                        help="OSCAR cluster endpoint URL")
    
    # Authentication options - either token or username/password
    auth_group = parser.add_mutually_exclusive_group()
    auth_group.add_argument("--oscar-token", type=str,
                           help="OSCAR OIDC authentication token")
    auth_group.add_argument("--oscar-username", type=str,
                           help="OSCAR username for basic authentication")
    
    parser.add_argument("--oscar-password", type=str,
                        help="OSCAR password for basic authentication (required if --oscar-username is used)")
    
    parser.add_argument("--mount-path", type=str,
                        default=DEFAULT_MOUNT_PATH,
                        help="Mount path for shared data")
    parser.add_argument("--service-name", type=str,
                        default="run-script-event2",
                        help="OSCAR service name to use for execution")
    
    parser.add_argument("--disable-ssl", 
                        action="store_true",
                        help="Disable verification of SSL certificates for the cluster service")
    
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
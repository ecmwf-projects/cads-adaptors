"""
MARS adaptor for CDS data retrieval.

IMPORTANT ARCHITECTURAL NOTE:
This module is imported by multiple services:
1. Data retrieval workers - actually execute MARS requests
2. API services - use adaptors for constraint validation, mapping, schema
3. Catalogue services - metadata and configuration management

To avoid forcing ALL services to have cads-mars-server installed, we follow
these import guidelines:

1. Module-level imports: Only cads_adaptors dependencies and standard library
2. Configuration: Read from environment variables (no cads-mars-server dependency)
3. Client imports: Encapsulated INSIDE execute_mars_pipe() and execute_mars_shares()
   - from cads_mars_server import client (pipe mode)
   - from cads_mars_server.ws_client import mars_via_ws_sync (shares mode)

This way, services that only validate constraints or manage mappings can import
this module without needing the full MARS client infrastructure installed.
"""

import os
import pathlib
import time
from typing import Any, BinaryIO
from urllib.parse import urlparse, urlunparse

from packaging import version

from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.exceptions import (
    MarsNoDataError,
    MarsRuntimeError,
    MarsSystemError,
)
from cads_adaptors.tools import adaptor_tools
from cads_adaptors.tools.date_tools import implement_embargo
from cads_adaptors.tools.general import (
    ensure_list,
    split_requests_on_keys,
)
from cads_adaptors.tools.simulate_preinterpolation import simulate_preinterpolation

# Configuration for MARS client selection and ports
# These are read from environment variables to avoid forcing a dependency on
# cads-mars-server for services that only use adaptors for constraints/mappings.
# The actual client imports (mars_client, ws_client) are deferred until needed
# inside execute_mars_pipe() and execute_mars_shares().
DEFAULT_PIPE_PORT = int(os.getenv("MARS_PIPE_PORT", "9000"))
DEFAULT_SHARES_PORT = int(os.getenv("MARS_SHARES_PORT", "9001"))


def _check_cads_mars_server_supports_websocket() -> bool:
    """
    Check if cads-mars-server is installed and supports WebSocket mode.
    
    WebSocket mode requires cads-mars-server >= 0.3.0.
    
    This function handles:
    - Module not installed (returns False)
    - Released versions (e.g., "0.3.0", "0.3.1")
    - Development versions from branches (e.g., "0.3.0.dev1+g1234567")
    - Missing __version__ attribute (returns False)
    
    For development versions, we check the base version (major.minor.patch)
    ignoring pre-release and local version identifiers. This ensures that
    builds from the websocketmars branch (which will have 0.3.0.devN versions)
    are recognized as supporting WebSocket features.
    
    Environment variable override:
    - Set MARS_FORCE_WEBSOCKET_MODE=true to bypass version checking
      (useful for testing with development installations)
    
    Returns:
        True if cads-mars-server >= 0.3.0 is available, False otherwise
    """
    # Check for force override (for testing/development)
    if os.getenv("MARS_FORCE_WEBSOCKET_MODE", "false").lower() == "true":
        return True
    
    try:
        import cads_mars_server
        
        # Check if __version__ attribute exists
        if not hasattr(cads_mars_server, "__version__"):
            # Missing version info - assume old version, fall back to pipe
            return False
        
        installed_version = version.parse(cads_mars_server.__version__)
        required_version = version.parse("0.3.0")
        
        # For development versions like "0.3.0.dev1+g1234567", the base_version
        # gives us "0.3.0" which we can compare. This ensures branch builds
        # are recognized as having the features.
        if hasattr(installed_version, "base_version"):
            base = version.parse(installed_version.base_version)
            return base >= required_version
        else:
            # Fallback for older packaging versions
            return installed_version >= required_version
            
    except ImportError:
        # cads-mars-server not installed
        return False
    except (AttributeError, ValueError, TypeError) as e:
        # AttributeError: __version__ doesn't exist (caught above, but defensive)
        # ValueError: version string can't be parsed
        # TypeError: __version__ is not a string
        return False
    except Exception:
        # Any other unexpected error - fall back to pipe mode
        return False


def _should_use_websocket_mode() -> bool:
    """
    Determine if WebSocket mode should be used based on configuration and version.
    
    WebSocket mode is enabled when BOTH conditions are met:
    1. MARS_USE_SHARES environment variable is set to "true"
    2. cads-mars-server >= 0.3.0 is installed
    
    This ensures backward compatibility: older cads-mars-server versions will
    fall back to pipe mode even if MARS_USE_SHARES=true is set.
    
    Returns:
        True if WebSocket mode should be used, False for pipe mode
    """
    use_shares_env = os.getenv("MARS_USE_SHARES", "false").lower() == "true"
    
    if not use_shares_env:
        # MARS_USE_SHARES not set or false - use pipe mode
        return False
    
    # MARS_USE_SHARES is true - check if server supports WebSocket
    if not _check_cads_mars_server_supports_websocket():
        # WebSocket requested but not supported - log warning and fall back
        # Note: This will be logged in execute_mars() when context is available
        return False
    
    return True


# Determine at module load time which mode to use
# This value is read by execute_mars() to select the client
USE_SHARES = _should_use_websocket_mode()

# This hard requirement of MARS requests should be moved to the proxy MARS client
ALWAYS_SPLIT_ON: list[str] = [
    "class",
    "type",
    "stream",
    "levtype",
    "expver",
    "domain",
    "system",
    "method",
    "origin",
]


def get_mars_server_list(config) -> list[str]:
    if config.get("mars_servers") is not None:
        return ensure_list(config["mars_servers"])

    # TODO: Refactor when we have a more stable set of mars-servers
    if os.getenv("MARS_API_SERVER_LIST") is not None:
        default_mars_server_list = os.getenv("MARS_API_SERVER_LIST")
    else:
        for default_mars_server_list in [
            "/etc/mars/mars-api-server-legacy.list",
            "/etc/mars/mars-api-server.list",
        ]:
            if os.path.exists(default_mars_server_list):
                break

    mars_server_list: str = config.get("mars_server_list", default_mars_server_list)
    if os.path.exists(mars_server_list):
        with open(mars_server_list) as f:
            mars_servers = f.read().splitlines()
    else:
        raise MarsSystemError(
            "MARS servers cannot be found, this is an error at the system level."
        )
    return mars_servers

def get_mars_server_list_ws(config) -> list[str]:
    """
    Convert HTTP pipe server URLs to WebSocket shares server URLs.
    
    Properly parses URLs and converts:
    - http:// -> ws://
    - https:// -> wss://
    - port 9000 -> port 9001 (or configured ports)
    
    Args:
        config: Configuration dictionary
        
    Returns:
        List of WebSocket server URLs
    """
    http_servers = get_mars_server_list(config)
    ws_servers = []
    
    for server in http_servers:
        parsed = urlparse(server)
        
        # Convert HTTP scheme to WebSocket scheme
        scheme = "wss" if parsed.scheme == "https" else "ws"
        
        # Convert port if it matches the default pipe port
        netloc = parsed.netloc
        if parsed.port == DEFAULT_PIPE_PORT:
            netloc = netloc.replace(
                f":{DEFAULT_PIPE_PORT}", f":{DEFAULT_SHARES_PORT}"
            )
        elif parsed.port is None:
            # No port specified, add the shares port
            netloc = f"{parsed.hostname}:{DEFAULT_SHARES_PORT}"
        
        ws_url = urlunparse((scheme, netloc, parsed.path, "", "", ""))
        ws_servers.append(ws_url)
    
    return ws_servers

def minimal_mars_schema(
    allow_duplicate_values_keys=None,
    remove_duplicate_values=False,
    key_regex=None,
    value_regex=None,
):
    """A minimal schema that ensures all values are lists of strings. Also
    ensures non-post-processing keys don't contain duplicate values.
    """
    # Regular expressions for valid keys and values. The one_char_minimum regex
    # matches any number of non-whitespace and space characters as long as there
    # is at least one non-whitespace.
    one_char_minimum = r"[\S ]*\S[\S ]*"
    whitespace = r"[ \t]*"
    key_regex = key_regex or rf"^{whitespace}{one_char_minimum}{whitespace}\Z"
    value_regex = value_regex or rf"^{whitespace}{one_char_minimum}{whitespace}\Z"

    # These are the only keys permitted to have duplicate values. Duplicate
    # values for field-selection keys sometimes leads to MARS rejecting the
    # request but other times can result in duplicate output fields which can
    # cause downstream problems. Manuel advises not to rely on specific MARS
    # behaviour when given duplicate values so we reject them in advance.
    postproc_keys = ["grid", "area"] + (allow_duplicate_values_keys or [])

    # Form a regex that matches any key that will not be interpreted as one of
    # postproc_keys by MARS. i.e. a case-insensitive regex that matches anything
    # other than optional whitespace followed by a sequence of characters whose
    # lead characters match lead characters of one of postproc_keys. This
    # complexity is required because MARS will interpret all of the following as
    # area: "ArEa", "areaXYZ", "are", "arefoo".
    same_lead_chars = [
        "(".join(list(k)) + "".join([")?"] * (len(k) - 1)) for k in postproc_keys
    ]
    not_postproc_key = r"(?i)^(?!\s*(" + "|".join(same_lead_chars) + "))"

    # Minimal schema
    schema = {
        "_draft": "7",
        "allOf": [  # All following schemas must match.
            # Basic requirements for all keys
            {
                "type": "object",  # Item is a dict
                "minProperties": 1,  # ...with at least 1 key
                "patternProperties": {
                    key_regex: {  # ...with names matching this
                        "type": "array",  # ...must hold lists
                        "minItems": 1,  # ...of at least 1 item
                        "items": {
                            "type": "string",  # ...which are strings
                            "pattern": value_regex,  # ...matching this regex
                            "_onErrorShowPattern": False,  # (error msg control)
                        },
                    }
                },
                "additionalProperties": False,  # ...with no non-matching keys
                "_onErrorShowPattern": False,  # (error msg control)
            },
            # Additional requirement for some keys
            {
                "type": "object",  # Item is a dict
                "patternProperties": {
                    not_postproc_key: {  # ...in which non post-processing keys
                        "type": "array",
                        "uniqueItems": True,  # ...containing duplicates
                        # ... are rejected or have duplicates removed
                        "_noRemoveDuplicates": not remove_duplicate_values,
                    }
                },
            },
        ],
    }

    return schema


def make_env_dict(config: dict[str, Any]) -> dict[str, Any]:
    # Add required fields to the env dictionary:
    env = {
        "user_id": config.get("user_uid"),
        "request_id": config.get("request_uid"),
        "namespace": (
            f"{os.getenv('OPENSTACK_PROJECT', 'NO-OSPROJECT')}:"
            f"{os.getenv('RUNTIME_NAMESPACE', 'NO-NAMESPACE')}"
        ),
        "host": os.getenv("HOSTNAME"),
    }
    env["username"] = str(env["namespace"]) + ":" + str(env["user_id"]).split("-")[-1]
    return env


def _mars_common_output(target, requests, reply, reply_message, context, time0):
    delta_time = time.time() - time0
    if os.path.exists(target):
        filesize = os.path.getsize(target)
        context.info(
            f"The MARS Request produced a target "
            f"(filesize={filesize / 1024 ** 2} MB, delta_time= {delta_time:.2f} seconds).",
            delta_time=delta_time,
            filesize=filesize,
        )
    else:
        filesize = 0
        context.info(
            f"The MARS request produced no target (delta_time= {delta_time:.2f} seconds).",
            delta_time=delta_time,
        )

    context.debug(message=reply_message)

    if reply.error:
        error_lines = "\n".join(
            [message for message in reply_message.split("\n") if "ERROR" in message]
        )
        error_message = (
            "MARS has returned an error, please check your selection.\n"
            f"Request submitted to the MARS server:\n{requests}\n"
            f"Full error message:\n{error_lines}\n"
        )
        context.add_user_visible_error(message=error_message)

        error_message += f"Exception: {reply.error}\n"
        raise MarsRuntimeError(error_message)

    if not filesize:
        error_message = (
            "MARS returned no data, please check your selection."
            f"Request submitted to the MARS server:\n{requests}\n"
        )
        context.add_user_visible_error(
            message=error_message,
        )
        raise MarsNoDataError(error_message)


def _prepare_mars_request(
    request: dict[str, Any] | list[dict[str, Any]],
    context: Context,
    config: dict[str, Any],
    mapping: dict[str, Any],
    target_fname: str,
    target_dir: str | pathlib.Path,
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    """
    Common preparation logic for both pipe and shares MARS implementations.
    
    Args:
        request: Single request dict or list of request dicts
        context: Context object for logging
        config: Configuration dictionary
        mapping: Mapping dictionary for field transformations
        target_fname: Target filename
        target_dir: Target directory path
        
    Returns:
        Tuple of (processed_requests, target_path, environment_dict)
    """
    requests = ensure_list(request)
    
    # Implement embargo if configured
    # Note: This is also done in normalise_request, but kept here for adaptors
    # that may not use normalise_request yet. Running twice is safe.
    if config.get("embargo") is not None:
        requests, _cacheable = implement_embargo(requests, config["embargo"])

    target = str(pathlib.Path(target_dir) / target_fname)

    split_on_keys = ALWAYS_SPLIT_ON + ensure_list(config.get("split_on", []))
    requests = split_requests_on_keys(requests, split_on_keys, context, mapping)

    # Add required fields to the env dictionary
    env = make_env_dict(config)
    
    return requests, target, env


def execute_mars_pipe(
    request: dict[str, Any] | list[dict[str, Any]],
    context: Context = Context(),
    config: dict[str, Any] = dict(),
    mapping: dict[str, Any] = dict(),
    target_fname: str = "data.grib",
    target_dir: str | pathlib.Path = "",
) -> str:
    """
    Execute MARS request using pipe-based client.
    
    Args:
        request: MARS request(s)
        context: Context for logging
        config: Configuration dictionary
        mapping: Field mapping dictionary
        target_fname: Output filename
        target_dir: Output directory
        
    Returns:
        Path to output file
    """
    from cads_mars_server import client as mars_client

    requests, target, env = _prepare_mars_request(
        request, context, config, mapping, target_fname, target_dir
    )

    mars_servers = get_mars_server_list(config)
    cluster = mars_client.RemoteMarsClientCluster(urls=mars_servers, log=context)

    time0 = time.time()
    context.info(f"Request sent to proxy MARS client: {requests}")
    reply = cluster.execute(requests, env, target)
    reply_message = str(reply.message)

    _mars_common_output(target, requests, reply, reply_message, context, time0)

    return target


def execute_mars_shares(
    request: dict[str, Any] | list[dict[str, Any]],
    context: Context = Context(),
    config: dict[str, Any] = dict(),
    mapping: dict[str, Any] = dict(),
    target_fname: str = "data.grib",
    target_dir: str | pathlib.Path = "",
    log_handler: Any = None,
) -> str:
    """
    Execute MARS request using WebSocket-based shares client.
    
    This implementation uses shared filesystem access, where the server
    writes directly to the shared filesystem and the client monitors progress.
    
    Args:
        request: MARS request(s)
        context: Context for logging
        config: Configuration dictionary
        mapping: Field mapping dictionary
        target_fname: Output filename
        target_dir: Output directory
        log_handler: Optional custom log handler for parsing MARS output.
                     Handler receives (line, ws, logger) and returns formatted
                     line or None to suppress. Can raise exceptions to abort.
        
    Returns:
        Path to output file
    """
    from cads_mars_server.ws_client import mars_via_ws_sync as mars_client

    requests, target, env = _prepare_mars_request(
        request, context, config, mapping, target_fname, target_dir
    )

    # Ensure target directory is accessible by MARS server
    if target_dir:
        os.chmod(target_dir, 0o777)

    mars_servers = get_mars_server_list_ws(config)

    time0 = time.time()
    context.info(f"Request(s) sent to proxy MARS client: {requests}")

    reply = mars_client(
        mars_servers,
        requests,
        env,
        target=str(target),
        logger=context,
        log_handler=log_handler,
    )

    reply_message = str(reply.message)

    _mars_common_output(target, requests, reply, reply_message, context, time0)

    return target

def execute_mars(
    request: dict[str, Any] | list[dict[str, Any]],
    context: Context = Context(),
    config: dict[str, Any] = dict(),
    mapping: dict[str, Any] = dict(),
    target_fname: str = "data.grib",
    target_dir: str | pathlib.Path = "",
    log_handler: Any = None,
) -> str:
    """
    Execute MARS request using the configured client (pipe or shares).
    
    The client selection requires BOTH conditions:
    1. MARS_USE_SHARES environment variable set to "true"
    2. cads-mars-server >= 0.3.0 installed
    
    If MARS_USE_SHARES is true but cads-mars-server < 0.3.0, falls back to pipe mode
    with a warning. This ensures backward compatibility with older deployments.
    
    Args:
        request: MARS request(s)
        context: Context for logging
        config: Configuration dictionary
        mapping: Field mapping dictionary
        target_fname: Output filename
        target_dir: Output directory
        log_handler: Optional custom log handler for parsing MARS output.
                     Handler receives (line, ws, logger) and returns formatted
                     line or None to suppress. Can raise exceptions to abort.
                     Only used when USE_SHARES=True (WebSocket client).
        
    Returns:
        Path to output file
    """
    # Check if WebSocket was requested but not available
    use_shares_env = os.getenv("MARS_USE_SHARES", "false").lower() == "true"
    
    if USE_SHARES:
        # Get version info for logging
        force_mode = os.getenv("MARS_FORCE_WEBSOCKET_MODE", "false").lower() == "true"
        
        try:
            import cads_mars_server
            if hasattr(cads_mars_server, "__version__"):
                version_str = cads_mars_server.__version__
                mode_info = f"cads-mars-server {version_str}"
            else:
                mode_info = "cads-mars-server (version unknown)"
            
            if force_mode:
                mode_info += " [FORCED MODE]"
            
            context.info(
                f"Using MARS Shares (WebSocket) client for MARS retrievals ({mode_info})."
            )
        except ImportError:
            mode_info = "FORCED MODE" if force_mode else ""
            context.info(f"Using MARS Shares (WebSocket) client for MARS retrievals. {mode_info}".strip())
        
        return execute_mars_shares(
            request,
            context=context,
            config=config,
            mapping=mapping,
            target_fname=target_fname,
            target_dir=target_dir,
            log_handler=log_handler,
        )
    else:
        # Log reason for using pipe mode
        if use_shares_env:
            # MARS_USE_SHARES was set but we're using pipe mode - explain why
            try:
                import cads_mars_server
                version_str = cads_mars_server.__version__
                context.add_user_visible_log(
                    f"WebSocket mode requested (MARS_USE_SHARES=true) but cads-mars-server "
                    f"version {version_str} < 0.3.0. Falling back to pipe mode. "
                    f"Upgrade to cads-mars-server >= 0.3.0 to use WebSocket features."
                )
            except ImportError:
                context.add_user_visible_log(
                    "WebSocket mode requested (MARS_USE_SHARES=true) but cads-mars-server "
                    "is not installed. Falling back to pipe mode."
                )
        
        context.info("Using MARS Pipe client for MARS retrievals.")
        return execute_mars_pipe(
            request,
            context=context,
            config=config,
            mapping=mapping,
            target_fname=target_fname,
            target_dir=target_dir,
        )

class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        result = execute_mars(
            request,
            context=self.context,
            target_dir=self.cache_tmp_path,
        )
        return open(result, "rb")


class MarsCdsAdaptor(cds.AbstractCdsAdaptor):
    def __init__(self, *args, **config) -> None:
        super().__init__(*args, **config)
        schema_options = config.get("schema_options", {})
        if not schema_options.get("disable_adaptor_schema"):
            self.adaptor_schema = minimal_mars_schema(**schema_options)

    def convert_format(self, *args, **kwargs):
        from cads_adaptors.tools.convertors import convert_format

        return convert_format(*args, **kwargs)

    def daily_reduce(self, *args, **kwargs) -> dict[str, Any]:
        from cads_adaptors.tools.post_processors import daily_reduce

        kwargs.setdefault("context", self.context)
        return daily_reduce(*args, **kwargs)

    def monthly_reduce(self, *args, **kwargs) -> dict[str, Any]:
        from cads_adaptors.tools.post_processors import monthly_reduce

        kwargs.setdefault("context", self.context)
        return monthly_reduce(*args, **kwargs)

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """Implemented in normalise_request, before the mapping is applied."""
        request = super().pre_mapping_modifications(request)

        if "format" in request:
            self.context.add_user_visible_error(
                "The 'format' key for requests is deprecated, please use 'data_format' instead. "
                "Use of 'format' is no longer part of the system testing, "
                "therefore it is not guaranteed to work."
            )
        # Remove "format" from request if it exists
        data_format = request.pop("format", ["grib"])
        data_format = adaptor_tools.handle_data_format(
            request.get("data_format", data_format)
        )

        # Account from some horribleness from the legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            data_format = "netcdf"
            request.setdefault("download_format", ["zip"])

        # Enforce value of data_format to normalized value
        request["data_format"] = [data_format]

        default_download_format = "as_source"
        download_format = ensure_list(
            request.pop("download_format", default_download_format)
        )[0]
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        # Perform actions necessary to simulate pre-interpolation of fields to
        # a regular grid?
        if cfg := self.config.get("simulate_preinterpolation"):
            request = simulate_preinterpolation(request, cfg, self.context)

        return request

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        # Call normalise_request to set self.mapped_requests
        request = self.normalise_request(request)

        # Get data_format from the list of mapped_requests, performs an additional
        # check that only one data_format is present across all mapped_requests,
        # and ensures a normalised value.
        mapped_requests, data_format = (
            adaptor_tools.get_data_format_from_mapped_requests(self.mapped_requests)
        )

        result = execute_mars(
            mapped_requests,
            context=self.context,
            config=self.config,
            mapping=self.mapping,
            target_dir=self.cache_tmp_path,
        )

        results_dict = self.post_process(result)

        # TODO?: Generalise format conversion to be a post-processor
        paths = self.convert_format(
            results_dict,
            data_format,
            context=self.context,
            config=self.config,
            target_dir=str(self.cache_tmp_path),
        )

        # A check to ensure that if there is more than one path, and download_format
        #  is as_source, we over-ride and zip up the files
        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return paths

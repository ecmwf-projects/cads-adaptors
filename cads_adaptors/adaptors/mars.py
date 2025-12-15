import os
import pathlib
import time
from typing import Any, BinaryIO

from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.exceptions import (
    CdsConfigError,
    MarsNoDataError,
    MarsRuntimeError,
    MarsSystemError,
)
from cads_adaptors.tools.adaptor_tools import handle_data_format
from cads_adaptors.tools.date_tools import implement_embargo
from cads_adaptors.tools.general import (
    ensure_list,
    split_requests_on_keys,
)

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


def execute_mars(
    request: dict[str, Any] | list[dict[str, Any]],
    context: Context = Context(),
    config: dict[str, Any] = dict(),
    mapping: dict[str, Any] = dict(),
    target_fname: str = "data.grib",
    target_dir: str | pathlib.Path = "",
) -> str:
    from cads_mars_server import client as mars_client

    requests = ensure_list(request)
    # Implement embargo if it is set in the config
    # This is now done in normalize request, but leaving it here for now, as running twice is not a problem
    #  and the some adaptors may not use normalise_request yet
    if config.get("embargo") is not None:
        requests, _cacheable = implement_embargo(requests, config["embargo"])

    target = str(pathlib.Path(target_dir) / target_fname)

    split_on_keys = ALWAYS_SPLIT_ON + ensure_list(config.get("split_on", []))
    requests = split_requests_on_keys(requests, split_on_keys, context, mapping)

    mars_servers = get_mars_server_list(config)

    cluster = mars_client.RemoteMarsClientCluster(urls=mars_servers, log=context)

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
    time0 = time.time()
    context.info(f"Request sent to proxy MARS client: {requests}")
    reply = cluster.execute(requests, env, target)
    reply_message = str(reply.message)
    delta_time = time.time() - time0
    if os.path.exists(target):
        filesize = os.path.getsize(target)
        context.info(
            f"The MARS Request produced a target "
            f"(filesize={filesize * 1e-6} Mb, delta_time= {delta_time:.2f} seconds).",
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

    return target


def minimal_mars_schema(
    allow_duplicate_values_keys=None,
    remove_duplicate_values=False,
    extra_key_chars=None,
    extra_value_chars=None,
):
    """A minimal schema required for the adaptor code to work and a syntactically
    valid MARS request to be formed. In future this should perhaps be made less
    minimal in order to do a bit more tidying up of the request. Value splitting on
    slashes could also be done in order to detect duplicate values when in
    slash-separated form.
    """
    # Negative look-ahead assertion used to reject any string containing a
    # newline, equal sign or comma in a key or value. These have special meaning
    # to MARS and can result in a syntactically invalid request, or can be used
    # to hack MARS.
    neg_assertion = r"(?!.*[\n=,].*$)"

    # Regular expressions for valid keys and values. Valid keys contain one or
    # more consecutive [a-zA-Z0-9_] characters. Valid values contain one or more
    # printable, non-whitespace characters (! to ~). (The real set of allowed
    # value characters is less than this but not hard-coded here as it's not
    # clearly documented.) Both can be bounded by any amount of whitespace as
    # this would not cause a MARS failure.
    extra_key_chars = extra_key_chars or ""
    extra_value_chars = extra_value_chars or ""
    ascii_word = (
        "a-zA-Z0-9_"  # This is more strict than \w, which includes some non-ascii
    )
    key_regex = rf"^{neg_assertion}[ \t]*[{ascii_word}{extra_key_chars}]+[ \t]*$"
    value_regex = rf"^{neg_assertion}[ \t]*[!-~{extra_value_chars}]+[ \t]*$"

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
    def __init__(self, *args, schema_options=None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.data_format: str | None = None
        schema_options = schema_options or {}
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
        data_format = request.pop("format", "grib")
        data_format = handle_data_format(request.get("data_format", data_format))

        # Account from some horribleness from the legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            data_format = "netcdf"
            request.setdefault("download_format", "zip")

        # Enforce value of data_format to normalized value
        request["data_format"] = data_format

        default_download_format = "as_source"
        download_format = request.pop("download_format", default_download_format)
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        return request

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        # Call normalise_request to set self.mapped_requests
        request = self.normalise_request(request)

        data_formats = [req.pop("data_format", None) for req in self.mapped_requests]
        data_formats = list(set(data_formats))
        if len(data_formats) != 1 or data_formats[0] is None:
            # It should not be possible to reach here, if it is, there is a problem.
            raise CdsConfigError(
                "Something has gone wrong in preparing your request, "
                "please try to submit your request again. "
                "If the problem persists, please contact user support."
            )
        self.data_format = data_formats[0]

        result = execute_mars(
            self.mapped_requests,
            context=self.context,
            config=self.config,
            mapping=self.mapping,
            target_dir=self.cache_tmp_path,
        )

        results_dict = self.post_process(result)

        # TODO?: Generalise format conversion to be a post-processor
        paths = self.convert_format(
            results_dict,
            self.data_format,
            context=self.context,
            config=self.config,
            target_dir=str(self.cache_tmp_path),
        )

        # A check to ensure that if there is more than one path, and download_format
        #  is as_source, we over-ride and zip up the files
        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return paths

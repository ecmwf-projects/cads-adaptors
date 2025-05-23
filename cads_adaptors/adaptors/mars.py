import os
import pathlib
import time
from typing import Any, BinaryIO

from cacholote.extra_encoders import InPlaceFile


from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.exceptions import MarsNoDataError, MarsRuntimeError, MarsSystemError
from cads_adaptors.tools import adaptor_tools
from cads_adaptors.tools.date_tools import implement_embargo
from cads_adaptors.tools.general import ensure_list, split_requests_on_keys

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

def download_file(url):
    import requests
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename


def execute_mars(
    request: dict[str, Any] | list[dict[str, Any]],
    context: Context = Context(),
    config: dict[str, Any] = dict(),
    mapping: dict[str, Any] = dict(),
    target_fname: str = "data.grib",
    target_dir: str = "",
) -> str:
    is_pipe = False
    if is_pipe:
        from cads_mars_server import client_pipe as mars_client
    else:
        from cads_mars_server import client_file as mars_client
        from cads_mars_server.config import local_target
    
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
    if is_pipe:
        context.debug(f"Pipe to {target}")
        cluster.execute(requests, env, target)
    else:
        context.add_user_visible_log(f'Requesting data from cads-mars-server on shared MARS cephfs ')
        reply = cluster.execute(requests, env)
        context.info(f'Request submitted {config.get("request_uid")} cached {reply.data is not None}')
        while reply.data is None:
            reply_message = str(reply.message)
            context.add_stdout(message=reply_message)
            if reply.data is None:
                time.sleep(1)
            reply = cluster.execute(requests, env)
        target = local_target(reply.data)
    reply_message = str(reply.message)
    delta_time = time.time() - time0
    # Check if the file exists
        
    if target.startswith("http"):
        _this_t0 = time.time()
        # If the target is a URL, we need to get the size from the headers
        ot = target
        try:
            target = download_file(target)
            context.info(f"Downloaded file from {ot} to {target} in {time.time() - _this_t0:.2f} seconds")
        except Exception as e:
            context.error(f"Failed to download file from {ot}: {e}")
            raise MarsRuntimeError(f"Failed to download file from {ot}: {e}")
    filesize = os.path.getsize(target)
        
    context.info(
        f"MARS Request complete. Filesize={filesize*1e-6} Mb, delta_time= {delta_time:.2f} seconds.",
        delta_time=delta_time,
        filesize=filesize,
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
        if filesize > 0:
            context.add_user_visible_error(message=f"Not rising this error { error_message } because the file exists")
        else:
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
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.data_format: str | None = None

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

        # TODO: Remove legacy syntax all together
        data_format = request.pop("format", "grib")
        data_format = request.pop("data_format", data_format)

        # Account from some horribleness from the legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            self.data_format = "netcdf"
            request.setdefault("download_format", "zip")

        default_download_format = "as_source"
        download_format = request.pop("download_format", default_download_format)
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        # Apply any mapping
        mapped_formats = self.apply_mapping({"data_format": data_format})
        # TODO: Add this extra mapping to apply_mapping?
        self.data_format = adaptor_tools.handle_data_format(
            mapped_formats["data_format"]
        )

        return request

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        import dask

        # Call normalise_request to set self.mapped_requests
        request = self.normalise_request(request)

        result = execute_mars(
            self.mapped_requests,
            context=self.context,
            config=self.config,
            mapping=self.mapping,
            target_dir=self.cache_tmp_path,
        )

        with dask.config.set(scheduler="threads"):
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

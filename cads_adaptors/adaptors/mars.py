import os
from typing import Any, BinaryIO, Union

from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.exceptions import MarsNoDataError, MarsRuntimeError, MarsSystemError
from cads_adaptors.tools import adaptor_tools
from cads_adaptors.tools.date_tools import implement_embargo
from cads_adaptors.tools.general import ensure_list


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
    request: Union[Request, list],
    context: Context,
    config: dict[str, Any] = dict(),
    target: str = "data.grib",
) -> str:
    from cads_mars_server import client as mars_client

    requests = ensure_list(request)
    if config.get("embargo") is not None:
        requests, _cacheable = implement_embargo(requests, config["embargo"])
    context.add_stdout(f"Request (after embargo implemented): {requests}")

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

    reply = cluster.execute(requests, env, target)
    reply_message = str(reply.message)
    context.add_stdout(message=reply_message)

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

    if not os.path.getsize(target):
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
        result = execute_mars(request, context=self.context)
        return open(result)  # type: ignore


class MarsCdsAdaptor(cds.AbstractCdsAdaptor):
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

    def retrieve(self, request: Request) -> BinaryIO:
        import dask

        # TODO: Remove legacy syntax all together
        data_format = request.pop("format", "grib")
        data_format = request.pop("data_format", data_format)
        data_format = adaptor_tools.handle_data_format(data_format)

        # Account from some horribleness from teh legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            data_format = "netcdf"
            request.setdefault("download_format", "zip")

        # To preserve existing ERA5 functionality the default download_format="as_source"
        self._pre_retrieve(request=request, default_download_format="as_source")

        result: Any = execute_mars(
            self.mapped_request, context=self.context, config=self.config
        )

        with dask.config.set(scheduler="threads"):
            result = self.post_process(result)

            # TODO?: Generalise format conversion to be a post-processor
            paths = self.convert_format(
                result,
                data_format,
                context=self.context,
                config=self.config,
            )

        # A check to ensure that if there is more than one path, and download_format
        #  is as_source, we over-ride and zip up the files
        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return self.make_download_object(paths)

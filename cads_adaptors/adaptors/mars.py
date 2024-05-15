import os
from typing import Any, BinaryIO, Union

from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.tools.date_tools import implement_embargo
from cads_adaptors.tools.general import ensure_list


def convert_format(
    result: str,
    data_format: str,
    context: Context,
    **kwargs,
) -> list:
    if isinstance(data_format, (list, tuple)):
        assert len(data_format) == 1, "Only one value of data_format is allowed"
        data_format = data_format[0]

    if data_format in ["netcdf4", "netcdf", "nc"]:
        to_netcdf_kwargs: dict[str, Any] = {}

        from cads_adaptors.tools.convertors import grib_to_netcdf_files

        # Give the power to overwrite the to_netcdf kwargs from the request
        to_netcdf_kwargs = {**to_netcdf_kwargs, **kwargs}
        try:
            paths = grib_to_netcdf_files(result, context=context, **to_netcdf_kwargs)
        except Exception as e:
            message = (
                "There was an error converting the GRIB data to netCDF.\n"
                "It may be that the selection you made was too complex, "
                "in which case you could try reducing your selection. "
                "For further help, or you believe this to be a problem with the dataset, "
                "please contact user support."
            )
            context.add_user_visible_error(message=message)
            context.add_stderr(message=f"Exception: {e}")
            raise e
    elif data_format in ["grib", "grib2", "grb", "grb2"]:
        paths = [result]
    else:
        message = "WARNING: Unrecoginsed data_format requested, returning as original grib/grib2 format"
        context.add_user_visible_log(message=message)
        context.add_stdout(message=message)
        paths = [result]
    return paths


def execute_mars(
    request: Union[Request, list],
    context: Context,
    config: dict[str, Any] = dict(),
    target: str = "data.grib",
    # mars_cmd: tuple[str, ...] = ("/usr/local/bin/mars", "r"),
    mars_server_list: str = os.getenv(
        "MARS_API_SERVER_LIST", "/etc/mars/mars-api-server.list"
    ),
) -> str:
    from cads_mars_server import client as mars_client

    requests = ensure_list(request)
    if config.get("embargo") is not None:
        requests, _cacheable = implement_embargo(requests, config["embargo"])
    context.add_stdout(f"Request (after embargo implemented): {requests}")

    if os.path.exists(mars_server_list):
        with open(mars_server_list) as f:
            mars_servers = f.read().splitlines()
    else:
        raise SystemError(
            "MARS servers cannot be found, this is an error at the system level."
        )

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
        raise RuntimeError(error_message)

    if not os.path.getsize(target):
        error_message = (
            "MARS returned no data, please check your selection."
            f"Request submitted to the MARS server:\n{requests}\n"
        )
        context.add_user_visible_error(
            message=error_message,
        )
        raise RuntimeError(error_message)

    return target


class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        result = execute_mars(request, context=self.context)
        return open(result)  # type: ignore


class MarsCdsAdaptor(cds.AbstractCdsAdaptor):
    def convert_format(self, *args, **kwargs):
        return convert_format(*args, **kwargs)

    def retrieve(self, request: Request) -> BinaryIO:
        # TODO: Remove legacy syntax all together
        data_format = request.pop("format", "grib")
        data_format = request.pop("data_format", data_format)

        # Account from some horribleness from teh legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            data_format = "netcdf"
            request.setdefault("download_format", "zip")

        # Allow user to provide format conversion kwargs
        convert_kwargs: dict[str, Any] = {
            **self.config.get("format_conversion_kwargs", dict()),
            **request.pop("format_conversion_kwargs", dict()),
        }

        # To preserve existing ERA5 functionality the default download_format="as_source"
        self._pre_retrieve(request=request, default_download_format="as_source")

        result = execute_mars(
            self.mapped_request, context=self.context, config=self.config
        )

        paths = self.convert_format(
            result, data_format, context=self.context, **convert_kwargs
        )

        # A check to ensure that if there is more than one path, and download_format
        #  is as_source, we over-ride and zip up the files
        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return self.make_download_object(paths)

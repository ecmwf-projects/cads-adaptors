import os
from typing import Any, BinaryIO, Union

from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.tools import adaptor_tools
from cads_adaptors.tools.date_tools import implement_embargo
from cads_adaptors.tools.general import ensure_list


def convert_format(
    result: Any,
    data_format: str,
    context: Context,
    current_result_format: str = "grib",
    open_datasets_kwargs: dict[str, Any] = dict(),
    **to_netcdf_kwargs,  # TODO: rename to something more generic
) -> list:
    data_format = adaptor_tools.handle_data_format(data_format)

    if data_format in ["netcdf"]:
        if current_result_format == "grib":
            from cads_adaptors.tools.convertors import grib_to_netcdf_files

            try:
                paths = grib_to_netcdf_files(
                    result,
                    context=context,
                    open_datasets_kwargs=open_datasets_kwargs,
                    **to_netcdf_kwargs,
                )
            except Exception as e:
                message = (
                    "There was an error converting the GRIB data to netCDF.\n"
                    "It may be that the selection you made was too large and/or complex, "
                    "in which case you could try reducing your selection. "
                    "For further help, or if you believe this to be a problem with the dataset, "
                    "please contact user support."
                )
                context.add_user_visible_error(message=message)
                context.add_stderr(message=f"Exception: {e}")
                raise e
        elif current_result_format in ["xarray"]:
            from cads_adaptors.tools.convertors import xarray_dict_to_netcdf

            paths = xarray_dict_to_netcdf(result, context=context, **to_netcdf_kwargs)

    elif data_format in ["grib"]:
        if current_result_format in ["xarray"]:
            message = (
                "WARNING: Unable to to returning post-processed data in grib format is not supported,"
                " returning in netcdf format"
            )
            from cads_adaptors.tools.convertors import xarray_dict_to_netcdf

            paths = xarray_dict_to_netcdf(result, context=context, **to_netcdf_kwargs)
        else:
            paths = [result]
    else:
        message = "WARNING: Unrecoginsed data_format requested, returning as original grib/grib2 format"
        context.add_user_visible_log(message=message)
        context.add_stdout(message=message)
        paths = [result]
    return paths


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
        raise SystemError(
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

    def pp_mapping(self, in_pp_config: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from cads_adaptors.tools.post_processors import pp_config_mapping

        pp_config = [
            pp_config_mapping(_pp_config) for _pp_config in ensure_list(in_pp_config)
        ]
        return pp_config

    def daily_statistics(self, *args, **kwargs) -> dict[str, Any]:
        from cads_adaptors.tools.post_processors import daily_statistics

        return daily_statistics(*args, context=self.context, **kwargs)

    def monthly_statistics(self, *args, **kwargs) -> dict[str, Any]:
        from cads_adaptors.tools.post_processors import monthly_statistics

        return monthly_statistics(*args, context=self.context, **kwargs)

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

        # TODO: deprecate this location for format_conversion_kwargs in the adaptor config
        convert_kwargs: dict[str, Any] = self.config.get(
            "format_conversion_kwargs", dict()
        )

        # Separate the open_datasets_kwargs from the to_netcdf_kwargs so they can be used in the correct place
        #  Preference allow for top level in daaset config:
        open_datasets_kwargs = {
            **convert_kwargs.pop("open_datasets_kwargs", dict()),
            **self.config.get("open_datasets_kwargs", dict()),
        }
        to_netcdf_kwargs = {
            **convert_kwargs.pop("to_netcdf_kwargs", dict()),
            **self.config.get("to_netcdf_kwargs", dict()),
        }

        post_process_steps: list[dict[str, Any]] = self.pp_mapping(
            request.pop("post_process", [])
        )

        # To preserve existing ERA5 functionality the default download_format="as_source"
        self._pre_retrieve(request=request, default_download_format="as_source")

        result = execute_mars(
            self.mapped_request, context=self.context, config=self.config
        )
        # Data returned from MARS is always in grib format
        current_result_format = "grib"

        with dask.config.set(scheduler="threads"):
            for pp_step in post_process_steps:
                _method = pp_step.pop("method")
                # TODO: Add extra condition to limit pps to datasets
                if _method is not None and not hasattr(self, _method):
                    self.context.add_user_visible_error(
                        message=f"Post-processor method '{_method}' not available for this dataset"
                    )
                    continue
                else:
                    method = getattr(self, _method)

                # post processing is done on xarray objects, so convert now if not already converted
                if current_result_format == "grib":
                    from cads_adaptors.tools.convertors import (
                        open_grib_file_as_xarray_dictionary,
                    )

                    result = open_grib_file_as_xarray_dictionary(
                        result,
                        open_datasets_kwargs=open_datasets_kwargs,
                        context=self.context,
                    )
                    current_result_format = "xarray"

                result = method(result, **pp_step)

            # TODO?: Generalise format conversion to be a post-processor
            paths = self.convert_format(
                result,
                data_format,
                context=self.context,
                current_result_format=current_result_format,
                open_datasets_kwargs=open_datasets_kwargs,
                **to_netcdf_kwargs,
            )

        # A check to ensure that if there is more than one path, and download_format
        #  is as_source, we over-ride and zip up the files
        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return self.make_download_object(paths)

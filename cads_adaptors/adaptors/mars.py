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

    # NOTE: The NetCDF compressed option will not be visible on the WebPortal, it is here for testing
    if data_format in ["netcdf", "nc", "netcdf_compressed"]:
        if data_format in ["netcdf_compressed"]:
            to_netcdf_kwargs = {
                "compression_options": "default",
            }
        else:
            to_netcdf_kwargs = {}
        from cads_adaptors.tools.convertors import grib_to_netcdf_files

        # Give the power to overwrite the to_netcdf kwargs from the request
        to_netcdf_kwargs = {**to_netcdf_kwargs, **kwargs}
        paths = grib_to_netcdf_files(result, **to_netcdf_kwargs)
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
    mars_cmd: tuple[str, ...] = ("/usr/local/bin/mars", "r"),
) -> str:
    import subprocess

    requests = ensure_list(request)
    if config.get("embargo") is not None:
        requests, _cacheable = implement_embargo(requests, config["embargo"])
    context.add_stdout(f"{requests}")

    with open("r", "w") as fp:
        for i, req in enumerate(requests):
            print("retrieve", file=fp)
            # Add target file to first request, any extra store in same grib
            if i == 0:
                print(f", target={target}", file=fp)
            for key, value in req.items():
                if not isinstance(value, (list, tuple)):
                    value = [value]
                print(f", {key}={'/'.join(str(v) for v in value)}", file=fp)

    env = dict(**os.environ)
    # FIXME: set with the namespace and user_id
    namespace = "cads"
    user_id = 0
    env["MARS_USER"] = f"{namespace}-{user_id}"

    popen = subprocess.Popen(
        mars_cmd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    popen.wait()
    if popen.stdout and (stdout := popen.stdout.read()):
        context.add_stdout(
            message=stdout,
        )
    if popen.returncode:
        if popen.stderr:
            stderr = popen.stderr.read()
            # This log is visible on the events table and Splunk
            context.add_stderr(
                message=stderr,
            )
        # This log is visible to the user on the WebPortal
        context.add_user_visible_error(
            message="Your MARS request has not completed successfully, please check your selection.",
        )
        # This exception is visible on Splunk
        raise RuntimeError(f"MARS has crashed.\n{stderr}")
    if not os.path.getsize(target):
        context.add_user_visible_error(
            message="MARS returned no data, please check your selection.",
        )
        raise RuntimeError("MARS returned no data.")

    return target


class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        result = execute_mars(request, context=self.context)
        return open(result)  # type: ignore


class MarsCdsAdaptor(cds.AbstractCdsAdaptor):
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

        paths = convert_format(
            result, data_format, context=self.context, **convert_kwargs
        )

        # A check to ensure that if there is more than one path, and download_format
        #  is as_source, we over-ride and zip up the files
        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return self.make_download_object(paths)

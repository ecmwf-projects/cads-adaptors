import os
from typing import BinaryIO, Union

from cads_adaptors import mapping
from cads_adaptors.adaptors import Context, Request, cds
from cads_adaptors.tools.general import ensure_list


def execute_mars(
    request: Union[Request, list],
    target: str = "data.grib",
    context: Context | None = None,
    mars_cmd: tuple[str, ...] = ("/usr/local/bin/mars", "r"),
) -> str:
    import subprocess

    requests = ensure_list(request)
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

    output = subprocess.run(
        mars_cmd,
        check=False,
        env=env,
        capture_output=True,
        text=True,
    )
    if context is not None:
        context.stdout = context.user_visible_log = output.stdout
        context.stderr = output.stderr
    if output.returncode:
        raise RuntimeError("MARS has crashed.")
    if not os.path.getsize(target):
        raise RuntimeError("MARS returned no data.")

    return target


class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        result = execute_mars(request, context=self.context)
        return open(result)  # type: ignore


class MarsCdsAdaptor(DirectMarsCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools

        # Format of data files, grib or netcdf
        data_format = request.pop("format", "grib")  # TODO: remove legacy syntax?
        data_format = request.pop("data_format", data_format)

        if data_format in ["netcdf", "nc", "netcdf_compressed"]:
            default_download_format = "zip"
        else:
            default_download_format = "as_source"

        # Format of download archive, as_source, zip, tar, list etc.
        download_format = request.pop("download_format", default_download_format)

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore

        result = execute_mars(mapped_request, context=self.context)

        # NOTE: The NetCDF compressed option will not be visible on the WebPortal, it is here for testing
        if data_format in ["netcdf", "nc", "netcdf_compressed"]:
            if data_format in ["netcdf_compressed"]:
                to_netcdf_kwargs = {
                    "compression_options": "default",
                }
            else:
                to_netcdf_kwargs = {}
            from cads_adaptors.tools.convertors import grib_to_netcdf_files

            results = grib_to_netcdf_files(result, **to_netcdf_kwargs)
        else:
            results = [result]

        download_kwargs = {
            "base_target": f"{self.collection_id}-{hash(tuple(request))}"
        }
        return download_tools.DOWNLOAD_FORMATS[download_format](
            results, **download_kwargs
        )

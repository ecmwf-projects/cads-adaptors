import os
from typing import BinaryIO, Union

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request, cds


def execute_mars(request: Union[Request, list], target="data.grib"):
    import subprocess

    with open("r", "w") as fp:
        print(f"retrieve, target={target}", file=fp)
        for key, value in request.items():
            if not isinstance(value, (list, tuple)):
                value = [value]
            print(f", {key}={'/'.join(str(v) for v in value)}", file=fp)

    env = dict(**os.environ)
    # FIXME: set with the namespace and user_id
    namespace = "cads"
    user_id = 0
    env["MARS_USER"] = f"{namespace}-{user_id}"

    subprocess.run(["/usr/local/bin/mars", "r"], check=True, env=env)

    return target


class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        result = execute_mars(request)

        return open(result)  # type: ignore


class MarsCdsAdaptor(DirectMarsCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools

        # Format of data files, grib or netcdf
        data_format = request.pop("format", "grib")

        # Format of download archive, as_source, zip, tar, list etc.
        download_format = request.pop("download_format", "as_source")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore
        if data_format not in ["grib"]:
            # FIXME: reformat if needed
            pass

        result = execute_mars(mapped_request)

        download_kwargs = {
            "base_target": f"{self.collection_id}-{hash(tuple(request))}"
        }
        return download_tools.DOWNLOAD_FORMATS[download_format](
            [result], **download_kwargs
        )

import os
from typing import BinaryIO, Union

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request, cds
from cads_adaptors.tools.general import ensure_list


def execute_mars(request: Union[Request, list], target="data.grib"):
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
        ["/usr/local/bin/mars", "r"], check=False, env=env, capture_output=True
    )
    return target, output


class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        result, output = execute_mars(request)

        self.context.stdout = output.stdout.decode()
        self.context.stderr = output.stderr.decode()
        self.context.user_visible_log = output.stdout.decode()

        if output.returncode:
            raise RuntimeError("The Direct MARS adaptor has crashed.")

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

        result, output = execute_mars(mapped_request)

        self.context.stdout = output.stdout.decode()
        self.context.stderr = output.stderr.decode()
        self.context.user_visible_log = output.stdout.decode()

        if output.returncode:
            raise RuntimeError("The MARS adaptor has crashed.")

        download_kwargs = {
            "base_target": f"{self.collection_id}-{hash(tuple(request))}"
        }
        return download_tools.DOWNLOAD_FORMATS[download_format](
            [result], **download_kwargs
        )

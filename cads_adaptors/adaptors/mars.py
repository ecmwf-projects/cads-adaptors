import os
from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request, cds


class DirectMarsCdsAdaptor(cds.AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: Request) -> BinaryIO:
        import subprocess

        with open("r", "w") as fp:
            print("retrieve, target=data.grib", file=fp)
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

        return open("data.grib")  # type: ignore



class MarsCdsAdaptor(DirectMarsCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        data_format = request.pop("format", "grib")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore
        if data_format != "grib":
            # FIXME: reformat if needed
            pass
        return super().retrieve(mapped_request)


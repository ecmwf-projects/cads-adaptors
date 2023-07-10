import os
from typing import Any, BinaryIO

from . import adaptor, constraints, costing, mapping


class AbstractCdsAdaptor(adaptor.AbstractAdaptor):
    resources = {"CADS_ADAPTORS": 1}

    def __init__(self, form: dict[str, Any], **config: Any):
        self.form = form
        self.constraints = config.pop("constraints", [])
        self.mapping = config.pop("mapping", {})
        self.licences: list[tuple[str, int]] = config.pop("licences", [])
        self.config = config

    def validate(self, request: adaptor.Request) -> bool:
        return True

    def apply_constraints(self, request: adaptor.Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request: adaptor.Request) -> dict[str, int]:
        costs = {"size": costing.estimate_size(self.form, request, self.constraints)}
        return costs

    def get_licences(self, request: adaptor.Request) -> list[tuple[str, int]]:
        return self.licences


class UrlCdsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        from .tools import url_tools

        data_format = request.pop("format", "zip")

        if data_format not in {"zip", "tgz"}:
            raise ValueError(f"{data_format=} is not supported")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore

        requests_urls = url_tools.requests_to_urls(
            mapped_request, patterns=self.config["patterns"]
        )

        path = url_tools.download_from_urls(
            [ru["url"] for ru in requests_urls], data_format=data_format
        )
        return open(path, "rb")


class LegacyCdsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        import cdsapi

        # parse input options
        collection_id = self.config.pop("collection_id", None)
        if not collection_id:
            raise ValueError("collection_id is required in request")

        # retrieve data
        client = cdsapi.Client(self.config["url"], self.config["key"], retry_max=1)
        result_path = client.retrieve(collection_id, request).download()
        return open(result_path, "rb")


class DirectMarsCdsAdaptor(AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: adaptor.Request) -> BinaryIO:
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
    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        data_format = request.pop("format", "grib")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore
        if data_format != "grib":
            # FIXME: reformat if needed
            pass
        return super().retrieve(mapped_request)

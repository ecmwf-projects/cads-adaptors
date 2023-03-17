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
        from . import url_tools

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

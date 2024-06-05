import os
from copy import deepcopy
from typing import Any, Union

from cads_adaptors import constraints, costing, mapping
from cads_adaptors.adaptors import AbstractAdaptor, Context, Request
from cads_adaptors.tools.general import ensure_list
from cads_adaptors.validation import enforce


class AbstractCdsAdaptor(AbstractAdaptor):
    resources = {"CADS_ADAPTORS": 1}
    adaptor_schema = {}

    def __init__(
        self,
        form: list[dict[str, Any]] | dict[str, Any] | None,
        context: Context | None = None,
        **config: Any,
    ):
        self.form = form
        self.collection_id = config.get("collection_id", "unknown-collection")
        self.constraints = config.pop("constraints", [])
        self.mapping = config.pop("mapping", {})
        self.licences: list[tuple[str, int]] = config.pop("licences", [])
        self.config = config
        if context is None:
            self.context = Context()
        else:
            self.context = context
        # The following attributes are updated during the retireve method
        self.input_request: Request = Request()
        self.mapped_request: Request = Request()
        self.download_format: str = "zip"
        self.receipt: bool = False
        self.schemas = config.pop("schemas", [])

    def validate(self, request: Request) -> bool:
        return True

    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def intersect_constraints(self, request: Request) -> list[Request]:
        return [
            self.normalise_request(request)
            for request in constraints.legacy_intersect_constraints(
                request, self.constraints
            )
        ]

    def apply_mapping(self, request: Request) -> Request:
        return mapping.apply_mapping(request, self.mapping)

    def estimate_costs(
        self, request: Request, cost_threshold: str = "max_costs"
    ) -> dict[str, int]:
        costing_config: dict[str, Any] = self.config.get("costing", dict())
        costing_kwargs: dict[str, Any] = costing_config.get("costing_kwargs", dict())
        cost_threshold = (
            cost_threshold if cost_threshold in costing_config else "max_costs"
        )
        costs = {}
        # Safety net, not all stacks have the latest version of the api:
        if "inputs" in request:
            request = request["inputs"]
        # "precise_size" is a new costing method that is more accurate than "size
        if "precise_size" in costing_config.get(cost_threshold, {}):
            costs["precise_size"] = costing.estimate_precise_size(
                self.form,
                request,
                self.constraints,
                **costing_kwargs,
            )
        # size is a fast and rough estimate of the number of fields
        costs["size"] = costing.estimate_number_of_fields(self.form, request)
        # Safety net for integration tests:
        costs["number_of_fields"] = costs["size"]
        return costs

    def normalise_request(self, request: Request) -> Request:
        schemas = self.schemas
        if adaptor_schema := self.adaptor_schema:
            schemas = [adaptor_schema] + schemas
        for schema in schemas:
            request = enforce.enforce(request, schema, self.context.logger)
        return request

    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        return self.licences

    # This is essentially a second __init__, but only for when we have a request at hand
    # and currently only implemented for retrieve methods
    def _pre_retrieve(self, request: Request, default_download_format="zip"):
        self.input_request = deepcopy(request)
        self.receipt = request.pop("receipt", False)
        self.mapped_request = self.apply_mapping(request)  # type: ignore

        self.download_format = self.mapped_request.pop(
            "download_format", default_download_format
        )

    def make_download_object(
        self,
        paths: str | list[str],
        **kwargs,
    ):
        from cads_adaptors.tools import download_tools

        # Ensure paths and filenames are lists
        paths = ensure_list(paths)
        filenames = [os.path.basename(path) for path in paths]
        # TODO: use request-id instead of hash
        kwargs.setdefault(
            "base_target", f"{self.collection_id}-{hash(tuple(self.input_request))}"
        )

        # Allow possibility of over-riding the download format from the adaptor
        download_format = kwargs.get("download_format", self.download_format)
        download_format = ensure_list(download_format)[0]

        # If length of paths is greater than 1, then we cannot provide as_source, therefore we zip
        if len(paths) > 1 and download_format == "as_source":
            download_format = "zip"

        # Allow adaptor possibility of over-riding request value
        if kwargs.get("receipt", self.receipt):
            receipt_kwargs = kwargs.pop("receipt_kwargs", {})
            kwargs.setdefault(
                "receipt", self.make_receipt(filenames=filenames, **receipt_kwargs)
            )
        self.context.add_stdout(
            f"Creating download object as {download_format} with paths:\n{paths}\n and kwargs:\n{kwargs}"
        )
        self.context.add_user_visible_log(
            f"Creating download object as {download_format} with files:\n{filenames}"
        )
        try:
            return download_tools.DOWNLOAD_FORMATS[download_format](paths, **kwargs)
        except Exception as err:
            self.context.add_user_visible_error(
                message=(
                    "There was an error whilst preparing your data for download, "
                    "please try submitting you request again. "
                    "If the problem persists, please contact user support. "
                    f"Files being prepared for download: {filenames}\n"
                )
            )
            self.context.add_stderr(
                f"Error whilst preparing download object: {err}\n"
                f"Paths: {paths}\n"
                f"Download format: {download_format}\n"
                f"kwargs: {kwargs}\n"
            )
            raise err

    def make_receipt(
        self,
        input_request: Union[Request, None] = None,
        download_size: Any = None,
        filenames: list = [],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Create a receipt to be included in the downloaded archive.

        **kwargs contains any other fields that are calculated during the runtime of the adaptor
        """
        from datetime import datetime as dt

        # Allow adaptor to override and provide sanitized "input_request" if necessary
        if input_request is None:
            input_request = self.input_request

        # Update kwargs with default values
        if download_size is None:
            download_size = "unknown"

        receipt = {
            "collection-id": self.collection_id,
            "request": input_request,
            "request-timestamp": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            "download-size": download_size,
            "filenames": filenames,
            # Get static URLs:
            "user-support": "https://support.ecmwf.int",
            "privacy-policy": "https://cds.climate.copernicus.eu/disclaimer-privacy",
            # TODO: Change to URLs for licence instead of slug
            "licence": [
                f"{licence[0]} (version {licence[1]})" for licence in self.licences
            ],
            "user_uid": self.config.get("user_uid"),
            "request_uid": self.config.get("request_uid"),
            #
            # TODO: Add URL/DNS information to the context for populating these fields:
            # "web-portal": self.???, # Need update to information available to adaptors
            # "api-access": "https://url-to-data-api/{self.collection_id}"
            # "metadata-api-access": "https://url-to-metadata-api/{self.collection_id}"
            #
            # TODO: Add metadata information to config, this could also be done via the metadata api
            # "citation": self.???, # Need update to information available to adaptors
            **kwargs,
            **self.config.get("additional_receipt_info", {}),
        }

        return receipt


class DummyCdsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> Any:
        pass

import os
from copy import deepcopy
from typing import Any, Union

from cads_adaptors import constraints, costing, mapping
from cads_adaptors.adaptors import AbstractAdaptor, Context, Request
from cads_adaptors.tools.general import ensure_list


class AbstractCdsAdaptor(AbstractAdaptor):
    resources = {"CADS_ADAPTORS": 1}

    def __init__(self, form: dict[str, Any], **config: Any):
        self.form = form
        self.collection_id = config.get("collection_id", "unknown-collection")
        self.constraints = config.pop("constraints", [])
        self.mapping = config.pop("mapping", {})
        self.licences: list[tuple[str, int]] = config.pop("licences", [])
        self.config = config
        self.context = Context()
        # The following attributes are updated during the retireve method
        self.input_request: Request = Request()
        self.mapped_request: Request = Request()
        self.download_format: str = "zip"
        self.receipt: bool = False

    def validate(self, request: Request) -> bool:
        return True

    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request: Request) -> dict[str, int]:
        costs = {
            "size": costing.estimate_size(self.form, request, self.constraints)
        }
        return costs

    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        return self.licences

    # This is essentially a second __init__, but only for when we have a request at hand
    # and currently only implemented for retrieve methods
    def _pre_retrieve(self, request: Request, default_download_format="zip"):
        self.input_request = deepcopy(request)
        self.receipt = request.pop("receipt", True)
        self.download_format = request.pop("download_format", default_download_format)
        self.mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore

    def make_download_object(
        self,
        paths: Union[str, list],
        **kwargs,
    ):
        from cads_adaptors.tools import download_tools

        # Allow possibility of over-riding the download format from the adaptor
        download_format = kwargs.get("download_format", self.download_format)

        paths = ensure_list(paths)
        filenames = [os.path.basename(path) for path in paths]
        # TODO: use request-id instead of hash
        kwargs.setdefault(
            "base_target", f"{self.collection_id}-{hash(tuple(self.input_request))}"
        )

        # Allow adaptor possibility of over-riding request value
        if kwargs.get("receipt", self.receipt):
            receipt_kwargs = kwargs.pop("receipt_kwargs", {})
            kwargs.setdefault(
                "receipt", self.make_receipt(filenames=filenames, **receipt_kwargs)
            )

        return download_tools.DOWNLOAD_FORMATS[download_format](paths, **kwargs)

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
            # TODO: Add request-id information to the context
            "request-uid": self.config.get("request_uid", "Unavailable"),
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

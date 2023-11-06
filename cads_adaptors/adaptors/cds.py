from typing import Any

from cads_adaptors import constraints, costing
from cads_adaptors.adaptors import AbstractAdaptor, Context, Request


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

    def validate(self, request: Request) -> bool:
        return True

    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request: Request) -> dict[str, int]:
        costs = {"size": costing.estimate_size(self.form, request, self.constraints)}
        return costs

    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        return self.licences

    def make_receipt(
        self,
        request: Request,
        download_size: [None, int] = None,
        filenames: list = [],
        **kwargs
    ) -> dict[str, Any]:
        """
        Create a receipt to be included in the downloaded archive.

        **kwargs contains any other fields that are calculated during the runtime of the adaptor
        """
        from datetime import datetime as dt
        # Update kwargs with default values
        if download_size is None:
            download_size = "unknown"

        receipt = {
            "collection-id": self.collection_id,
            "request": request,
            "request-timestamp": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            "request-id": self.config.get("process_id"),
            "download-size": download_size,
            "filenames": filenames,
            "licence": self.licences,
            # TODO: fetch relevant information from metadata, potentially via API or populated directly
            #   in the config opbject.
            # "web-portal": self.???, # Need update to information available to adaptors
            # "request-id": self.???, # Need update to information available to adaptors
            # "citation": self.???, # Need update to information available to adaptors
            # "api-access": "https://url-to-data-api/{self.collection_id}"
            # "metadata-api-access": "https://url-to-metadata-api/{self.collection_id}"
            # "user-support": "https://link/to/user/support"
            # "privacy-policy": "https://link/to/privacy/policy"
            **kwargs,
            **self.config.get("additional_receipt_info", {}),
        }

        return receipt

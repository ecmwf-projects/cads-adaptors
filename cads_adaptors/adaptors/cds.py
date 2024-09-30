import os
import pathlib
from copy import deepcopy
from random import randint
from typing import Any, Union

from cads_adaptors import constraints, costing, mapping
from cads_adaptors.adaptors import AbstractAdaptor, Context, Request
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools.general import ensure_list
from cads_adaptors.validation import enforce


class AbstractCdsAdaptor(AbstractAdaptor):
    resources = {"CADS_ADAPTORS": 1}
    adaptor_schema: dict[str, Any] = {}

    def __init__(
        self,
        form: list[dict[str, Any]] | dict[str, Any] | None,
        context: Context | None = None,
        cache_tmp_path: pathlib.Path | None = None,
        **config: Any,
    ) -> None:
        self.collection_id = config.get("collection_id", "unknown-collection")
        self.constraints = config.pop("constraints", [])
        self.mapping = config.pop("mapping", {})
        self.licences: list[tuple[str, int]] = config.pop("licences", [])
        super().__init__(form, context, cache_tmp_path, **config)

        # The following attributes are updated during the retireve method
        self.input_request: Request = dict()
        self.mapped_request: Request = dict()
        self.download_format: str = "zip"
        self.receipt: bool = False
        self.schemas: list[dict[str, Any]] = config.pop("schemas", [])
        self.intersect_constraints_bool: bool = config.get(
            "intersect_constraints", False
        )
        self.embargo: dict[str, int] | None = config.get("embargo", None)
        # Flag to ensure we only normalise the request once
        self.normalised: bool = False
        # List of steps to perform after retrieving the data
        self.post_process_steps: list[dict[str, Any]] = [{}]

    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def intersect_constraints(self, request: Request, **kwargs) -> list[Request]:
        return constraints.legacy_intersect_constraints(
            request, self.constraints, context=self.context, **kwargs
        )

    def apply_mapping(self, request: Request) -> Request:
        return mapping.apply_mapping(request, self.mapping)

    def estimate_costs(self, request: Request, **kwargs: Any) -> dict[str, int]:
        cost_threshold = kwargs.get("cost_threshold", "max_costs")
        costing_config: dict[str, Any] = self.config.get("costing", dict())
        costing_kwargs: dict[str, Any] = costing_config.get("costing_kwargs", dict())
        cost_threshold = (
            cost_threshold if cost_threshold in costing_config else "max_costs"
        )
        max_costs = costing_config.get(cost_threshold, {})
        costs: dict[str, int] = {}
        # Safety net, not all stacks have the latest version of the api:
        if "inputs" in request:
            request = request["inputs"]
        # size is a fast and rough estimate of the number of fields
        costs["size"] = costing.estimate_number_of_fields(self.form, request)
        # TODO: Can this safety net be removed yet?
        # Safety net for integration tests:
        costs["number_of_fields"] = costs["size"]

        # # Exit here if fast cost estimates is above threshold, precise_size can OOM kill the retreive-api
        # for key, value in costs.items():
        #     if value > max_costs.get(
        #         key, 1e6
        #     ):  # This is a hard-coded limit which should protect the system
        #         self.context.add_stdout(f"Returning quick costs: {costs}")
        #         return costs

        # "precise_size" is a new costing method that is more accurate than "size
        if "precise_size" in max_costs:
            self.context.add_stdout(f"Computing precise size for selection.")
            intersected_selection = self.intersect_constraints(request, allow_partial=True)

            # # Remove duplicates from the list of dicts
            # # Found not to be a big bottleneck, but could be optimised
            # intersected_selection = []
            # for i_c in self.intersect_constraints(request, allow_partial=True):
            #     if i_c not in intersected_selection:
            #         intersected_selection.append(i_c)

            self.context.add_stdout(
                f"{len(intersected_selection)} intersected selection hypercubes found."
            )
            costs["precise_size"] = costing.estimate_precise_size(
                self.form,
                intersected_selection,
                **costing_kwargs,
            )
        self.context.add_stdout(f"Computed costs: {costs}")
        return costs

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """
        Method called before the mapping is applied to the request. This will differ for each
        adaptor, so is separated out from the normalise_request method.
        """
        # Move the receipt flag from the request to the adaptor attributes (currently not in use)
        self.receipt = request.pop("receipt", False)

        # Extract post-process steps from the request before applying the mapping
        self.post_process_steps = request.pop("post_process", [])
        self.context.debug(
            f"Post-process steps extracted from request:\n{self.post_process_steps}"
        )

        return request

    def normalise_request(self, request: Request) -> Request:
        """
        Normalise the request prior to submission to the broker, and at the start of the retrieval.
        This is executed on the retrieve-api pod, and then repeated on the worker pod.

        The returned request needs to be compatible with the web-portal, it is currently what is used
        on the "Your requests" page, hence it should not be modified to much from the user's request.
        """
        if self.normalised:
            return request

        # Make a copy of the original request for debugging purposes
        self.input_request = deepcopy(request)
        self.context.debug(f"Input request:\n{self.input_request}")

        # Enforce the schema on the input request
        schemas = self.schemas
        if not isinstance(schemas, list):
            schemas = [schemas]
        # Apply first dataset schemas, then adaptor schema
        if adaptor_schema := self.adaptor_schema:
            schemas = schemas + [adaptor_schema]
        for schema in schemas:
            request = enforce.enforce(request, schema, self.context.logger)

        # Pre-mapping modifications
        working_request = self.pre_mapping_modifications(deepcopy(request))

        # If specified by the adaptor, intersect the request with the constraints.
        # The intersected_request is a list of requests
        if self.intersect_constraints_bool:
            self.intersected_requests = self.intersect_constraints(working_request)
            if len(self.intersected_requests) == 0:
                msg = "Error: no intersection with the constraints."
                self.context.add_user_visible_error(message=msg)
                raise InvalidRequest(msg)
        else:
            self.intersected_requests = ensure_list(working_request)

        # Map the list of requests
        self.mapped_requests = [
            self.apply_mapping(i_request) for i_request in self.intersected_requests
        ]

        # Implement embargo if specified
        if self.embargo is not None:
            from cads_adaptors.tools.date_tools import implement_embargo

            try:
                self.mapped_requests, cacheable_embargo = implement_embargo(
                    self.mapped_requests, self.embargo
                )
            except ValueError as e:
                self.context.add_user_visible_error(message=f"{e}")
                raise InvalidRequest(f"{e}")

            if not cacheable_embargo:
                # Add an uncacheable key to the request
                random_key = str(randint(0, 2**128))
                request["_part_of_request_under_embargo"] = random_key

        # At this point, the self.mapped_requests could be used to create a requesthash

        # For backwards compatibility, we set self.mapped_request to the first request, and assume
        #  it is the only one. Adaptors should be updated to use self.mapped_requests instead.
        self.mapped_request = self.mapped_requests[0]

        self.context.add_stdout(
            f"Request mapped to (collection_id={self.collection_id}):\n{self.mapped_requests}"
        )

        # Avoid the cache by adding a random key-value pair to the request (if cache avoidance is on)
        if self.config.get("avoid_cache", False):
            random_key = str(randint(0, 2**128))
            request["_in_adaptor_no_cache"] = random_key

        self.normalised = True
        return request

    def set_download_format(self, download_format, default_download_format="zip"):
        """Check that requested download format is supported by the adaptor, and if not set to default."""
        # Apply any mapping
        mapped_formats = self.apply_mapping(
            {
                "download_format": download_format,
            }
        )

        self.download_format = mapped_formats["download_format"]
        if isinstance(self.download_format, list):
            try:
                assert len(self.download_format) == 1
            except AssertionError:
                message = "Multiple download formats specified, only one is allowed"
                self.context.add_user_visible_error(message=message)
                raise InvalidRequest(message)
            self.download_format = self.download_format[0]

        from cads_adaptors.tools.download_tools import DOWNLOAD_FORMATS

        if self.download_format not in DOWNLOAD_FORMATS:
            self.context.add_user_visible_log(
                "WARNING: Download format not supported for this dataset. "
                f"Defaulting to {default_download_format}."
            )
            self.download_format = default_download_format

    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        return self.licences

    def pp_mapping(self, in_pp_config: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Map the post-process steps from the request to the correct functions."""
        from cads_adaptors.tools.post_processors import pp_config_mapping

        pp_config = [
            pp_config_mapping(_pp_config) for _pp_config in ensure_list(in_pp_config)
        ]
        return pp_config

    def post_process(self, result: Any) -> dict[str, Any]:
        """Perform post-process steps on the retrieved data."""
        for i, pp_step in enumerate(self.pp_mapping(self.post_process_steps)):
            self.context.add_stdout(
                f"Performing post-process step {i+1} of {len(self.post_process_steps)}: {pp_step}"
            )
            # TODO: pp_mapping should have ensured "method" is always present

            if "method" not in pp_step:
                self.context.add_user_visible_error(
                    message="Post-processor method not specified"
                )
                continue

            method_name = pp_step.pop("method")
            # TODO: Add extra condition to limit pps from dataset configurations
            if not hasattr(self, method_name):
                self.context.add_user_visible_error(
                    message=f"Post-processor method '{method_name}' not available for this dataset"
                )
                continue
            method = getattr(self, method_name)

            # post processing is done on xarray objects,
            # so on first pass we ensure result is opened as xarray
            if i == 0:
                from cads_adaptors.tools.convertors import (
                    open_result_as_xarray_dictionary,
                )

                post_processing_kwargs = self.config.get("post_processing_kwargs", {})

                open_datasets_kwargs = post_processing_kwargs.get(
                    "open_datasets_kwargs", {}
                )
                post_open_datasets_kwargs = post_processing_kwargs.get(
                    "post_open_datasets_kwargs", {}
                )
                self.context.add_stdout(
                    f"Opening result: {result} as xarray dictionary with kwargs:\n"
                    f"open_dataset_kwargs: {open_datasets_kwargs}\n"
                    f"post_open_datasets_kwargs: {post_open_datasets_kwargs}"
                )
                result = open_result_as_xarray_dictionary(
                    result,
                    context=self.context,
                    open_datasets_kwargs=open_datasets_kwargs,
                    post_open_datasets_kwargs=post_open_datasets_kwargs,
                )

            result = method(result, **pp_step)

        return result

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
        if self.input_request is None:
            self.input_request = {}
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
        # self.context.add_user_visible_log(
        #     f"Creating download object as {download_format} with files:\n{filenames}"
        # )
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

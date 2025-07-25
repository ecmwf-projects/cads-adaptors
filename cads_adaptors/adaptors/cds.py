import bisect
import os
import pathlib
import time
from copy import deepcopy
from random import randint
from typing import Any, BinaryIO

from cads_adaptors import constraints, costing, mapping
from cads_adaptors.adaptors import AbstractAdaptor, Context, Request
from cads_adaptors.exceptions import CdsConfigurationError, InvalidRequest
from cads_adaptors.tools.general import ensure_list
from cads_adaptors.tools.hcube_tools import hcubes_intdiff2
from cads_adaptors.validation import enforce

DEFAULT_COST_TYPE = "size"
DEFAULT_COST_TYPE_FOR_COSTING_CLASS = DEFAULT_COST_TYPE
COST_TYPE_WITH_HIGHEST_COST_LIMIT_RATIO_FOR_COSTING_CLASS = "highest_cost_limit_ratio"


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

    def retrieve_list_of_results(self, request: Request) -> list[str]:
        """
        Return a list of results, which are paths to files that have been downloaded,
        and post-processed if necessary.
        This is to separate the internal processing from the returning of an open file
        object for the retrive-api.
        It is required for adaptors used by the multi-adaptor.
        """
        raise NotImplementedError

    def retrieve(self, request: Request) -> BinaryIO:
        result = self.retrieve_list_of_results(request)
        return self.make_download_object(result)

    def check_validity(self, request: Request) -> None:
        return

    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def intersect_constraints(self, request: Request) -> list[Request]:
        return constraints.legacy_intersect_constraints(
            request, self.constraints, context=self.context
        )

    def apply_mapping(self, request: Request) -> Request:
        return mapping.apply_mapping(request, self.mapping, context=self.context)

    def get_cost_type_with_highest_cost_limit_ratio(
        self, costs: dict[str, int], limits: dict[str, int]
    ) -> str | None:
        """
        Determine the cost type with the highest cost/limit ratio.
        This is implementing the same logic as https://github.com/ecmwf-projects/cads-processing-api-service/blob/main/cads_processing_api_service/costing.py.
        """
        highest_cost_limit_ratio = 0.0
        highest_cost: dict[str, Any] = {"type": None, "cost": 0.0, "limit": 1.0}
        for limit_id, limit in limits.items():
            cost = costs.get(limit_id, 0.0)
            cost_limit_ratio = cost / limit if limit > 0 else 1.0
            if cost_limit_ratio > highest_cost_limit_ratio:
                highest_cost_limit_ratio = cost_limit_ratio
                highest_cost = {"type": limit_id, "cost": cost, "limit": limit}
        return highest_cost["type"]

    def estimate_costs(self, request: Request, **kwargs: Any) -> dict[str, int]:
        cost_threshold = kwargs.get("cost_threshold", "max_costs")
        costing_config: dict[str, Any] = self.config.get("costing", dict())
        costing_kwargs: dict[str, Any] = costing_config.get("costing_kwargs", dict())
        cost_threshold = (
            cost_threshold if cost_threshold in costing_config else "max_costs"
        )
        costs = {}
        # Safety net, not all stacks have the latest version of the api:
        if "inputs" in request:
            request = request["inputs"]
        mapped_request = self.apply_mapping(request)

        # Must also map the weights
        # N.B. This is just a partial mapping, as not all apply_mapping steps are covered for the weights.
        # Use with caution!
        rename = self.mapping.get("rename", {})
        remap = self.mapping.get("remap", {})
        weighted_keys = costing_kwargs.get("weighted_keys", {})
        weighted_values = costing_kwargs.get("weighted_values", {})

        # rename keys for weighted_keys
        mapped_weighted_keys = {
            rename.get(key, key): value for key, value in weighted_keys.items()
        }
        # rename keys and remap values for weighted_values
        mapped_weighted_values = {
            rename.get(key, key): {
                remap.get(key, {}).get(v, v): w for v, w in values.items()
            }
            for key, values in weighted_values.items()
        }

        # "precise_size" is a new costing method that is more accurate than "size
        costing_limits = costing_config.get(cost_threshold, {})
        if "precise_size" in costing_limits:
            costs["precise_size"] = costing.estimate_precise_size(
                self.form,
                mapped_request,
                self.constraints,
                **costing_kwargs,
            )
        # size is a fast and rough estimate of the number of fields
        costs[DEFAULT_COST_TYPE] = costing.estimate_number_of_fields(
            self.form,
            mapped_request,
            mapping=self.mapping,
            **{
                **costing_kwargs,
                "weighted_keys": mapped_weighted_keys,
                "weighted_values": mapped_weighted_values,
            },
        )
        # Safety net for integration tests:
        costs["number_of_fields"] = costs[DEFAULT_COST_TYPE]

        # add costing class
        costing_class_kwargs: dict[str, Any] = costing_config.get(
            "costing_class_kwargs", dict()
        )
        if costing_class_kwargs:
            based_on_cost_type = costing_class_kwargs.get(
                "cost_type", DEFAULT_COST_TYPE_FOR_COSTING_CLASS
            )
            if (
                based_on_cost_type
                == COST_TYPE_WITH_HIGHEST_COST_LIMIT_RATIO_FOR_COSTING_CLASS
            ):
                based_on_cost_type = self.get_cost_type_with_highest_cost_limit_ratio(
                    costs, costing_limits
                )

            cost_value = costs.get(
                based_on_cost_type, costs[DEFAULT_COST_TYPE_FOR_COSTING_CLASS]
            )

            costing_classes_inclusive_upper_bounds = costing_class_kwargs.get(
                "inclusive_upper_bounds", []
            )
            if isinstance(costing_classes_inclusive_upper_bounds, list):
                costing_classes_inclusive_upper_bounds.sort()
                cost_class = bisect.bisect_left(
                    costing_classes_inclusive_upper_bounds, cost_value
                )
            elif isinstance(costing_classes_inclusive_upper_bounds, dict):
                costing_classes_inclusive_upper_bounds = [
                    (v, k) for k, v in costing_classes_inclusive_upper_bounds.items()
                ]
                costing_classes_inclusive_upper_bounds.sort()
                cost_class_index = bisect.bisect_left(
                    costing_classes_inclusive_upper_bounds,
                    cost_value,
                    key=lambda x: x[0],
                )
                if cost_class_index < len(costing_classes_inclusive_upper_bounds):
                    cost_class = costing_classes_inclusive_upper_bounds[
                        cost_class_index
                    ][1]
                else:
                    cost_class = costing_class_kwargs.get(
                        "last_class_name", cost_class_index
                    )
            else:
                raise CdsConfigurationError

            costs["cost_class"] = cost_class

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

    def ensure_list_values(self, dicts):
        for d in dicts:
            for key in d:
                d[key] = ensure_list(d[key])

    def satisfy_conditions(
        self,
        requests: list[dict[str, list[Any]]],
        conditions: list[dict[str, list[Any]]],
    ):
        try:
            _, d12, _ = hcubes_intdiff2(requests, conditions)
            return not d12
        except Exception:
            return False

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

        # Implement a request-level tagging system
        try:
            self.conditional_tagging = self.config.get("conditional_tagging", None)
            if self.conditional_tagging is not None:
                self.ensure_list_values(self.intersected_requests)
                for tag in self.conditional_tagging:
                    conditions = self.conditional_tagging[tag]
                    self.ensure_list_values(conditions)
                    if self.satisfy_conditions(self.intersected_requests, conditions):
                        hidden_tag = f"__{tag}"
                        request[hidden_tag] = True
        except Exception as e:
            self.context.add_stdout(
                f"An error occured while attempting conditional tagging: {e!r}"
            )

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
                request["__part_of_request_under_embargo"] = random_key

        # At this point, the self.mapped_requests could be used to create a requesthash

        # For backwards compatibility, we set self.mapped_request to the first request, and assume
        #  it is the only one. Adaptors should be updated to use self.mapped_requests instead.
        self.mapped_request = self.mapped_requests[0]

        self.context.info(
            f"Request mapped to (collection_id={self.collection_id}):\n{self.mapped_requests}"
        )

        # Avoid the cache by adding a random key-value pair to the request (if cache avoidance is on)
        if self.config.get("avoid_cache", False):
            random_key = str(randint(0, 2**128))
            request["__in_adaptor_no_cache"] = random_key

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
            self.context.info(
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
                self.context.debug(
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
        paths: list[str],
        **kwargs,
    ) -> BinaryIO:
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
        self.context.debug(
            f"Creating download object as {download_format} with paths:\n{paths}\n and kwargs:\n{kwargs}"
        )
        # self.context.add_user_visible_log(
        #     f"Creating download object as {download_format} with files:\n{filenames}"
        # )
        try:
            time0 = time.time()
            download_object = download_tools.DOWNLOAD_FORMATS[download_format](
                paths, **kwargs
            )
            delta_time = time.time() - time0
            try:
                filesize = os.path.getsize(download_object.name)
            except AttributeError:
                self.context.warning(f"Unexpected download object: {download_object}")
                filesize = 0

            self.context.info(
                f"Download object created. Filesize={filesize*1e-6} Mb, "
                f"delta_time= {delta_time:.2f} seconds.",
                delta_time=delta_time,
                filesize=filesize,
            )
            return download_object
        except Exception as err:
            self.context.add_user_visible_error(
                message=(
                    "There was an error whilst preparing your data for download, "
                    "please try submitting you request again. "
                    "If the problem persists, please contact user support. "
                    "Files being prepared for download:\n"
                    "\n -".join(filenames)
                )
            )
            self.context.error(
                f"Error whilst preparing download object: {err}\n"
                f"Paths: {paths}\n"
                f"Download format: {download_format}\n"
                f"kwargs: {kwargs}\n"
            )
            raise err

    def make_receipt(
        self,
        input_request: Request | None = None,
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
    def retrieve(self, request: Request) -> BinaryIO:
        dummy_file = self.cache_tmp_path / "dummy.grib"
        with dummy_file.open("w") as fp:
            fp.write("DUMMY CONTENT")
        return open(dummy_file, "rb")

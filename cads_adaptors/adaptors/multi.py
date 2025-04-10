from typing import Any

from cads_adaptors import AbstractCdsAdaptor, mapping
from cads_adaptors.adaptors import Request
from cads_adaptors.exceptions import CdsConfigurationError, MultiAdaptorNoDataError, InvalidRequest
from cads_adaptors.tools import adaptor_tools
from cads_adaptors.tools.general import ensure_list


class MultiAdaptor(AbstractCdsAdaptor):
    @property
    def extract_subrequest_kws(self) -> list[str]:
        # extract keywords from a function signature
        # (this is possibly overkill, but it's useful if )we think the function signature may change)
        import inspect

        sig = inspect.signature(self.extract_subrequest)
        return [
            name
            for name, param in sig.parameters.items()
            if param.default != inspect.Parameter.empty
        ]

    def get_extract_subrequest_kwargs(
        self, this_adaptor_config: dict[str, Any]
    ) -> dict[str, Any]:
        # Get any top level kwargs for extract_subrequest
        extract_subrequest_kwargs: dict[str, Any] = {
            k: self.config["extract_subrequest_kwargs"][k]
            for k in self.extract_subrequest_kws
            if k in self.config.get("extract_subrequest_kwargs", {})
        }

        for k in self.extract_subrequest_kws:
            if k not in this_adaptor_config:
                continue
            if k not in extract_subrequest_kwargs:
                extract_subrequest_kwargs[k] = this_adaptor_config[k]
                continue

            # k in both this_adaptor_config and extract_subrequest_kwargs, check they are same type
            try:
                assert isinstance(
                    this_adaptor_config[k], type(extract_subrequest_kwargs[k])
                )
            except AssertionError:
                raise CdsConfigurationError(
                    f"Adaptor configuration error: extract_subrequest_kwargs: {k} "
                    f"has been set in both the top-level adaptor.json and the sub-adaptor.json, "
                    f"but they are not the same type. "
                )

            if isinstance(this_adaptor_config[k], dict):
                extract_subrequest_kwargs[k] = {
                    **extract_subrequest_kwargs[k],
                    **this_adaptor_config[k],
                }
            elif isinstance(this_adaptor_config[k], list):
                extract_subrequest_kwargs[k] = extract_subrequest_kwargs[k] + [
                    val
                    for val in this_adaptor_config[k]
                    if val not in extract_subrequest_kwargs[k]
                ]
            else:
                extract_subrequest_kwargs[k] = this_adaptor_config[k]

        return extract_subrequest_kwargs

    @staticmethod
    def extract_subrequest(
        full_request: Request,  # User request
        this_values: dict[str, Any],  # key: [values] for the adaptor component
        dont_split_keys: list[str] = ["area", "grid"],
        filter_keys: None | list[str] = None,
        required_keys: list[str] = [],
    ) -> Request:
        """
        Basic request splitter, splits based on whether the values are relevant to
        the specific adaptor.
        More complex constraints may need a more detailed splitter.
        """
        required_keys = ensure_list(required_keys)
        this_request = {}
        # Default filter_keys to all keys
        if filter_keys is None:
            filter_keys = list(full_request.keys())
        for key, req_vals in full_request.items():
            # If not in filter_keys or is in dont_split_key, then copy the key and values to the new request
            #  filter_keys may make dont_split_keys redundant, but keep both for now
            if key not in ensure_list(filter_keys) or key in ensure_list(
                dont_split_keys
            ):
                this_request[key] = req_vals
            else:
                # filter for values relevant to this_adaptor:
                these_vals = [
                    v
                    for v in ensure_list(req_vals)
                    if str(v) in this_values.get(key, [])
                ]
                if len(these_vals) > 0:
                    # if values then add to request
                    this_request[key] = these_vals
                elif key in required_keys:
                    # If a required key is missing, then return an empty dictionary.
                    #  optional keys must be set in the adaptor.json via gecko
                    return dict()

        # Our request may not have included all keys, so do a final check that all required keys are present
        if not all([key in this_request for key in required_keys]):
            return dict()

        return this_request

    def split_adaptors(
        self, request: Request
    ) -> dict[str, tuple[AbstractCdsAdaptor, Request]]:
        from cads_adaptors.tools import adaptor_tools

        sub_adaptors = {}
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(
                adaptor_desc | {"context": self.context},
                self.form,
            )
            this_values = adaptor_desc.get("values", {})

            extract_subrequest_kwargs = self.get_extract_subrequest_kwargs(
                this_adaptor.config
            )
            this_request = self.extract_subrequest(
                request, this_values, **extract_subrequest_kwargs
            )
            self.context.info(
                f"MultiAdaptor, {adaptor_tag}, this_request: {this_request}"
            )

            if len(this_request) > 0:
                try:
                    this_request = this_adaptor.normalise_request(this_request)
                except Exception:
                    self.context.warning(
                        f"MultiAdaptor failed to normalise request.\n"
                        f"adaptor_tag: {adaptor_tag}\nthis_request: {this_request}"
                    )
                sub_adaptors[adaptor_tag] = (this_adaptor, this_request)

        if len(sub_adaptors) == 0:
            message = (
                "Request has not produced a valid combination of values, "
                f"please check your selection.\n{request}"
            )
            self.context.add_user_visible_error(message)
            raise InvalidRequest(message)

        return sub_adaptors

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        request = super().pre_mapping_modifications(request)

        download_format = request.pop("download_format", "zip")
        self.set_download_format(download_format)

        return request

    def retrieve_list_of_results(self, request: Request) -> list[str]:
        request = self.normalise_request(request)
        # TODO: handle lists of requests, normalise_request has the power to implement_constraints
        #  which produces a list of complete hypercube requests.
        try:
            assert len(self.mapped_requests) == 1
        except AssertionError:
            self.context.add_user_visible_log(
                f"WARNING: More than one request was mapped: {self.mapped_requests}, "
                f"returning the first one only:\n{self.mapped_requests[0]}"
            )
        self.mapped_request = self.mapped_requests[0]

        self.context.debug(f"MultiAdaptor, full_request: {self.mapped_request}")

        sub_adaptors = self.split_adaptors(self.mapped_request)

        paths: list[str] = []
        exception_logs: dict[str, str] = {}
        for adaptor_tag, [adaptor, req] in sub_adaptors.items():
            try:
                this_result = adaptor.retrieve_list_of_results(req)
            except Exception as err:
                exception_logs[adaptor_tag] = f"{err}"
            else:
                paths.extend(this_result)

        if len(paths) == 0:
            raise MultiAdaptorNoDataError(
                "MultiAdaptor returned no results, the error logs of the sub-adaptors is as follows:\n"
                f"{exception_logs}"
            )

        self.context.debug(f"MultiAdaptor, result paths:\n{paths}")

        return paths


class MultiMarsCdsAdaptor(MultiAdaptor):
    def convert_format(self, *args, **kwargs) -> list[str]:
        from cads_adaptors.tools.convertors import convert_format

        return convert_format(*args, **kwargs)

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """Implemented in normalise_request, before the mapping is applied."""
        request = super().pre_mapping_modifications(request)

        # TODO: Remove legacy syntax all together
        data_format = request.pop("format", "grib")
        data_format = request.pop("data_format", data_format)

        # Account from some horribleness from the legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            data_format = "netcdf"
            request.setdefault("download_format", "zip")

        default_download_format = "as_source"
        download_format = request.pop("download_format", default_download_format)
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        # Apply any mapping
        mapped_formats = self.apply_mapping({"data_format": data_format})
        # TODO: Add this extra mapping to apply_mapping?
        self.data_format = adaptor_tools.handle_data_format(
            mapped_formats["data_format"]
        )
        return request

    def retrieve_list_of_results(self, request: Request) -> list[str]:
        """For MultiMarsCdsAdaptor we just want to apply mapping from each adaptor."""
        import dask

        from cads_adaptors.adaptors.mars import execute_mars

        request = self.normalise_request(request)
        # This will apply any top level multi-adaptor mapping, currently not used but could potentially
        #   be useful to reduce the repetitive config in each sub-adaptor of adaptor.json

        # self.mapped_requests contains the schema-checked, intersected and (top-level mapping) mapped request
        self.context.debug(
            f"MultiMarsCdsAdaptor, mapped full request: {self.mapped_requests}"
        )

        # We now split the mapped_request into sub-adaptors
        mapped_requests = []
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(adaptor_desc, self.form)
            this_values = adaptor_desc.get("values", {})
            extract_subrequest_kwargs = self.get_extract_subrequest_kwargs(
                this_adaptor.config
            )
            for mapped_request_piece in self.mapped_requests:
                this_request = self.extract_subrequest(
                    mapped_request_piece, this_values, **extract_subrequest_kwargs
                )
                if len(this_request) > 0:
                    mapped_requests.append(
                        mapping.apply_mapping(this_request, this_adaptor.mapping)
                    )

            self.context.debug(
                f"MultiMarsCdsAdaptor, {adaptor_tag}, this_request: {this_request}"
            )

        if len(mapped_requests) == 0:
            message = (
                "Request has not produced a valid combination of values, "
                f"please check your selection.\n{request}"
            )
            self.context.add_user_visible_error(message)
            raise InvalidRequest(message)

        self.context.debug(
            f"MultiMarsCdsAdaptor, mapped and split requests: {mapped_requests}"
        )
        result = execute_mars(
            mapped_requests,
            context=self.context,
            config=self.config,
            mapping=self.mapping,
            target_dir=self.cache_tmp_path,
        )

        with dask.config.set(scheduler="threads"):
            paths = self.convert_format(
                result,
                self.data_format,
                self.context,
                self.config,
                target_dir=str(self.cache_tmp_path),
            )

        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return paths

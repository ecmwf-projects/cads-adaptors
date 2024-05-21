from copy import deepcopy
from typing import Any

from cads_adaptors import AbstractCdsAdaptor, mapping
from cads_adaptors.adaptors import Request
from cads_adaptors.tools.general import ensure_list


class MultiAdaptor(AbstractCdsAdaptor):
    @staticmethod
    def split_request(
        full_request: Request,  # User request
        this_values: dict[str, Any],  # key: [values] for the adaptor component
        dont_split_keys: list[str] = ["area", "grid"],
        required_keys: list[str] = [],
        **config: Any,
    ) -> Request:
        """
        Basic request splitter, splits based on whether the values are relevant to
        the specific adaptor.
        More complex constraints may need a more detailed splitter.
        """
        required_keys = ensure_list(required_keys)
        this_request = {}
        # loop over keys in the full_request
        for key, req_vals in full_request.items():
            # filter for values relevant to this_adaptor:
            if key in ensure_list(dont_split_keys):
                these_vals = req_vals
            else:
                these_vals = [
                    v for v in ensure_list(req_vals) if v in this_values.get(key, [])
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

            this_request = self.split_request(
                request, this_values, **this_adaptor.config
            )
            self.context.add_stdout(
                f"MultiAdaptor, {adaptor_tag}, this_request: {this_request}"
            )

            if len(this_request) > 0:
                this_request["download_format"] = "list"
                this_request["receipt"] = False
                # Now try to normalise the request

                try:
                    this_request = this_adaptor.normalise_request(this_request)
                except Exception:
                    self.context.add_stdout(
                        f"MultiAdaptor, {adaptor_tag}, this_request: {this_request}"
                    )
                sub_adaptors[adaptor_tag] = (this_adaptor, this_request)

        return sub_adaptors

    def _pre_retrieve(self, request, default_download_format="zip"):
        self.input_request = deepcopy(request)
        self.receipt = request.pop("receipt", False)
        self.mapped_request = mapping.apply_mapping(request, self.mapping)
        self.download_format = self.mapped_request.pop("download_format", default_download_format)

    def retrieve(self, request: Request):
        self._pre_retrieve(request, default_download_format="zip")

        self.context.add_stdout(f"MultiAdaptor, full_request: {self.mapped_request}")

        sub_adaptors = self.split_adaptors(self.mapped_request)

        results = []
        exception_logs: dict[str, str] = {}
        for adaptor_tag, [adaptor, req] in sub_adaptors.items():
            try:
                this_result = adaptor.retrieve(req)
            except Exception as err:
                exception_logs[adaptor_tag] = f"{err}"
            else:
                results += this_result

        if len(results) == 0:
            raise RuntimeError(
                "MultiAdaptor returned no results, the error logs of the sub-adaptors is as follows:\n"
                f"{exception_logs}"
            )
        # close files
        [res.close() for res in results]
        # get the paths
        paths = [res.name for res in results]

        return self.make_download_object(
            paths,
        )


class MultiMarsCdsAdaptor(MultiAdaptor):
    def convert_format(self, *args, **kwargs):
        from cads_adaptors.adaptors.mars import convert_format

        return convert_format(*args, **kwargs)

    def retrieve(self, request: Request):
        """For MultiMarsCdsAdaptor we just want to apply mapping from each adaptor."""
        from cads_adaptors.adaptors.mars import execute_mars
        from cads_adaptors.tools import adaptor_tools

        # Format of data files, grib or netcdf
        data_format = request.pop("format", "grib")
        data_format = request.pop("data_format", data_format)

        # Account from some horribleness from teh legacy system:
        if data_format.lower() in ["netcdf.zip", "netcdf_zip", "netcdf4.zip"]:
            data_format = "netcdf"
            request.setdefault("download_format", "zip")

        # Allow user to provide format conversion kwargs
        convert_kwargs = {
            **self.config.get("format_conversion_kwargs", dict()),
            **request.pop("format_conversion_kwargs", dict()),
        }

        self._pre_retrieve(request, default_download_format="as_source")

        mapped_requests = []
        self.context.add_stdout(f"MultiMarsCdsAdaptor, full_request: {self.mapped_request}")
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(adaptor_desc, self.form)
            this_values = adaptor_desc.get("values", {})

            this_request = self.split_request(
                self.mapped_request, this_values, **this_adaptor.config
            )
            self.context.add_stdout(
                f"MultiMarsCdsAdaptor, {adaptor_tag}, this_request: {this_request}"
            )

            if len(this_request) > 0:
                mapped_requests.append(
                    mapping.apply_mapping(this_request, this_adaptor.mapping)
                )

        self.context.add_stdout(
            f"MultiMarsCdsAdaptor, mapped_requests: {mapped_requests}"
        )
        result = execute_mars(mapped_requests, context=self.context)

        paths = self.convert_format(result, data_format, self.context, **convert_kwargs)

        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return self.make_download_object(paths)

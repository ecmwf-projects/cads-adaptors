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
        **config: Any,
    ) -> Request:
        """
        Basic request splitter, splits based on whether the values are relevant to
        the specific adaptor.
        More complex constraints may need a more detailed splitter.
        """
        this_request = {}
        # loop over keys in this_values, i.e. the keys relevant to this_adaptor
        for key in list(this_values):
            # get request values for that key
            req_vals = full_request.get(key, [])
            # filter for values relevant to this_adaptor:
            these_vals = [
                v for v in ensure_list(req_vals) if v in this_values.get(key, [])
            ]
            if len(these_vals) > 0:
                # if values then add to request
                this_request[key] = these_vals
            elif key in config.get("required_keys", []):
                # If a required key is missing, then return an empty dictionary.
                #  optional keys must be set in the adaptor.json via gecko
                return {}

        return this_request

    def _pre_retrieve(self, request, default_download_format="zip"):
        self.input_request = deepcopy(request)
        self.receipt = request.pop("receipt", False)
        self.download_format = request.pop("download_format", default_download_format)

    def retrieve(self, request: Request):
        from cads_adaptors.tools import adaptor_tools

        self._pre_retrieve(request, default_download_format="zip")

        these_requests = {}
        exception_logs: dict[str, str] = {}
        self.context.logger.debug(f"MultiAdaptor, full_request: {request}")
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(adaptor_desc, self.form)
            this_values = adaptor_desc.get("values", {})

            this_request = self.split_request(
                request, this_values, **this_adaptor.config
            )
            self.context.logger.debug(
                f"MultiAdaptor, {adaptor_tag}, this_request: {this_request}"
            )

            # TODO: check this_request is valid for this_adaptor, or rely on try?
            #  i.e. split_request does NOT implement constraints.
            if len(this_request) > 0:
                this_request["download_format"] = "list"
                this_request["receipt"] = False
                these_requests[this_adaptor] = this_request

        results = []
        for adaptor, req in these_requests.items():
            try:
                this_result = adaptor.retrieve(req)
            except Exception as err:
                exception_logs[adaptor] = f"{err}"
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
    def retrieve(self, request: Request):
        """For MultiMarsCdsAdaptor we just want to apply mapping from each adaptor."""
        from cads_adaptors.adaptors.mars import convert_format, execute_mars
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
        self.context.logger.debug(f"MultiMarsCdsAdaptor, full_request: {request}")
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(adaptor_desc, self.form)
            this_values = adaptor_desc.get("values", {})

            this_request = self.split_request(
                request, this_values, **this_adaptor.config
            )
            self.context.logger.debug(
                f"MultiMarsCdsAdaptor, {adaptor_tag}, this_request: {this_request}"
            )

            if len(this_request) > 0:
                mapped_requests.append(
                    mapping.apply_mapping(this_request, this_adaptor.mapping)
                )

        self.context.logger.debug(
            f"MultiMarsCdsAdaptor, mapped_requests: {mapped_requests}"
        )
        result = execute_mars(mapped_requests, context=self.context)

        paths = convert_format(result, data_format, self.context, **convert_kwargs)

        if len(paths) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return self.make_download_object(paths)

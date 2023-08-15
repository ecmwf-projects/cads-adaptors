import typing as T

import yaml

from cads_adaptors import AbstractCdsAdaptor
from cads_adaptors.adaptors import Request
from cads_adaptors.tools.general import ensure_list
from cads_adaptors.tools.logger import logger


class MultiAdaptor(AbstractCdsAdaptor):
    @staticmethod
    def split_request(
        full_request: Request,  # User request
        this_values: T.Dict[str, T.Any],  # key: [values] for the adaptor component
        **config: T.Any,
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
                # If a required key, then return an empty dictionary.
                #  optional keys must be set in the adaptor.json via gecko
                return {}

        return this_request

    def retrieve(self, request: Request):
        from cads_adaptors.tools import adaptor_tools, download_tools

        download_format = request.pop("download_format", "zip")

        these_requests = {}
        exception_logs: T.Dict[str, str] = {}
        logger.debug(f"MultiAdaptor, full_request: {request}")
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(adaptor_desc, self.form)
            this_values = adaptor_desc.get("values", {})

            this_request = self.split_request(request, this_values, **self.config)
            logger.debug(f"MultiAdaptor, {adaptor_tag}, this_request: {this_request}")

            # TODO: check this_request is valid for this_adaptor, or rely on try?
            #  i.e. split_request does NOT implement constraints.
            if len(this_request) > 0:
                this_request.setdefault("download_format", "list")
                these_requests[this_adaptor] = this_request

        results = []
        for adaptor, req in these_requests.items():
            try:
                this_result = adaptor.retrieve(req)
            except Exception:
                exception_logs[adaptor] = Exception
            else:
                results += this_result
        
        if len(results)==0:
            raise Exception(logger.debug(exception_logs))

        # close files
        [res.close() for res in results]
        # get the paths
        paths = [res.name for res in results]

        download_kwargs = dict(
            base_target=f"{self.collection_id}-{hash(tuple(results))}"
        )

        return download_tools.DOWNLOAD_FORMATS[download_format](
            paths, **download_kwargs
        )

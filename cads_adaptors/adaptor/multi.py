import logging
from typing import Any

import yaml

from cads_adaptors import AbstractCdsAdaptor
from cads_adaptors.adaptor import Request

logger = logging.Logger(__name__)


def ensure_list(input_item):
    if not isinstance(input_item, list):
        return [input_item]
    return input_item


class MultiAdaptor(AbstractCdsAdaptor):
    # Alternatively inherit the DirectMarsCdsAdaptor class, but this may create an unwanted dependancy
    #  Also, this may not be required when all workers are turned into mars-workers
    resources = {"MARS_CLIENT": 1}

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
            elif key not in config.get("optional_keys", []):
                # If not an optional key, then return an empty dictionary.
                #  optional keys must be set in the adaptor.json via gecko
                return {}

        return this_request

    def retrieve(self, request: Request):
        from cads_adaptors.tools import adaptor_tools, download_tools

        download_format = request.pop("download_format", "zip")

        results = []
        exception_logs = {}
        for adaptor_tag, adaptor_desc in self.config["adaptors"].items():
            this_adaptor = adaptor_tools.get_adaptor(adaptor_desc, self.form)
            this_values = adaptor_desc.get("values", {})

            this_request = self.split_request(request, this_values, **self.config)
            logger.debug(f"{adaptor_tag}, request: {this_request}")
            if len(this_request) == 0:
                # if request is empty then continue
                continue
            this_request.setdefault("download_format", "list")
            # TODO: check this_request is valid for this_adaptor, or rely on try? i.e. split_request does
            #       NOT implement constraints.
            try:
                results += ensure_list(this_adaptor.retrieve(this_request))
            except Exception as err:
                # Catch any possible exception and store error message in case all adaptors fail
                logger.debug(f"{adaptor_tag} Error: {err}")
                exception_logs[adaptor_tag] = f"{err}"

        if len(results) == 0:
            raise RuntimeError(
                "MultiAdaptor returned no results, the error logs of the sub-adaptors is as follows:\n"
                f"{yaml.safe_dump(exception_logs)}"
            )

        # return self.merge_results(results, prefix=self.collection_id)
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

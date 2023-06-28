from typing import Any

import yaml  # type: ignore

# import os
from cads_adaptors.adaptor_cds import AbstractCdsAdaptor
from cads_adaptors.adaptor import Request


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
        for key, vals in full_request.items():
            this_request[key] = [v for v in vals if v in this_values.get(key, [])]

        # Check all required keys exist:
        if not all([key in this_request for key in config.get("required_keys", [])]):
            return {}

        return this_request

    @staticmethod
    def merge_results(results: list):
        """Basic results merge, creates a zip file containing all results."""
        import zipfile

        base_target = str(hash(tuple(results)))

        target = f"{base_target}.zip"

        with zipfile.ZipFile(target, mode="w") as archive:
            for p in results:
                archive.write(p)

        # TODO: clean up afterwards?
        # for p in results:
        #     os.remove(p)

        return target

    def __init__(self, form: dict[str, Any], **config: Any):
        from cads_adaptors.tools import adaptor_tools

        super().__init__(form, **config)
        self.adaptors = {}
        self.values = {}
        for adaptor_tag, adaptor_desc in config["adaptors"].items():
            self.adaptors[adaptor_tag] = adaptor_tools.get_adaptor(adaptor_desc, form)
            self.values[adaptor_tag] = adaptor_desc.get("values", {})

    def retrieve(self, request: Request):
        results = []
        exception_logs = {}
        for adaptor_tag, this_adaptor in self.adaptors.items():
            this_request = self.split_request(
                request, self.values[adaptor_tag], **self.config
            )

            # TODO: check this_request is valid for this_adaptor, or rely on try? i.e. split_request does
            #       NOT implement constraints.
            try:
                results.append(this_adaptor.retrieve(this_request))
            except Exception as err:
                # Catch any possible exception and store error message in case all adaptors fail
                exception_logs[adaptor_tag] = f"{err}"

        if len(results) == 0:
            raise RuntimeError(
                "MultiAdaptor returned no results, the error logs of the sub-adaptors is as follows:\n"
                f"{yaml.safe_dump(exception_logs)}"
            )

        return self.merge_results(results)

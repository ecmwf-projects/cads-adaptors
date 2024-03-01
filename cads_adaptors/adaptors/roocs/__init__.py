import os
from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request


class RoocsCdsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facets = self.config.get("facets", dict())
        self.facet_groups = self.config.get("facet_groups", dict())
        self.facets_order = self.config.get("facets_order", [])

    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools, url_tools

        os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"

        # switch off interactive logging to avoid threading issues
        os.environ["ROOK_MODE"] = "sync"

        import rooki

        request = mapping.apply_mapping(request, self.mapping)

        workflow = self.construct_workflow(request)
        response = rooki.rooki.orchestrate(workflow=workflow._serialise())

        response = workflow.orchestrate()

        try:
            urls = response.download_urls()
        except Exception:
            raise Exception(response.status)

        paths = url_tools.try_download(urls, context=self.context)

        return download_tools.DOWNLOAD_FORMATS["zip"](paths)

    def construct_workflow(self, request):
        os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"
        import rooki.operators as rookops

        from cads_adaptors.adaptors.roocs import operators

        facets = self.find_facets(request)

        dataset_id = ".".join(facets.values())
        variable_id = facets.get("variable", "")

        workflow = rookops.Input(variable_id, [dataset_id])

        for operator_class in operators.ROOKI_OPERATORS:
            operator = operator_class(request)
            kwargs = dict()
            for parameter in operator.parameters:
                if parameter.__name__ in request:
                    kwargs = operator.update_kwargs(kwargs, parameter())
            if kwargs:
                workflow = getattr(rookops, operator.ROOKI)(workflow, **kwargs)

        return workflow

    def find_facets(self, request):
        """
        Expand the CDS request into a full, unique set of facets for ROOCS.

        NOTE: This method assumes unique facets for each CDS request.
        """
        remap = self.mapping.get("remap", dict())

        request = {
            k: (v if not isinstance(v, list) else v[0]) for k, v in request.items()
        }
        request = {k: remap.get(k, dict()).get(v, v) for k, v in request.items()}

        print("REQUEST1: ", request)

        for key in self.facets[0]:
            if "-" in key:
                chunks = key.split("-")
                
                print("CHUNKS1: ", chunks)
                
                if "constraints" in self.config:
                    print("CONSTRAINTS")
                    key_mapping = {
                        value: key for key, value in self.config["constraints"].items()
                        if not isinstance(value, dict)
                    }
                    chunks = [key_mapping.get(chunk, chunk) for chunk in chunks]
                
                print("CHUNKS2: ", chunks)
                    
                request_chunks = [
                    request.get(item) for item in chunks
                    if request.get(item) not in [None, "None"]
                ]
                request[key] = "-".join(request_chunks)
                for chunk in chunks:
                    request.pop(chunk, None)
                
        print("REQUEST2: ", request)

        request = {k: v for k, v in request.items() if k in self.facets[0]}
        print("REQUEST3: ", request)

        for raw_candidate in self.facets:
            candidate = raw_candidate.copy()

            for key, groups in self.facet_groups.items():
                if key in candidate:
                    for group in groups:
                        if candidate[key] in groups[group]:
                            candidate[key] = group

            if candidate.items() >= request.items():
                break
        else:
            raise ValueError(f"No data found for request {request}")

        # raise ValueError(str(raw_candidate) + " | " + str(self.facets_order))
        return {key: raw_candidate[key] for key in self.facets_order}

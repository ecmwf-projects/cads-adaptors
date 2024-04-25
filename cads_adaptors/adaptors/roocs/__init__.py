import os
import re
from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request


class RoocsCdsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facets = self.config.get("facets", dict())
        self.facet_groups = self.config.get("facet_groups", dict())
        self.facet_search = self.config.get("facet_search", dict())
        self.facets_order = self.config.get("facets_order", [])
        self.operators = self.config.get("operators", dict())

    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools, url_tools

        os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"

        # switch off interactive logging to avoid threading issues
        os.environ["ROOK_MODE"] = "sync"

        import rooki

        request = mapping.apply_mapping(request, self.mapping)

        workflow = self.construct_workflow(request)
        
        raise ValueError(workflow._serialise())

        response = workflow.orchestrate()

        try:
            urls = response.download_urls()
        except Exception:
            raise Exception(response.status)
        
        urls += [response.provenance(), response.provenance_image()]

        paths = url_tools.try_download(urls, context=self.context)

        return download_tools.DOWNLOAD_FORMATS["zip"](paths)

    def construct_workflow(self, request):
        os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"
        import rooki.operators as rookops

        from cads_adaptors.adaptors.roocs import operators

        facets = self.find_facets(request)

        dataset_ids = [
            ".".join(facet for facet in sub_facets.values() if facet is not None)
            for sub_facets in facets
        ]
        variable_id = facets[0].get("variable", "")

        workflow = rookops.Input(variable_id, dataset_ids)
        
        for operator, operator_kwargs in self.operators.items():
            tmp_kwargs = operator_kwargs.copy()
            for key, value in operator_kwargs.items():
                if "." in value:
                    klass, method = value.split(".")
                    tmp_kwargs.pop(key)
                    tmp_kwargs = {
                        **tmp_kwargs,
                        **getattr(getattr(operators, klass.capitalize())(request), method)()
                    }
            workflow = getattr(rookops, operator)(workflow, **tmp_kwargs)

        for operator_class in operators.ROOKI_OPERATORS:
            operator = operator_class(request)
            kwargs = dict()
            for parameter in operator.parameters:
                if parameter.__name__ in request:
                    kwargs = operator.update_kwargs(kwargs, parameter())
            if kwargs:
                workflow = getattr(rookops, operator.ROOKI)(workflow, **kwargs)

        if list(eval(workflow._serialise())) == ["inputs", "doc"]:
            workflow = rookops.Subset(workflow)

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

        for key in self.facets[0]:
            if "-" in key:
                chunks = key.split("-")
                
                if "constraints_map" in self.config:
                    key_mapping = {
                        value: key for key, value in self.config["constraints_map"].items()
                        if not isinstance(value, dict)
                    }
                    chunks = [key_mapping.get(chunk, chunk) for chunk in chunks]
                    
                request_chunks = [
                    request.get(item) for item in chunks
                    if request.get(item) not in [None, "None"]
                ]
                request[key] = "-".join(request_chunks)
                for chunk in chunks:
                    request.pop(chunk, None)

        request = {k: v for k, v in request.items() if k in self.facets[0]}

        matched_facets = []

        for raw_candidate in self.facets:
            candidate = raw_candidate.copy()
            tmp_request = request.copy()

            for key, groups in self.facet_groups.items():
                if key in candidate:
                    for group in groups:
                        if candidate[key] in groups[group]:
                            tmp_request[key] = candidate[key]
            
            regex_facets = {
                key: self.facet_search[key].format(**{key: tmp_request.pop(key)})
                for key in self.facet_search
            }                                

            if candidate.items() <= tmp_request.items():
                print(candidate, tmp_request)
                for key, value in regex_facets.items():
                    if not re.search(value, candidate[key]):
                        break
                else:
                    matched_facets.append(candidate)
                    
            else:
                matched_facets.append(candidate)
                
        if not matched_facets:
            raise ValueError(f"No data found for request {request}")

        return [
            {key: final_candidate[key] for key in self.facets_order}
            for final_candidate in matched_facets
        ]

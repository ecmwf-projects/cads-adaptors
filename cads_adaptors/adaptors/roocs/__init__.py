import os
from typing import BinaryIO

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request

os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"


class RoocsCdsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools

        workflow = self.construct_workflow(request)
        response = workflow.orchestrate()

        try:
            urls = response.download_urls()
        except Exception:
            raise Exception(response.status)

        return download_tools.DOWNLOAD_FORMATS["zip"](urls)

    def construct_workflow(self, request):
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
        facets = self.config.get("facets", dict())

        for candidate in facets:
            if candidate.items() >= request.items():
                break
        else:
            raise ValueError(f"No data found for request {request}")

        return candidate
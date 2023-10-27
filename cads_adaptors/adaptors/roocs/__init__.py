import os
import socket
from typing import BinaryIO

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request
from cads_adaptors.tools.logger import logger


class RoocsCdsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facets = self.config.get("facets", dict())
        self.facet_groups = self.config.get("facet_groups", dict())
        self.facets_order = self.config.get("facets_order", [])

    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools

        os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"
        os.environ["ROOK_MODE"] = "foo"
        import rooki
        from requests import get

        ip = get("https://api.ipify.org").content.decode("utf8")

        workflow = self.construct_workflow(request)
        logger.info(workflow._serialise())
        logger.info(f"ROOK_URL: {os.environ['ROOK_URL']}")
        logger.info(socket.gethostbyname(socket.gethostname()))
        logger.info("My public IP address is: {}".format(ip))
        response = rooki.rooki.orchestrate(workflow=workflow._serialise())

        response = workflow.orchestrate()

        try:
            urls = response.download_urls()
        except Exception:
            raise Exception(response.status)

        return download_tools.DOWNLOAD_FORMATS["zip"](urls)

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
        request = {k: v for k, v in request.items() if k in self.facets[0]}

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

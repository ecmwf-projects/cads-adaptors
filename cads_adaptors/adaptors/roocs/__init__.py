
import os

from typing import BinaryIO

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request


os.environ["ROOK_URL"] = "http://rook.dkrz.de/wps"


class RoocsCdsAdaptor(AbstractCdsAdaptor):
    
    def retrieve(self, request: Request) -> BinaryIO:
        workflow = self.construct_workflow(request)
        with open("dummy.grib", "wb") as fp:
            with open("/dev/urandom", "rb") as random:
                while size > 0:
                    length = min(size, 10240)
                    fp.write(random.read(length))
                    size -= length
        return open("dummy.grib", "rb")

    def construct_workflow(self, request):
        from rooki import rooki  # rooki must be imported before rookops
        import rooki.operators as rookops

        facets = self.find_facets(request)
        
        dataset_id = ".".join(facets.values())
        variable_id = facets.get("variable", "")
        
        workflow = rookops.Input(variable_id, [dataset_id])
        
        
        
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
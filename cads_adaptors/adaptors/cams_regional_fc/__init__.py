from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.adaptors.cds import Request
from typing import BinaryIO
import json

# CHECKLIST:
# - LATEST: OK
# - ARCHIVE: OK
# - RATE LIMITING: OK
# - CACHE: OK

def debug_input(adaptor, request, message, output_file):
    # dumping the config and request
    with open(output_file, "w") as fp:
        fp.write("\n\n\n")
        fp.write(f'{message}')
        fp.write(json.dumps(adaptor.config))
        fp.write("\n\n\n")
        fp.write(json.dumps(request))
    return output_file
    

class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import new_cams_regional_fc
        self.context.add_stdout(f"----------> INITIAL REQUEST: {request}")
        request.pop('_in_adaptor_no_cache',None)
        self.context.add_stdout(f"----------> PURE REQUEST: {request}")
        
        # Intersect constraints
        if self.config.get("intersect_constraints", False):
            requests = self.intersect_constraints(request)
            # TODO: inhibit the cache avoidance mechanism from normalise_request
            # when called from intersect_constraints
            for subrequest in requests:
                 subrequest.pop('_in_adaptor_no_cache',None)
            if len(requests) == 0:
                msg = "Error: no intersection with the constraints."
                raise InvalidRequest(msg)
        else:
            requests = [request]
        self.context.add_stdout(f"----------> INTERSECTED REQUESTS: {requests}")
        
        # apply mapping
        requests = [self.apply_mapping(request) for request in requests]
        #self.mapped_request = self.apply_mapping(request)
        #self.context.add_stdout(f"----------> MAPPED REQUEST: {self.mapped_request}")
        #ALSO send [self.mapped_request] to new_cams_regional_fc instead of requests
        # request["type"] = [t.upper() for t in request["type"]]
        # request["format"] = ['grib']
        result_file = new_cams_regional_fc(self.context, self.config, self.mapping, requests)
            
        return open(result_file.path, "rb")

class CAMSEuropeAirQualityForecastsAdaptorForLatestData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import retrieve_latest
        #result_file_path = debug_input(self, request, "From CAMSEuropeAirQualityForecastsAdaptorForLatestData:", 'dummy.txt')
        result_file = retrieve_latest(self.context, request['requests'], request['dataset_dir'], request['integration_server'])
        if hasattr(result_file, "path"):
            return open(result_file.path, "rb")
        else:
            return None
    
class CAMSEuropeAirQualityForecastsAdaptorForArchivedData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import retrieve_archived
        #result_file_path = debug_input(self, request, "From CAMSEuropeAirQualityForecastsAdaptorForArchivedData:", 'dummy.txt')
        result_file = retrieve_archived(self.context, request['requests'], request['dataset_dir'], request['integration_server'])
        if hasattr(result_file, "path"):
            return open(result_file.path, "rb")
        else:
            return None

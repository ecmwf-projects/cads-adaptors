from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.adaptors.cds import Request
from typing import BinaryIO


class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import new_cams_regional_fc
        request.pop('_in_adaptor_no_cache',None)
        
        self.normalise_request(request)

        result_file = new_cams_regional_fc(self.context, self.config, self.mapping, self.mapped_requests)
            
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

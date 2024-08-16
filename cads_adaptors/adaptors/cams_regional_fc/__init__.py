from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.adaptors.cds import Request
from typing import Any, BinaryIO
import logging
import json

import time
import zipfile
import logging
from copy import deepcopy
from datetime import datetime, timedelta

import traceback
# from cdscompute.decorators import configure
from cdscompute.errors import NoDataException
from cds_common.cams.regional_fc_api import regional_fc_api
from .cams_regional_fc import cams_regional_fc,new_cams_regional_fc,retrieve_latest,retrieve_archived
from .api_retrieve import api_retrieve


# CHECKLIST:
# - LATEST: OK
# - ARCHIVE: OK
# - RATE LIMITING: TBD
# - CACHE: TBD

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
        request["type"] = [t.upper() for t in request["type"]]
        # request["format"] = ['grib']
        result_file = new_cams_regional_fc(self.context, self.config, [request])
            
        return open(result_file.path, "rb")

class CAMSEuropeAirQualityForecastsAdaptorForLatestData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        #result_file_path = debug_input(self, request, "From CAMSEuropeAirQualityForecastsAdaptorForLatestData:", 'dummy.txt')
        result_file = retrieve_latest(self.context, request['requests'], request['dataset_dir'], request['integration_server'])
        return open(result_file.path, "rb")
    
class CAMSEuropeAirQualityForecastsAdaptorForArchivedData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        #result_file_path = debug_input(self, request, "From CAMSEuropeAirQualityForecastsAdaptorForArchivedData:", 'dummy.txt')
        result_file = retrieve_archived(self.context, request['requests'], request['dataset_dir'], request['integration_server'])
        return open(result_file.path, "rb")

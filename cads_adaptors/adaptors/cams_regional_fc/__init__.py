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
from .cams_regional_fc import cams_regional_fc,new_cams_regional_fc
from .api_retrieve import api_retrieve


# CHECKLIST:
# - LATEST: OK
# - ARCHIVE: OK
# - RATE LIMITING: TBD
# - CACHE: TBD
class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        request["type"] = [t.upper() for t in request["type"]]
        request["format"] = ['grib']
        result_file = new_cams_regional_fc(self.context, self.config, [request])

        # # dumping the config and request
        # with open("dummy.grib", "w") as fp:
        #     fp.write("From inside cads_adaptors:\n\n\n")
        #     fp.write(json.dumps(self.config))
        #     fp.write("\n\n\n")
        #     fp.write(json.dumps(request))
        #     fp.write("\n\n\n")
        #     fp.write(json.dumps(result_file))
            
        return open(result_file.path, "rb")

# class CAMSEuropeAirQualityForecastsAdaptorForLatestData(AbstractCdsAdaptor):
#     def retrieve(self, request: Request) -> BinaryIO:
#         return None
    
# class CAMSEuropeAirQualityForecastsAdaptorForArchivedData(AbstractCdsAdaptor):
#     def retrieve(self, request: Request) -> BinaryIO:
#         return None

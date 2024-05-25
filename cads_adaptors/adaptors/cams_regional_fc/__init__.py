from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.adaptors import Request
from typing import Any, BinaryIO
import logging
import json

import time
import zipfile
import logging
from copy import deepcopy
from datetime import datetime, timedelta

from cdscompute.errors import NoDataException

from cds_common.cams.regional_fc_api import regional_fc_api
from cds_common.url2.downloader import Downloader
from cds_common.system import cds_forms_dir
from cds_common import hcube_tools, tree_tools, date_tools



class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        with open("dummy.grib", "w") as fp:
            fp.write("From inside cads_adaptors:\n\n\n")
            fp.write(json.dumps(self.config))
            fp.write("\n\n\n")
            fp.write(json.dumps(request))
            
        return open("dummy.grib", "rb")

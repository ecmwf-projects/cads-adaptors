from typing import BinaryIO

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request

STACK_TEMP_DIR = "/tmp/cams-europe-air-quality-forecasts/temp"
STACK_DOWNLOAD_DIR = "/tmp/cams-europe-air-quality-forecasts/download"


class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import cams_regional_fc

        request.pop("__in_adaptor_no_cache", None)
        self.normalise_request(request)

        # for now this is needed down the road to enforce the schema
        # in ./cds-common/cds_common/request_schemas/enforce_schema.py
        setattr(self.context, "request", {"mapping": self.mapping})

        result_file = cams_regional_fc(self.context, self.config, self.mapped_requests)

        return open(result_file.path, "rb")


class CAMSEuropeAirQualityForecastsAdaptorForLatestData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .subrequest_main import subrequest_main

        subrequest_main("latest", request, self.config, self.context)


class CAMSEuropeAirQualityForecastsAdaptorForArchivedData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .subrequest_main import subrequest_main

        subrequest_main("archived", request, self.config, self.context)

from typing import Any

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, CachingArgs, Request

STACK_TEMP_DIR = "/tmp/cams-europe-air-quality-forecasts/temp"
STACK_DOWNLOAD_DIR = "/tmp/cams-europe-air-quality-forecasts/download"
DEFAULT_NO_CACHE_KEY = "_no_cache"


class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.download_format = "as_source"

    def retrieve_list_of_results(
        self,
        mapped_requests: list[Request],
        area: list[float | int] | dict[str, float | int],
        post_process_steps: list[dict[str, Any]],
    ) -> list[str]:
        from .cams_regional_fc import cams_regional_fc

        # for now this is needed down the road to enforce the schema
        # in ./cds-common/cds_common/request_schemas/enforce_schema.py
        setattr(self.context, "request", {"mapping": self.mapping})

        result_file = cams_regional_fc(self.context, self.config, mapped_requests)

        return [result_file]


class CAMSEuropeAirQualityForecastsAdaptorForLatestData(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.download_format = "as_source"

    def get_caching_args(self, request: Request) -> CachingArgs:
        args = super().get_caching_args(request)
        args.must_be_one_mapped_request()
        return args

    def retrieve_list_of_results(
        self,
        mapped_requests: list[Request],
        area: list[float | int] | dict[str, float | int],
        post_process_steps: list[dict[str, Any]],
    ) -> list[str]:
        from .subrequest_main import subrequest_main

        (request,) = mapped_requests
        return [subrequest_main("latest", request, self.config, self.context)]


class CAMSEuropeAirQualityForecastsAdaptorForArchivedData(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.download_format = "as_source"

    def get_caching_args(self, request: Request) -> CachingArgs:
        args = super().get_caching_args(request)
        args.must_be_one_mapped_request()
        return args

    def retrieve_list_of_results(
        self,
        mapped_requests: list[Request],
        area: list[float | int] | dict[str, float | int],
        post_process_steps: list[dict[str, Any]],
    ) -> list[str]:
        from .subrequest_main import subrequest_main

        (request,) = mapped_requests
        return [subrequest_main("archived", request, self.config, self.context)]

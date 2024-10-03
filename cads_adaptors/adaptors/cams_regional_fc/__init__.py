from typing import BinaryIO

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request


class CAMSEuropeAirQualityForecastsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import cams_regional_fc

        request.pop("_in_adaptor_no_cache", None)
        self.normalise_request(request)

        # for now this is needed down the road to enforce the schema
        # in ./cds-common/cds_common/request_schemas/enforce_schema.py
        self.context.request = {"mapping": self.mapping}

        result_file = cams_regional_fc(self.context, self.config, self.mapped_requests)

        return open(result_file.path, "rb")


class CAMSEuropeAirQualityForecastsAdaptorForLatestData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import retrieve_latest

        message = (
            f"The parent request is {request['parent_request_uid']}, "
            "launched by user {request['parent_request_user_uid']}."
        )
        self.context.add_stdout(message)

        result_file = retrieve_latest(
            self.context,
            request["requests"],
            request["dataset_dir"],
            request["integration_server"],
        )
        if hasattr(result_file, "path"):
            return open(result_file.path, "rb")
        else:
            return None


class CAMSEuropeAirQualityForecastsAdaptorForArchivedData(AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        from .cams_regional_fc import retrieve_archived

        message = (
            f"The parent request is {request['parent_request_uid']}, "
            "launched by user {request['parent_request_user_uid']}."
        )
        self.context.add_stdout(message)

        result_file = retrieve_archived(
            self.context,
            request["requests"],
            request["dataset_dir"],
            request["integration_server"],
        )
        if hasattr(result_file, "path"):
            return open(result_file.path, "rb")
        else:
            return None

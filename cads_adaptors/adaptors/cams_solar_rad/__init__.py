from typing import Any, BinaryIO

from cads_adaptors.adaptors.cams_solar_rad.functions import (
    BadRequest,
    NoData,
    determine_result_filename,
    get_numeric_user_id,
    solar_rad_retrieve,
)
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request
from cads_adaptors.exceptions import InvalidRequest


class CamsSolarRadiationTimeseriesAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Schema required to ensure adaptor will not fall over with an uncaught exception
        self.schemas.append(
            {
                "_draft": "7",
                "type": "object",  # Request should be a single dict
                "required": [  # ... with at least these keys
                    "sky_type",
                    "location",
                    "altitude",
                    "date",
                    "time_step",
                    "time_reference",
                    "format",
                ],
                "properties": {
                    "sky_type": {"type": "string"},
                    "location": {
                        "type": "object",
                        "properties": {
                            "latitude": {
                                "maximum": 90.0,
                                "minimum": -90.0,
                                "type": "number",
                            },
                            "longitude": {
                                "maximum": 180.0,
                                "minimum": -180.0,
                                "type": "number",
                            },
                        },
                    },
                    "altitude": {"type": "string", "format": "numeric string"},
                    "date": {"type": "string", "format": "date range"},
                    "time_step": {"type": "string"},
                    "time_reference": {"type": "string"},
                    "format": {"type": "string"},
                },
                "_defaults": {"format": "csv"},
            }
        )

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """Implemented in normalise_request, before the mapping is applied."""
        request = super().pre_mapping_modifications(request)

        default_download_format = "as_source"
        download_format = request.pop("download_format", default_download_format)
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        return request

    def retrieve(self, request: Request) -> BinaryIO:
        self.context.debug(f"Request is {request!r}")

        self.normalise_request(request)
        try:
            assert len(self.mapped_requests) == 1
        except AssertionError:
            if len(self.mapped_requests) == 0:
                msg = "Error: no intersection with the constraints."
            else:
                msg = "Error: unexpected intersection with more than 1 constraint."
            self.context.add_user_visible_log(
                f"WARNING: More than one request was mapped: {self.mapped_requests}, "
                f"returning the first one only:\n{self.mapped_requests[0]}"
            )
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        mreq = self.mapped_requests[0]
        self.context.debug(f"Mapped request is {mreq!r}")

        numeric_user_id = get_numeric_user_id(self.config["user_uid"])
        result_filename = determine_result_filename(self.config, mreq)

        try:
            solar_rad_retrieve(
                mreq,
                user_id=numeric_user_id,
                outfile=result_filename,
                logger=self.context,
            )

        except (BadRequest, NoData) as e:
            msg = e.args[0]
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        return open(result_filename, "rb")

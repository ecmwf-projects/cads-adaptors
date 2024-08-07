from typing import BinaryIO

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

    def retrieve(self, request: Request) -> BinaryIO:
        self.context.debug(f"Request is {request!r}")

        # Intersect constraints
        if self.config.get("intersect_constraints", False):
            requests = self.intersect_constraints(request)
            if len(requests) != 1:
                if len(requests) == 0:
                    msg = "Error: no intersection with the constraints."
                else:
                    # TODO: check if indeed this can never happen
                    msg = "Error: unexpected intersection with more than 1 constraint."
                self.context.add_user_visible_error(msg)
                raise InvalidRequest(msg)
            request_after_intersection = requests[0]
        else:
            request_after_intersection = request

        # Apply mapping
        self._pre_retrieve(
            request_after_intersection, default_download_format="as_source"
        )
        mreq = self.mapped_request
        self.context.debug(f"Mapped request is {mreq!r}")

        numeric_user_id = get_numeric_user_id(self.config["user_uid"])
        result_filename = determine_result_filename(
            self.config, request_after_intersection
        )

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

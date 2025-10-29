from typing import Any, BinaryIO

from cads_adaptors.adaptors.cams_solar_rad.functions import (
    BadRequest,
    NoData,
    solar_rad_retrieve,
)
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request
from cads_adaptors.exceptions import InvalidRequest


class CamsSolarRadiationTimeseriesAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Schema required to ensure adaptor will not fall over with an uncaught exception.
        # This is here rather than in the config because it's fundamentally tied to the
        # code. It defines the assumptions that the code can safely make about the
        # request.
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
                ],
                "properties": {
                    "sky_type": {"type": "string"},
                    "location": {
                        "type": "object",
                        "required": ["latitude", "longitude"],
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
                    "data_format": {"type": "string"},
                },
            }
        )

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """Implemented in normalise_request, before the mapping is applied."""
        request = super().pre_mapping_modifications(request)

        # Rename format to data_format for backwards compatibility with old key
        # name. This can't be done in the usual way (using remapping) because
        # data_format is in the constraints and remapping isn't done until after
        # the constraints are applied.
        request.setdefault("data_format", request.pop("format", "csv"))

        return request

    def retrieve(self, request: Request) -> BinaryIO:
        self.context.debug(f"Request is {request!r}")

        self.normalise_request(request)

        # Intersecting the constraints should never result in >1 request
        if len(self.mapped_requests) != 1:
            msg = (
                f"Request pre-processing resulted in {len(self.mapped_requests)} "
                "requests"
            )
            self.context.error(f"{msg}: {self.mapped_requests!r}")
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        mreq = self.mapped_requests[0]
        self.context.debug(f"Mapped request is {mreq!r}")

        # Although the schema should have ensured that each value is a scalar,
        # applying the constraints will have turned them back to one-element
        # strings. Turn back into scalars.
        for k, v in mreq.items():
            if isinstance(v, list):
                mreq[k] = v[0]

        outfile = self._result_filename(mreq)

        try:
            solar_rad_retrieve(
                mreq,
                user_id=self._user_id(mreq),
                outfile=outfile,
                logger=self.context,
            )

        except (BadRequest, NoData) as e:
            msg = e.args[0]
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        return open(outfile, "rb")

    def _user_id(self, mreq):
        """Return the current user ID, unless the current user is Wekeo and they've
        provided a user ID in the request, in which case return that. This means that Wekeo
        users don't get considered as a single user by the backend provider, and so are
        allowed separate individual quotas for the number of requests they can make per day.
        """
        req_user_id = mreq.get("_user_id")
        while isinstance(req_user_id, (list, tuple)) and req_user_id:
            req_user_id = req_user_id[0]
        self.context.debug(
            "Wekeo user IDs are " + repr(self.config.get("wekeo_user_ids"))
        )
        if req_user_id and self.config["user_uid"] in self.config.get(
            "wekeo_user_ids", []
        ):
            self.context.info(f"Using WEKEO user ID for backend: {req_user_id}")
            return str(req_user_id)
        else:
            return self.config["user_uid"]

    def _result_filename(self, request):
        request_uid = self.config.get("request_uid", "no-request-uid")
        extension = {"csv": "csv", "csv_expert": "csv", "netcdf": "nc"}.get(
            request["data_format"]
        )
        if not extension:
            raise Exception("Unrecognised format: " + repr(request["data_format"]))
        return f"result-{request_uid}.{extension}"

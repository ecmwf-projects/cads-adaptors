import copy
import tempfile
from datetime import UTC, datetime, timedelta
from typing import Any

from dateutil.parser import parse as dtparse

from cads_adaptors.adaptors import Request, cds
from cads_adaptors.exceptions import ArcoDataLakeNoDataError, InvalidRequest
from cads_adaptors.tools.general import ensure_list

LAT_NAME = "latitude"
LON_NAME = "longitude"
DEFAULT_DATA_FORMAT = "netcdf"
DATA_FORMATS = {
    "netcdf": ["netcdf", "netcdf4", "nc"],
    "csv": ["csv"],
}
NAME_DICT = {
    "time": "valid_time",
}
DEFAULT_AREA = [90, -180, -90, 180]
DEFAULT_MAXIMUM_AREA_EXTENT = {"latitude": 1, "longitude": 1}


class ArcoDataLakeCdsAdaptor(cds.AbstractCdsAdaptor):
    def _normalise_variable(self, request: Request) -> None:
        variable = sorted(ensure_list(request.get("variable")))
        if not variable:
            raise InvalidRequest("Please specify at least one variable.")
        request["variable"] = variable

    def _normalise_location(self, request: Request) -> None:
        if "location" not in request:
            return

        locations = ensure_list(request.get("location"))
        msg = (
            "Please specify a single valid location using the format "
            f"{dict.fromkeys([LAT_NAME, LON_NAME], 0)}."
        )
        if len(locations) != 1:
            raise InvalidRequest(msg)
        (location,) = locations
        if not isinstance(location, dict) or not set(location) == {LAT_NAME, LON_NAME}:
            raise InvalidRequest(f"Invalid {location=}. {msg}")
        try:
            request["location"] = {k: float(v) for k, v in sorted(location.items())}
        except (ValueError, TypeError):
            raise InvalidRequest(f"Invalid {location=}. {msg}")

    def _normalise_area(self, request: Request) -> None:
        if "area" not in request:
            return

        area = ensure_list(request.get("area")) or DEFAULT_AREA
        msg = "Please specify the `area` parameter in the form [north, west, south, east]."
        if len(area) != 4:
            raise InvalidRequest(msg)
        try:
            area = list(map(float, area))
        except ValueError:
            raise InvalidRequest(msg)
        if area[0] < area[2] or area[3] < area[1]:
            raise InvalidRequest(msg)

        max_extent = self.config.get("maximum_area_extent", DEFAULT_MAXIMUM_AREA_EXTENT)
        extent = {LAT_NAME: abs(area[0] - area[2]), LON_NAME: abs(area[1] - area[3])}
        for k, v in extent.items():
            if k in max_extent and v > max_extent[k]:
                raise InvalidRequest(
                    f"{LAT_NAME} exceeds the maximum extent allowed ({max_extent[k]}°). Received: {v}°."
                )
        request["area"] = area

    def _normalise_date(self, request: Request) -> None:
        date_key = self.config.get("date_key", "date")
        date = ensure_list(request.get(date_key))
        date_range = sorted(str(date[0]).split("/") if len(date) == 1 else date)
        if len(date_range) == 1:
            date_range *= 2
        if len(date_range) != 2:
            raise InvalidRequest(
                'Please specify a single date range using the format "yyyy-mm-dd/yyyy-mm-dd" or '
                '["yyyy-mm-dd", "yyyy-mm-dd"].'
            )

        # Embargo check
        if "embargo" in self.config and self.config["embargo"]:
            embargo = self.config["embargo"]
            embargo_error_time_format: str = embargo.pop(
                "error_time_format",
                "%Y-%m-%d",  # Default to daily embargo
            )
            embargo_datetime = datetime.now(UTC) - timedelta(**embargo)
            if dtparse(date_range[0]).date() > embargo_datetime.date():
                raise InvalidRequest(
                    "You have requested data under embargo, the latest available data is: "
                    f" {embargo_datetime.strftime(embargo_error_time_format)}"
                )
            if dtparse(date_range[1]).date() > embargo_datetime.date():
                date_range[1] = embargo_datetime.strftime(embargo_error_time_format)
                self.context.add_user_visible_error(
                    "Part of the data you have requested is under embargo, "
                    "your request has been modified to the latest available data: "
                    f"{date_key}={date_range}"
                )
        request[date_key] = date_range

    def _normalise_data_format(self, request: Request) -> None:
        data_formats = ensure_list(request.get("data_format", DEFAULT_DATA_FORMAT))
        if len(data_formats) != 1:
            raise InvalidRequest("Please specify a single data_format.")
        (data_format,) = data_formats
        available_options = set()
        for key, value in DATA_FORMATS.items():
            available_options.update(value)
            if isinstance(data_format, str) and data_format.lower() in value:
                request["data_format"] = key
                return
        raise InvalidRequest(
            f"Invalid {data_format=}. Available options: {available_options}"
        )

    def pre_mapping_modifications(self, request: Request) -> Request:
        request = super().pre_mapping_modifications(request)

        download_format = request.pop("download_format", "as_source")
        self.set_download_format(download_format)
        return request

    def normalise_request(self, request: Request) -> Request:
        if self.normalised:
            return request

        if len({"area", "location"} & set(request)) != 1:
            raise InvalidRequest(
                "The parameters `area` and `request` are mutually exclusive, and one of them is required."
            )

        request = copy.deepcopy(request)
        self._normalise_variable(request)
        self._normalise_location(request)
        self._normalise_area(request)
        self._normalise_date(request)
        self._normalise_data_format(request)

        request = super().normalise_request(request)
        if len(self.mapped_requests) != 1:
            raise InvalidRequest("Empty or multiple requests are not supported.")

        return dict(sorted(request.items()))

    def retrieve_list_of_results(self, request: Request) -> list[str]:
        import xarray as xr

        self.normalise_request(request)  # Needed to populate self.mapped_requests
        (request,) = self.mapped_requests

        try:
            ds = xr.open_dataset(self.config["url"], engine="zarr")
        except Exception:
            self.context.add_user_visible_error(
                "Cannot access the ARCO Data Lake.\n"
                "This may be due to temporary connectivity issues with the source data.\n"
                "If this problem persists, please contact user support."
            )
            raise

        try:
            ds = ds[ensure_list(request["variable"])]
        except KeyError as exc:
            self.context.add_user_visible_error(f"Invalid variable: {exc}.")
            raise

        # Normalised request is guarenteed to have a value for date_key, set to a list of two values
        date_range = request[self.config.get("date_key", "date")]
        source_date_key = self.config.get("source_date_key", "time")
        selection: dict[str, Any] = {source_date_key: slice(*date_range)}
        try:
            ds = ds.sel(**selection)
        except TypeError:
            self.context.add_user_visible_error(f"Invalid {date_range=}.")
            raise
        if not ds.sizes[source_date_key]:
            msg = f"No data found for {date_range=}."
            self.context.add_user_visible_error(msg)
            raise ArcoDataLakeNoDataError(msg)

        if "location" in request:
            method = "nearest"
            indexers = request["location"]
        else:
            method = None
            indexers = {
                LAT_NAME: slice(request["area"][2], request["area"][0]),
                LON_NAME: slice(request["area"][1], request["area"][3]),
            }
        ds = ds.sel(indexers, method=method)
        if not all([ds.sizes.get(dim, 1) for dim in indexers]):
            msg = f"No data found for {indexers=}."
            self.context.add_user_visible_error(msg)
            raise ArcoDataLakeNoDataError(msg)

        ds = ds.rename(NAME_DICT)

        match request["data_format"]:
            case "netcdf":
                _, path = tempfile.mkstemp(
                    prefix=self.config.get("collection-id", "arco-data"),
                    suffix=".nc",
                    dir=self.cache_tmp_path,
                )
                ds.to_netcdf(path)
            case "csv":
                _, path = tempfile.mkstemp(
                    prefix=self.config.get("collection-id", "arco-data"),
                    suffix=".csv",
                    dir=self.cache_tmp_path,
                )
                ds.to_pandas().to_csv(path)
            case data_format:
                raise NotImplementedError(f"Invalid {data_format=}.")

        return [str(path)]

import copy
import tempfile

from cads_adaptors.adaptors import Request, cds
from cads_adaptors.exceptions import ArcoDataLakeNoDataError, InvalidRequest
from cads_adaptors.tools.general import ensure_list

SPATIAL_COORDINATES = {"latitude", "longitude"}
DEFAULT_DATA_FORMAT = "netcdf"
DATA_FORMATS = {
    "netcdf": ["netcdf", "netcdf4", "nc"],
    "csv": ["csv"],
}
NAME_DICT = {
    "time": "valid_time",
}


class ArcoDataLakeCdsAdaptor(cds.AbstractCdsAdaptor):
    def _normalise_variable(self, request: Request) -> None:
        variable = sorted(ensure_list(request.get("variable")))
        if not variable:
            raise InvalidRequest("Please specify at least one variable.")
        request["variable"] = variable

    def _normalise_location(self, request: Request) -> None:
        locations = ensure_list(request.get("location"))
        msg = "Please specify a single valid location using the format {'latitude': 0, 'longitude': 0}"
        if len(locations) != 1:
            raise InvalidRequest(msg)
        (location,) = locations
        if not isinstance(location, dict) or not set(location) == SPATIAL_COORDINATES:
            raise InvalidRequest(f"Invalid {location=}. {msg}")
        try:
            request["location"] = {k: float(v) for k, v in sorted(location.items())}
        except (ValueError, TypeError):
            raise InvalidRequest(f"Invalid {location=}. {msg}")

    def _normalise_date(self, request: Request) -> None:
        date = ensure_list(request.get("date"))
        if not date:
            request["date"] = date
            return

        if len(date) != 1:
            raise InvalidRequest(
                "Please specify a single date range using the format yyyy-mm-dd/yyyy-mm-dd."
            )
        split = sorted(str(date[0]).split("/"))
        request["date"] = ["/".join([split[0], split[-1]])]

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

    def normalise_request(self, request: Request) -> Request:
        if self.normalised:
            return request

        request = copy.deepcopy(request)
        self._normalise_variable(request)
        self._normalise_location(request)
        self._normalise_date(request)
        self._normalise_data_format(request)

        request = super().normalise_request(request)
        if len(self.mapped_requests) != 1:
            raise InvalidRequest("Empty or multiple requests are not supported.")

        return dict(sorted(request.items()))

    def retrieve_list_of_results(self, request: Request) -> list[str]:
        import dask
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

        if date := request["date"]:
            try:
                ds = ds.sel(time=slice(*date[0].split("/")))
            except TypeError:
                self.context.add_user_visible_error(f"Invalid {date=}")
                raise
            if not ds.sizes["time"]:
                msg = f"No data found for {date=}"
                self.context.add_user_visible_error(msg)
                raise ArcoDataLakeNoDataError(msg)

        ds = ds.sel(request["location"], method="nearest")
        ds = ds.rename(NAME_DICT)

        with dask.config.set(scheduler="threads"):
            match request["data_format"]:
                case "netcdf":
                    _, path = tempfile.mkstemp(suffix=".nc", dir=self.cache_tmp_path)
                    ds.to_netcdf(path)
                case "csv":
                    _, path = tempfile.mkstemp(suffix=".csv", dir=self.cache_tmp_path)
                    ds.to_pandas().to_csv(path)
                case data_format:
                    raise NotImplementedError(f"Invalid {data_format=}.")

        self.download_format = "as_source"  # Prevent from writing a zip file
        return [str(path)]

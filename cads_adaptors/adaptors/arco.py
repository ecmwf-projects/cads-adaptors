import copy

from cads_adaptors.adaptors import Request, cds
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools.general import ensure_list

DEFAULT_LOCATION = {"latitude": 0, "longitude": 0}
DEFAULT_DATA_FORMAT = "netcdf"
DATA_FORMATS = {
    "netcdf": ["netcdf", "netcdf4", "nc"],
    "csv": ["csv"],
}
NAME_DICT = {
    "time": "valid_time",
}


class ArcoDataLakeCdsAdaptor(cds.AbstractCdsAdaptor):
    def normalise_request(self, request: Request) -> Request:
        if self.normalised:
            return request

        request = copy.deepcopy(request)
        request["variable"] = sorted(ensure_list(request.get("variable")))

        location = request.get("location", DEFAULT_LOCATION)
        if (
            not isinstance(location, dict)
            or not set(location) >= set(DEFAULT_LOCATION)
            or not all(isinstance(v, int | float) for v in location.values())
        ):
            raise InvalidRequest(f"Invalid {location=}.")
        request["location"] = {k: float(location[k]) for k in sorted(DEFAULT_LOCATION)}

        data_format = request.get("data_format", DEFAULT_DATA_FORMAT)
        for key, value in DATA_FORMATS.items():
            if data_format in value:
                request["data_format"] = key
                break
        else:
            raise InvalidRequest(f"Invalid {data_format=}.")

        request = super().normalise_request(request)
        if len(self.mapped_requests) != 1:
            raise InvalidRequest("Empty or multiple requests are not supported.")

        return request

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

        if variable := request["variable"]:
            try:
                ds = ds[ensure_list(variable)]
            except KeyError as exc:
                self.context.add_user_visible_error(f"Invalid variable: {exc}")
                raise

        ds = ds.sel(request["location"], method="nearest")
        ds = ds.rename(NAME_DICT)

        file_path = self.cache_tmp_path / "data"
        with dask.config.set(scheduler="threads"):
            match request["data_format"]:
                case "netcdf":
                    file_path = file_path.with_suffix(".nc")
                    ds.to_netcdf(file_path)
                case "csv":
                    file_path = file_path.with_suffix(".csv")
                    ds.to_pandas().to_csv(file_path)
                case data_format:
                    raise NotImplementedError(f"Invalid {data_format=}.")

        self.download_format = "as_source"  # Prevent from writing a zip file
        return [str(file_path)]

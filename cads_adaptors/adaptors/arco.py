import os
from typing import BinaryIO

from cads_adaptors.adaptors import Request, cds
from cads_adaptors.tools.general import ensure_list

DEFAULT_BASE_URL = (
    "https://s3.cds.ecmwf.int/swift/v1/AUTH_cf0252abd9434b5e859bc07c159ec2f4"
)


class ArcoCdsAdaptor(cds.AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        self.context.add_stdout(f"ARCO DEBUG: {request}")
        import xarray as xr

        self._pre_retrieve(request=request)
        BASEURL = self.config.get("BASEURL", DEFAULT_BASE_URL)
        BUCKET = self.config.get(
            "BUCKET", "cadl-arco-geo/arco/era5/sfc/geoChunked.zarr"
        )

        arco_ds = xr.open_zarr(os.path.join(BASEURL, BUCKET))
        self.context.add_stdout(f"ARCO DEBUG, connect to BUCKET: {arco_ds}")

        variables = ensure_list(self.mapped_request.get("variable", []))

        arco_ds = arco_ds[variables]
        self.context.add_stdout(f"{self.mapped_request}")

        location = self.mapped_request.get("location", {"latitude": 0, "longitude": 0})
        latitude = location["latitude"]
        longitude = location["longitude"]

        dates = self.mapped_request.get("date", [])
        date_slice = slice(dates[0], dates[-1])

        arco_ds = arco_ds.sel(latitude=latitude, longitude=longitude, time=date_slice)

        self.context.add_stdout(f"ARCO DEBUG: {arco_ds}")

        arco_ds.to_netcdf("output.nc")

        return self.make_download_object("output.nc")

import os
from typing import Any
from cads_adaptors.adaptors import Context

STANDARD_COMPRESSION_OPTIONS = {
    "default": {
        "compression": "gzip",
        "compression_opts": 9,
        "shuffle": True,
        "engine": "h5netcdf",
    }
}


def grib_to_netcdf_files(
    grib_file: str,
    compression_options: None | str | dict[str, Any] = None,
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    context: Context = Context(),
    **to_netcdf_kwargs,
):
    fname, _ = os.path.splitext(os.path.basename(grib_file))
    grib_file = os.path.realpath(grib_file)

    import cfgrib
    import dask
    import xarray as xr

    with dask.config.set(scheduler="threads"):
        if open_datasets_kwargs is None:
            open_datasets_kwargs = {
                "chunks": {"time": 1, "step": 1, "plev": 1}  # Auto chunk by field
            }

        # Option for manual split of the grib file into list of xr.Datasets using list of open_ds_kwargs
        if isinstance(open_datasets_kwargs, list):
            datasets: list[xr.Dataset] = []
            for open_ds_kwargs in open_datasets_kwargs:
                # Default engine is cfgrib
                open_ds_kwargs.setdefault("engine", "cfgrib")
                ds = xr.open_dataset(grib_file, **open_ds_kwargs)
                if ds:
                    datasets.append(ds)
        else:
            datasets = cfgrib.open_datasets(grib_file, **open_datasets_kwargs)

        if len(datasets) == 0:
            message = (
                "We are unable to convert this GRIB data to netCDF, "
                "please download as GRIB and convert to netCDF locally.\n"
            )
            context.add_user_visible_error(message=message)
            context.add_stderr(message=message)
            raise RuntimeError(message)

        if isinstance(compression_options, str):
            compression_options = STANDARD_COMPRESSION_OPTIONS.get(
                compression_options, {}
            )

        if compression_options is not None:
            to_netcdf_kwargs.setdefault(
                "engine", compression_options.pop("engine", "h5netcdf")
            )

        out_nc_files = []
        for i, dataset in enumerate(datasets):
            if compression_options is not None:
                to_netcdf_kwargs.update(
                    {
                        "encoding": {var: compression_options for var in dataset},
                    }
                )
            out_fname = f"{fname}_{i}.nc"
            dataset.to_netcdf(out_fname, **to_netcdf_kwargs)
            out_nc_files.append(out_fname)

    return out_nc_files

import os
from typing import Any

from cads_adaptors.adaptors import Context

STANDARD_COMPRESSION_OPTIONS = {
    "default": {
        "compression": "lzf",
        "engine": "h5netcdf",
    }
}


def grib_to_netcdf_files(
    grib_file: str,
    compression_options: str | dict[str, Any] = "default",
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    context: Context = Context(),
    out_fname_tag: str = "",
    **to_netcdf_kwargs,
):
    context.add_stdout(
        f"Converting {grib_file} to netCDF files with:\n"
        f"to_netcdf_kwargs: {to_netcdf_kwargs}\n"
        f"compression_options: {compression_options}\n"
        f"open_datasets_kwargs: {open_datasets_kwargs}\n"
    )
    fname, _ = os.path.splitext(os.path.basename(grib_file))
    grib_file = os.path.realpath(grib_file)
    # Allow renaming of variables from what cfgrib decides
    rename: dict[str, str] = to_netcdf_kwargs.pop("rename", {})
    # Squeeze any unused dimensions
    squeeze: bool = to_netcdf_kwargs.pop("squeeze", False)
    # Allow expanding of dimensionality, e.g. to ensure that time is always a dimension
    # (this is applied after squeezing)
    expand_dims: list[str] = to_netcdf_kwargs.pop("expand_dims", [])

    import cfgrib
    import dask
    import xarray as xr

    with dask.config.set(scheduler="threads"):
        if open_datasets_kwargs is None:
            open_datasets_kwargs = {
                "chunks": {
                    "time": 12,
                    "step": 1,
                    "plev": 1,
                    "valid_time": 12,
                }  # Auto chunk 12 time steps
            }

        # Option for manual split of the grib file into list of xr.Datasets using list of open_ds_kwargs
        context.add_stdout(f"Opening {grib_file} with kwargs: {open_datasets_kwargs}")
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

        to_netcdf_kwargs.setdefault(
            "engine", compression_options.pop("engine", "h5netcdf")
        )

        out_nc_files = []
        for i, dataset in enumerate(datasets):
            if squeeze:
                dataset = dataset.squeeze(drop=True)
            for old_name, new_name in rename.items():
                if old_name in dataset:
                    dataset = dataset.rename({old_name: new_name})
            for dim in expand_dims:
                if dim in dataset:
                    dataset = dataset.expand_dims(dim)
            to_netcdf_kwargs.update(
                {
                    "encoding": {var: compression_options for var in dataset},
                }
            )
            out_fname = f"{fname}_{i}{out_fname_tag}.nc"
            context.add_stdout(f"Writing {out_fname} with kwargs: {to_netcdf_kwargs}")
            dataset.to_netcdf(out_fname, **to_netcdf_kwargs)
            out_nc_files.append(out_fname)

    return out_nc_files

import os
from typing import Any

import cfgrib
import xarray as xr

from cads_adaptors.adaptors import Context

STANDARD_COMPRESSION_OPTIONS = {
    "default": {
        "zlib": True,
        "complevel": 1,
        "shuffle": True,
        "engine": "h5netcdf",
    }
}


DEFAULT_CHUNKS = {
    "time": 12,
    "step": 1,
    "isobaricInhPa": 1,
    "hybrid": 1,
    "valid_time": 12,
    "number": 1,
    "realization": 1,
    "depthBelowLandLayer": 1,
}


def grib_to_netcdf_files(
    grib_file: str,
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    context: Context = Context(),
    **to_netcdf_kwargs,
):
    grib_file = os.path.realpath(grib_file)

    context.add_stdout(
        f"Converting {grib_file} to netCDF files with:\n"
        f"to_netcdf_kwargs: {to_netcdf_kwargs}\n"
        f"open_datasets_kwargs: {open_datasets_kwargs}\n"
    )

    datasets = open_grib_file_as_xarray_dictionary(
        grib_file, open_datasets_kwargs=open_datasets_kwargs, context=context
    )
    # Fail here on empty lists so that error message is more informative
    if len(datasets) == 0:
        message = (
            "We are unable to convert this GRIB data to netCDF, "
            "please download as GRIB and convert to netCDF locally.\n"
        )
        context.add_user_visible_error(message=message)
        context.add_stderr(message=message)
        raise RuntimeError(message)

    out_nc_files = xarray_dict_to_netcdf(datasets, context=context, **to_netcdf_kwargs)

    return out_nc_files


def xarray_dict_to_netcdf(
    datasets: dict[str | int, xr.Dataset],
    context: Context = Context(),
    compression_options: str | dict[str, Any] = "default",
    out_fname_prefix: str = "",
    **to_netcdf_kwargs,
):
    if isinstance(compression_options, str):
        compression_options = STANDARD_COMPRESSION_OPTIONS.get(compression_options, {})

    to_netcdf_kwargs.setdefault("engine", compression_options.pop("engine", "h5netcdf"))

    # Allow renaming of variables from what cfgrib decides
    rename: dict[str, str] = to_netcdf_kwargs.pop("rename", {})
    # Squeeze any unused dimensions
    squeeze: bool = to_netcdf_kwargs.pop("squeeze", False)
    # Allow expanding of dimensionality, e.g. to ensure that time is always a dimension
    # (this is applied after squeezing)
    expand_dims: list[str] = to_netcdf_kwargs.pop("expand_dims", [])
    out_nc_files = []
    for out_fname_base, dataset in datasets.items():
        if squeeze:
            dataset = dataset.squeeze(drop=True)
        for old_name, new_name in rename.items():
            if old_name in dataset:
                dataset = dataset.rename({old_name: new_name})
        for dim in expand_dims:
            if dim in dataset and dim not in dataset.dims:
                dataset = dataset.expand_dims(dim)
        to_netcdf_kwargs.update(
            {
                "encoding": {var: compression_options for var in dataset},
            }
        )
        out_fname = f"{out_fname_prefix}{out_fname_base}.nc"
        context.add_stdout(f"Writing {out_fname} with kwargs:\n{to_netcdf_kwargs}")
        dataset.to_netcdf(out_fname, **to_netcdf_kwargs)
        out_nc_files.append(out_fname)

    return out_nc_files


def open_grib_file_as_xarray_dictionary(
    grib_file: str,
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    context: Context = Context(),
) -> dict[str | int, xr.Dataset]:
    """
    Open a grib file and return as a dictionary of xarray datasets,
    where the key will be used in any filenames created from the dataset.
    """
    fname, _ = os.path.splitext(os.path.basename(grib_file))
    if open_datasets_kwargs is None:
        open_datasets_kwargs = {}

    # Option for manual split of the grib file into list of xr.Datasets using list of open_ds_kwargs
    context.add_stdout(f"Opening {grib_file} with kwargs: {open_datasets_kwargs}")
    if isinstance(open_datasets_kwargs, list):
        datasets: dict[str | int, xr.Dataset] = {}
        for i, open_ds_kwargs in enumerate(open_datasets_kwargs):
            # Default engine is cfgrib
            open_ds_kwargs.setdefault("engine", "cfgrib")
            open_ds_kwargs.setdefault("chunks", DEFAULT_CHUNKS)
            ds_tag = open_ds_kwargs.pop("tag", i)
            try:
                ds = xr.open_dataset(grib_file, **open_ds_kwargs)
            except Exception:
                ds = None
            if ds:
                datasets[f"{fname}_{ds_tag}"] = ds
    else:
        open_datasets_kwargs.setdefault("chunks", DEFAULT_CHUNKS)
        # First try and open with xarray as a single dataset,
        # xarray.open_dataset will handle a number of the potential conflicts in fields
        try:
            ds_tag = open_datasets_kwargs.pop("tag", 0)
            datasets = {
                f"{fname}_{ds_tag}": xr.open_dataset(
                    grib_file, **{**{"errors": "raise"}, **open_datasets_kwargs}
                )
            }
        except Exception:
            context.add_stderr(
                f"Failed to open with xr.open_dataset({grib_file}, **{open_datasets_kwargs}), "
                "opening with cfgrib.open_datasets instead."
            )
            datasets = {
                f"{fname}_{i}": ds
                for i, ds in enumerate(
                    cfgrib.open_datasets(grib_file, **open_datasets_kwargs)
                )
            }

    return datasets

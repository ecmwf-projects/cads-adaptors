import os
from typing import Any, Callable

import cfgrib
import xarray as xr

from cads_adaptors.adaptors import Context
from cads_adaptors.tools import adaptor_tools

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


def add_user_log_and_raise_error(
    message: str,
    context: Context = Context(),
    thisError=ValueError,
):
    context.add_user_visible_error(message)
    raise thisError(message)

def convert_format(
    result: Any,
    target_format: str,
    context: Context,
    open_datasets_kwargs: dict[str, Any] = dict(),
    **to_netcdf_kwargs,  # TODO: rename to something more generic
) -> list[str]:
    target_format = adaptor_tools.handle_data_format(target_format)

    convertor: None | Callable = {
        "netcdf": result_to_netcdf_files,
        "grib": result_to_grib_files,
    }.get(target_format, None)

    if convertor is not None:
        return convertor(
            result,
            context=context,
            open_datasets_kwargs=open_datasets_kwargs,
            **to_netcdf_kwargs,
        )

    else:
        message = f"WARNING: Unrecoginsed target_format requested, returning in original format: {result}"
        context.add_user_visible_error(message=message)
        context.add_stderr(message=message)
        return [result]


def result_to_grib_files(
    result: Any,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a result of unknown type to grib files."""
    if isinstance(result, str):
        return unknown_filetype_to_grib_files(result, context=context, **kwargs)
    elif isinstance(result, xr.Dataset):
        context.add_user_visible_error(
            "Cannot convert xarray dataset to grib, returning as netCDF."
        )
        return xarray_dict_to_netcdf({"data": result}, context=context, **kwargs)
    elif isinstance(result, list):
        # Ensure objects are same type (This may not be necessary, but it probably implies something is wrong)
        _result_type: list[type] = list(set([type(r) for r in result]))
        assert (
            len(_result_type) == 1
        ), f"Result list contains mixed types: {_result_type}"
        result_type = _result_type[0]
        if result_type == str:
            out_results = []
            for res in result:
                out_results += unknown_filetype_to_grib_files(
                    res, context=context, **kwargs
                )
            return out_results
        elif result_type == xr.Dataset:
            context.add_user_visible_error(
                "Cannot convert xarray dataset to grib, returning as netCDF."
            )
            return xarray_dict_to_netcdf(
                {f"data_{i}": res for i, res in enumerate(result)},
                context=context,
                **kwargs,
            )
        else:
            add_user_log_and_raise_error(
                f"Unable to convert result of type {result_type} to grib files. result:\n{result}",
                context=context, thisError=ValueError
            )

    elif isinstance(result, dict):
        # Ensure all values are of the same type
        # (This may not be necessary, but it probably implies something is wrong)
        _result_type = list(set([type(r) for r in result.values()]))
        assert (
            len(_result_type) == 1
        ), f"Result dictionary contains mixed types: {_result_type}"
        result_type = _result_type[0]

        if result_type == str:
            out_results = []
            for k, v in result.items():
                out_results += unknown_filetype_to_grib_files(
                    v, context=context, tag=k, **kwargs
                )
            return out_results
        elif result_type == xr.Dataset:
            context.add_user_visible_error(
                "Cannot convert xarray dataset to grib, returning as netCDF."
            )
            return xarray_dict_to_netcdf(
                {k: res for k, res in result.items()},
                context=context,
                **kwargs,
            )
        else:
            add_user_log_and_raise_error(
                f"Unable to convert result of type {result_type} to grib files. result:\n{result}",
                context=context, thisError=ValueError
            )
    else:
        add_user_log_and_raise_error(
            f"Unable to convert result of type {type(result)} to grib files. result:\n{result}",
            context=context, thisError=ValueError
        )


def result_to_netcdf_files(
    result: Any,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a result of unknown type to netCDF files."""
    if isinstance(result, str):
        return unknown_filetype_to_netcdf_files(result, context=context, **kwargs)
    elif isinstance(result, xr.Dataset):
        return xarray_dict_to_netcdf({"data": result}, context=context, **kwargs)

    elif isinstance(result, list):
        # Ensure objects are same type (This may not be necessary, but it probably implies something is wrong)
        _result_type: list[type] = list(set([type(r) for r in result]))
        assert (
            len(_result_type) == 1
        ), f"Result list contains mixed types: {_result_type}"
        result_type = _result_type[0]
        if result_type == str:
            out_results = []
            for res in result:
                out_results += unknown_filetype_to_netcdf_files(
                    res, context=context, **kwargs
                )
            return out_results

        elif result_type == xr.Dataset:
            return xarray_dict_to_netcdf(
                {f"data_{i}": res for i, res in enumerate(result)},
                context=context,
                **kwargs,
            )

        else:
            add_user_log_and_raise_error(
                f"Unable to convert result of type {result_type} to netCDF files. result:\n{result}",
                context=context, thisError=ValueError
            )
    elif isinstance(result, dict):
        # Ensure all values are of the same type
        # (This may not be necessary, but it probably implies something is wrong)
        _result_type = list(set([type(r) for r in result.values()]))
        assert (
            len(_result_type) == 1
        ), f"Result dictionary contains mixed types: {_result_type}"
        result_type = _result_type[0]
        if result_type == str:
            out_results = []
            for k, v in result.items():
                out_results += unknown_filetype_to_netcdf_files(
                    v, context=context, tag=k, **kwargs
                )
            return out_results
        elif result_type == xr.Dataset:
            return xarray_dict_to_netcdf(result, context=context, **kwargs)
        else:
            add_user_log_and_raise_error(
                f"Unable to convert result of type {result_type} to netCDF files. result:\n{result}",
                context=context, thisError=ValueError
            )

    else:
        add_user_log_and_raise_error(
            f"Unable to convert result of type {type(result)} to netCDF files. result:\n{result}",
            context=context, thisError=ValueError
        )


def unknown_filetype_to_grib_files(
    infile: str,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a file of unknown type to netCDF files."""
    _, ext = os.path.splitext(os.path.basename(infile))
    if ext.lower() in ["grib", "grib2"]:
        return [infile]
    elif ext.lower() in ["netcdf", "nc", "csv"]:
        context.add_user_visible_error(
            f"Cannot convert {ext} to grib, returning original file."
        )
        return [infile]
    else:
        add_user_log_and_raise_error(
            f"Unknown file type: {infile}",
            context=context, thisError=ValueError
        )


def unknown_filetype_to_netcdf_files(
    infile: str,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a file of unknown type to netCDF files."""
    _, ext = os.path.splitext(os.path.basename(infile))
    if ext.lower() in ["netcdf", "nc"]:
        return [infile]
    elif ext.lower() in ["grib", "grib2"]:
        context.add_stdout(f"Converting {infile} to netCDF files with kwargs: {kwargs}")
        return grib_to_netcdf_files(infile, context=context, **kwargs)
    else:
        add_user_log_and_raise_error(
            f"Unknown file type: {infile}",
            context=context, thisError=ValueError
        )

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
    datasets: dict[str, xr.Dataset],
    context: Context = Context(),
    compression_options: str | dict[str, Any] = "default",
    out_fname_prefix: str = "",
    **to_netcdf_kwargs,
):
    """
    Convert a dictionary of xarray datasets to netCDF files, where the key of the dictionary
    is used in the filename.
    """
    if isinstance(compression_options, str):
        compression_options = STANDARD_COMPRESSION_OPTIONS.get(compression_options, {})

    to_netcdf_kwargs.setdefault("engine", compression_options.pop("engine", "h5netcdf"))

    # Allow renaming of variables from what cfgrib decides
    rename: dict[str, str] = to_netcdf_kwargs.pop("rename", {})
    # Allow expanding of dimensionality, e.g. to ensure that time is always a dimension
    # (this is applied after squeezing)
    expand_dims: list[str] = to_netcdf_kwargs.pop("expand_dims", [])
    out_nc_files = []
    for out_fname_base, dataset in datasets.items():
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


def open_result_as_xarray_dictionary(
    result: Any,
    context: Context = Context(),
    **kwargs,
) -> dict[str, xr.Dataset]:
    """
    Open a result of unknown type and return as a dictionary of xarray datasets,
    where the key will be used in any filenames created from the dataset.
    """
    if isinstance(result, str):
        datasets = open_file_as_xarray_dictionary(result, context=context, **kwargs)
    elif isinstance(result, xr.Dataset):
        datasets = {"data": result}
    elif isinstance(result, dict):
        datasets = {}
        for k, v in result.items():
            if isinstance(v, str):
                datasets.update(
                    open_file_as_xarray_dictionary(v, context=context, **kwargs)
                )
            elif isinstance(v, xr.Dataset):
                datasets[k] = v
            else:
                add_user_log_and_raise_error(
                    "Incorrect result type, "
                    "if result dictionary it must be of type: dict[str, str | xr.Dataset]."
                    f"result:\n{result}",
                    context=context, thisError=ValueError
                )
    elif isinstance(result, list):
        datasets = {}
        for k, v in enumerate(result):
            if isinstance(v, str):
                datasets.update(
                    open_file_as_xarray_dictionary(v, context=context, **kwargs)
                )
            elif isinstance(v, xr.Dataset):
                datasets[f"data_{k}"] = v
            else:
                add_user_log_and_raise_error(
                    "Incorrect result type, "
                    "if result list it must be of type: list[str | xr.Dataset]."
                    f"result:\n{result}",
                    context=context, thisError=ValueError
                )
    else:
        add_user_log_and_raise_error(
            f"Unable to open result of type {type(result)} as an xarray dataset.",
            context=context, thisError=ValueError
        )

    return datasets


def open_file_as_xarray_dictionary(
    infile: str,
    context: Context = Context(),
    **kwargs,
) -> dict[str, xr.Dataset]:
    """
    Open a file of unknown type and return as a dictionary of xarray datasets,
    where the key will be used in any filenames created from the dataset.
    """
    _, ext = os.path.splitext(os.path.basename(infile))
    if ext.lower() in ["netcdf", "nc"]:
        return open_netcdf_as_xarray_dictionary(infile, context=context, **kwargs)
    elif ext.lower() in ["grib", "grib2"]:
        return open_grib_file_as_xarray_dictionary(infile, context=context, **kwargs)
    else:
        add_user_log_and_raise_error(
            f"Unable to open file {infile} as an xarray dataset.",
            context=context, thisError=ValueError
        )

# FUNCTIONS THAT OPEN FILES WITH XARRAY:


def open_netcdf_as_xarray_dictionary(
    netcdf_file: str,
    context: Context = Context(),
    open_datasets_kwargs: dict[str, Any] = {},
    **kwargs,
) -> dict[str, xr.Dataset]:
    """
    Open a netcdf file and return as a dictionary of xarray datasets,
    where the key will be used in any filenames created from the dataset.
    """
    assert isinstance(
        open_datasets_kwargs, dict
    ), "open_datasets_kwargs must be a dictionary for netCDF"
    # This is to maintain some consistency with the grib file opening
    open_datasets_kwargs = {
        "chunks": DEFAULT_CHUNKS,
        **open_datasets_kwargs,
        **kwargs,
    }
    context.add_stdout(f"Opening {netcdf_file} with kwargs: {open_datasets_kwargs}")
    datasets = {
        os.path.basename(netcdf_file): xr.open_dataset(
            netcdf_file, **open_datasets_kwargs
        )
    }

    return datasets


def open_grib_file_as_xarray_dictionary(
    grib_file: str,
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    context: Context = Context(),
    **kwargs,
) -> dict[str, xr.Dataset]:
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
        datasets: dict[str, xr.Dataset] = {}
        for i, open_ds_kwargs in enumerate(open_datasets_kwargs):
            # Default engine is cfgrib
            open_ds_kwargs.setdefault("engine", "cfgrib")
            open_ds_kwargs.setdefault("chunks", DEFAULT_CHUNKS)
            # Any defined kwargs are used for all datasets
            open_ds_kwargs.update(kwargs)
            ds_tag = open_ds_kwargs.pop("tag", i)
            try:
                ds = xr.open_dataset(grib_file, **open_ds_kwargs)
            except Exception:
                ds = None
            if ds:
                datasets[f"{fname}_{ds_tag}"] = ds
    else:
        open_datasets_kwargs.setdefault("chunks", DEFAULT_CHUNKS)
        # Include any additional kwargs, this may be useful for post-processing
        open_datasets_kwargs.update(kwargs)
        open_datasets_kwargs.setdefault("errors", "raise")
        # First try and open with xarray as a single dataset,
        # xarray.open_dataset will handle a number of the potential conflicts in fields
        ds_tag = open_datasets_kwargs.pop("tag", "0")
        try:
            datasets = {
                f"{fname}_{ds_tag}": xr.open_dataset(grib_file, **open_datasets_kwargs)
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

import os
from typing import Any, Callable, NoReturn

import cfgrib
import xarray as xr

from cads_adaptors.adaptors import Context
from cads_adaptors.tools import adaptor_tools
from cads_adaptors.tools.general import ensure_list

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
) -> NoReturn:
    context.add_user_visible_error(message)
    raise thisError(message)


def convert_format(
    result: Any,
    target_format: str,
    context: Context = Context(),
    config: dict[str, Any] = {},
) -> list[str]:
    target_format = adaptor_tools.handle_data_format(target_format)
    post_processing_kwargs = config.get("post_processing_kwargs", {})
    open_datasets_kwargs: dict[str, Any] = post_processing_kwargs.get(
        "open_datasets_kwargs", {}
    )
    post_open_kwargs: dict[str, Any] = post_processing_kwargs.get(
        "post_open_datasets_kwargs", {}
    )

    # Keywords specific to writing to the target format
    to_target_kwargs: dict[str, Any] = config.get(f"to_{target_format}_kwargs", {})

    convertor: None | Callable = {
        "netcdf": result_to_netcdf_files,
        "netcdf_legacy": result_to_netcdf_legacy_files,
        "grib": result_to_grib_files,
    }.get(target_format, None)

    if convertor is not None:
        return convertor(
            result,
            context=context,
            open_datasets_kwargs=open_datasets_kwargs,
            post_open_kwargs=post_open_kwargs,
            **to_target_kwargs,
        )

    else:
        message = (
            f"WARNING: Unrecoginsed target_format requested ({target_format}), "
            f"returning in original format: {result}"
        )
        context.add_user_visible_error(message=message)
        context.add_stderr(message=message)
        return ensure_list(result)


def result_to_grib_files(
    result: Any,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a result of unknown type to grib files."""
    context.add_stdout(
        f"Converting result ({result}) to grib files with kwargs: {kwargs}"
    )
    if isinstance(result, str):
        return unknown_filetype_to_grib_files(result, context=context, **kwargs)
    elif isinstance(result, xr.Dataset):
        context.add_user_visible_error(
            "Cannot convert xarray.Dataset to grib, returning as netCDF. "
            "Please note that post-processing uses xarray.Datasets. "
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
                context=context,
                thisError=ValueError,
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
                context=context,
                thisError=ValueError,
            )
    else:
        add_user_log_and_raise_error(
            f"Unable to convert result of type {type(result)} to grib files. result:\n{result}",
            context=context,
            thisError=ValueError,
        )


def result_to_netcdf_files(
    result: Any,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a result of unknown type to netCDF files."""
    context.add_stdout(
        f"Converting result ({result}) to netCDF files with kwargs: {kwargs}"
    )
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
                context=context,
                thisError=ValueError,
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
                context=context,
                thisError=ValueError,
            )

    else:
        add_user_log_and_raise_error(
            f"Unable to convert result of type {type(result)} to netCDF files. result:\n{result}",
            context=context,
            thisError=ValueError,
        )


def result_to_netcdf_legacy_files(
    result: Any,
    context: Context = Context(),
    command: str | list[str] = ["grib_to_netcdf", "-S", "param"],
    filter_rules: str | None = None,
    **kwargs,
) -> list[str]:
    """
    Legacy grib_to_netcdf convertor, which will be marked as deprecated.
    Can only accept a grib file, or list/dict of grib files as input.
    Converts to netCDF3 only.
    """
    context.add_user_visible_error(
        "The 'netcdf_legacy' format is deprecated and no longer supported, please use 'netcdf' instead."
    )
    context.add_stdout(
        f"Converting result ({result}) to legacy netCDF files with grib_to_netcdf"
    )

    # Check result is a single grib_file or a list/dict of grib_files
    if isinstance(result, str):
        assert (
            result.endswith(".grib") or result.endswith(".grib2")
        ), f"The 'netcdf_legacy' format can only accept a grib files as input. File received: {result}"
        fname, _ = os.path.splitext(os.path.basename(result))
        result = {fname: result}

    elif isinstance(result, list):
        # Ensure objects are same type (This may not be necessary, but it probably implies something is wrong)
        results_types: list[type] = list(set([type(r) for r in result]))
        result_type = results_types[0]
        assert (
            len(results_types) == 1
            and result_type == str
            and (result[0].endswith(".grib") or result[0].endswith(".grib2"))
        ), f"The 'netcdf_legacy' format can only accept grib files as input. Types received: {results_types}"

        result = {
            os.path.splitext(os.path.basename(result))[0]: result for result in result
        }
    elif isinstance(result, dict):
        # Ensure all values are of the same type
        # (This may not be necessary, but it probably implies something is wrong)
        result_types = list(set([type(r) for r in result.values()]))
        result_type = result_types[0]
        assert (
            len(result_types) == 1
            and result_type == str
            and (result[0].endswith(".grib") or result[0].endswith(".grib2"))
        ), f"The 'netcdf_legacy' format can only accept grib files as input. Types received: {results_types}"

    else:
        add_user_log_and_raise_error(
            f"Unable to convert result of type {type(result)} to 'netcdf_legacy' files. result:\n{result}",
            context=context,
            thisError=ValueError,
        )
    print(f"grib_to_netcdf in results: {result}")

    if filter_rules:
        # Filter the grib files to netCDFable chunks (in replacement of split_on in legacy system)
        here = os.getcwd()
        with open(f"{here}/filter_rules", "w") as f:
            f.write(filter_rules)
        filtered_results = {}
        for out_fname_base, grib_file in result.items():
            import glob
            full_grib_path = os.path.realpath(grib_file)
            temp_filter_folder = (
                f"{os.path.dirname(full_grib_path)}/{out_fname_base}.filtered"
            )
            os.makedirs(temp_filter_folder, exist_ok=True)
            os.chdir(temp_filter_folder)
            os.system(
                f"grib_filter {here}/filter_rules {full_grib_path}"
            )
            os.chdir(here)
            for filter_file in glob.glob(f"{temp_filter_folder}/*.grib*"):
                filter_base = os.path.splitext(os.path.basename(filter_file))[0]
                filtered_results[f"{out_fname_base}_{filter_base}"] = filter_file
        result = filtered_results

    print(f"Grib files for conversion: {result}")
    nc_files = []
    for out_fname_base, grib_file in result.items():
        out_fname = f"{out_fname_base}.nc"
        nc_files.append(out_fname)
        command = ensure_list(command)
        os.system(" ".join(command + ["-o", out_fname, grib_file]))
    
    return nc_files


def unknown_filetype_to_grib_files(
    infile: str,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a file of unknown type to netCDF files."""
    _, ext = os.path.splitext(os.path.basename(infile))
    if ext.lower() in [".grib", ".grib2"]:
        return [infile]
    elif ext.lower() in [".netcdf", ".nc", ".csv"]:
        context.add_user_visible_error(
            f"Cannot convert {ext} to grib, returning original file."
        )
        return [infile]
    else:
        add_user_log_and_raise_error(
            f"Unknown file type: {infile}", context=context, thisError=ValueError
        )


def unknown_filetype_to_netcdf_files(
    infile: str,
    context: Context = Context(),
    **kwargs,
) -> list[str]:
    """Convert a file of unknown type to netCDF files."""
    _, ext = os.path.splitext(os.path.basename(infile))
    if ext.lower() in [".netcdf", ".nc"]:
        return [infile]
    elif ext.lower() in [".grib", ".grib2"]:
        context.add_stdout(f"Converting {infile} to netCDF files with kwargs: {kwargs}")
        return grib_to_netcdf_files(infile, context=context, **kwargs)
    else:
        add_user_log_and_raise_error(
            f"Unknown file type: {infile}", context=context, thisError=ValueError
        )


def grib_to_netcdf_files(
    grib_file: str,
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    post_open_kwargs: dict[str, Any] = {},
    context: Context = Context(),
    **to_netcdf_kwargs,
):
    to_netcdf_kwargs.update(to_netcdf_kwargs.pop("to_netcdf_kwargs", {}))
    grib_file = os.path.realpath(grib_file)

    context.add_stdout(
        f"Converting {grib_file} to netCDF files with:\n"
        f"to_netcdf_kwargs: {to_netcdf_kwargs}\n"
        f"open_datasets_kwargs: {open_datasets_kwargs}\n"
        f"post_open_kwargs: {post_open_kwargs}\n"
    )

    datasets = open_grib_file_as_xarray_dictionary(
        grib_file,
        open_datasets_kwargs=open_datasets_kwargs,
        post_open_kwargs=post_open_kwargs,
        context=context,
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
) -> list[str]:
    """
    Convert a dictionary of xarray datasets to netCDF files, where the key of the dictionary
    is used in the filename.
    """
    if isinstance(compression_options, str):
        compression_options = STANDARD_COMPRESSION_OPTIONS.get(compression_options, {})

    to_netcdf_kwargs.setdefault("engine", compression_options.pop("engine", "h5netcdf"))
    out_nc_files = []
    for out_fname_base, dataset in datasets.items():
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
                    context=context,
                    thisError=ValueError,
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
                    context=context,
                    thisError=ValueError,
                )
    else:
        add_user_log_and_raise_error(
            f"Unable to open result of type {type(result)} as an xarray dataset.",
            context=context,
            thisError=ValueError,
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
    if ext.lower() in [".netcdf", ".nc"]:
        return open_netcdf_as_xarray_dictionary(infile, context=context, **kwargs)
    elif ext.lower() in [".grib", ".grib2"]:
        return open_grib_file_as_xarray_dictionary(infile, context=context, **kwargs)
    else:
        add_user_log_and_raise_error(
            f"Unable to open file {infile} as an xarray dataset.",
            context=context,
            thisError=ValueError,
        )


def safely_rename_variable(dataset: xr.Dataset, rename: dict[str, str]) -> xr.Dataset:
    """
    Rename variables in an xarray dataset,
    ensuring that the new names are not already in the dataset.
    """
    # Create a rename order based on variabels that exist in datasets, and if there is
    # a conflict, the variable that is being renamed will be renamed first.
    rename_order: list[str] = []
    conflicts: list[str] = []
    for old_name, new_name in rename.items():
        if old_name not in dataset:
            continue

        if new_name in dataset:
            rename_order.append(old_name)
            conflicts.append(old_name)
        else:
            rename_order = [old_name] + rename_order

    # Ensure that the conflicts are handled correctly
    # Is this necessary? We can let xarray fail by itself in the next step.
    for conflict in conflicts:
        new_name = rename[conflict]
        if (new_name not in rename_order) or (
            rename_order.index(conflict) > rename_order.index(new_name)
        ):
            raise ValueError(
                f"Refusing to to rename to existing variable name: {conflict}->{new_name}"
            )

    for old_name in rename_order:
        dataset = dataset.rename({old_name: rename[old_name]})

    return dataset


def safely_expand_dims(dataset: xr.Dataset, expand_dims: list[str]) -> xr.Dataset:
    """
    Expand dimensions in an xarray dataset, ensuring that the new dimensions are not already in the dataset
    and that the order of dimensions is preserved.
    """
    dims_required = [c for c in dataset.coords if c in expand_dims + list(dataset.dims)]
    dims_missing = [
        (c, i) for i, c in enumerate(dims_required) if c not in dataset.dims
    ]
    dataset = dataset.expand_dims(
        dim=[x[0] for x in dims_missing], axis=[x[1] for x in dims_missing]
    )
    return dataset


def post_open_datasets_modifications(
    datasets: dict[str, xr.Dataset],
    rename: dict[str, str] = {},
    expand_dims: list[str] = [],
) -> dict[str, Any]:
    """
    Apply post-opening modifications to the datasets, such as renaming variables
    and expanding dimensions.
    """
    out_datasets = {}
    for out_fname_base, dataset in datasets.items():
        dataset = safely_rename_variable(dataset, rename)

        dataset = safely_expand_dims(dataset, expand_dims)

        out_datasets[out_fname_base] = dataset

    return out_datasets


# FUNCTIONS THAT OPEN FILES WITH XARRAY:
def open_netcdf_as_xarray_dictionary(
    netcdf_file: str,
    context: Context = Context(),
    open_datasets_kwargs: dict[str, Any] = {},
    post_open_kwargs: dict[str, Any] = {},
    **kwargs,
) -> dict[str, xr.Dataset]:
    """
    Open a netcdf file and return as a dictionary of xarray datasets,
    where the key will be used in any filenames created from the dataset.
    """
    fname, _ = os.path.splitext(os.path.basename(netcdf_file))

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
    datasets = {fname: xr.open_dataset(netcdf_file, **open_datasets_kwargs)}

    datasets = post_open_datasets_modifications(datasets, **post_open_kwargs)

    return datasets


def open_grib_file_as_xarray_dictionary(
    grib_file: str,
    open_datasets_kwargs: None | dict[str, Any] | list[dict[str, Any]] = None,
    post_open_kwargs: dict[str, Any] = {},
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

    datasets = post_open_datasets_modifications(datasets, **post_open_kwargs)

    return datasets

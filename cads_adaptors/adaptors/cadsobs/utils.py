import logging
import uuid
from pathlib import Path
from typing import Tuple

import cftime
import h5netcdf
import numpy
import pandas
import xarray
from fsspec.implementations.http import HTTPFileSystem

from cads_adaptors.adaptors.cadsobs.models import (
    RetrieveArgs,
    RetrieveFormat,
    RetrieveParams,
)
from cads_adaptors.exceptions import CadsObsRuntimeError

logger = logging.getLogger(__name__)
MAX_NUMBER_OF_GROUPS = 10
TIME_UNITS_REFERENCE_DATE = "1900-01-01 00:00:00"
SPATIAL_COORDINATES = ["latitude", "longitude"]


def _get_output_path(output_dir: Path, dataset: str, format: RetrieveFormat) -> Path:
    """Return the path of the output file."""
    if format == "csv":
        extension = ".csv"
    else:
        extension = ".nc"
    output_path = Path(output_dir, dataset + "_" + uuid.uuid4().hex + extension)
    return output_path


def _add_attributes(
    oncobj: h5netcdf.File, field_attributes: dict, global_attributes: dict
):
    """Add relevant attributes to the output netCDF."""
    if "height_of_station_above_sea_level" in oncobj.variables:
        oncobj.variables["height_of_station_above_sea_level"].attrs["units"] = "m"
    for coord in ["longitude", "latitude"]:
        if coord in oncobj.variables:
            oncobj.variables[coord].attrs["standard_name"] = coord
            if coord == "longitude":
                oncobj.variables[coord].attrs["units"] = "degrees_east"
            if coord == "latitude":
                oncobj.variables[coord].attrs["units"] = "degrees_north"
    oncobj.variables["report_timestamp"].attrs["standard_name"] = "time"
    oncobj.attrs["featureType"] = "point"
    # Variables defined as part of the CDM lite
    for oncvar in oncobj.variables:
        if oncvar in field_attributes:
            oncobj.variables[oncvar].attrs.update(field_attributes[oncvar])
    # Global attributes
    oncobj.attrs.update(global_attributes)


def _get_url_ncobj(fs: HTTPFileSystem, url: str) -> h5netcdf.File:
    """Open an URL as a netCDF file object with h5netcdf."""
    fobj = fs.open(url)
    logger.debug(f"Reading data from {url}.")
    # xarray won't read bytes object directly with netCDF4
    ncfile = h5netcdf.File(fobj, "r")
    return ncfile


def _get_output_dtype(ivar: str, ivarobj: h5netcdf.Variable) -> str:
    if ivar == "observed_variable":
        dtype = "S1"
    else:
        dtype = ivarobj.dtype
    return dtype


def _get_char_sizes(fs: HTTPFileSystem, object_urls: list[str]) -> dict[str, int]:
    """
    Iterate over the input files to get the size of the string variables.

    We need to know this beforehand so we can stream to the output file.
    """
    char_sizes = {}
    for url in object_urls:
        with _get_url_ncobj(fs, url) as incobj:
            for var, varobj in incobj.items():
                if varobj.dtype.kind == "S":
                    char_size = varobj.shape[1]
                else:
                    continue
                if var not in char_sizes:
                    char_sizes[var] = char_size
                else:
                    char_sizes[var] = max(char_sizes[var], char_size)

    return char_sizes


def _filter_asset_and_save(
    fs: HTTPFileSystem,
    oncobj: h5netcdf.File,
    retrieve_args: RetrieveArgs,
    url: str,
    char_sizes: dict[str, int],
    cdm_lite_variables: list[str],
):
    """Get the filtered data from the asset and dump it to the output file."""
    with _get_url_ncobj(fs, url) as incobj:
        mask = _get_mask(incobj, retrieve_args.params)
        if mask.any():
            number_of_groups = len(_ezclump(mask))
            mask_size = mask.sum()
            # We will download the full chunks in this cases, as it is way more efficient
            download_all_chunk = (
                number_of_groups > MAX_NUMBER_OF_GROUPS or mask_size > mask.size * 0.8
            )
            if download_all_chunk:
                logger.debug("Downloading all chunk for efficiency")

            # Resize dimension needs to be done explicitly in h5netcdf
            output_current_size = oncobj.dimensions["index"].size
            new_size = output_current_size + mask.sum()
            oncobj.resize_dimension("index", new_size)
            # Get the variables in the input file that are in the CDM lite specification.
            vars_in_cdm_lite = _get_vars_in_cdm_lite(incobj, cdm_lite_variables)
            # Handle coordinate renaming
            vars_to_rename = dict()
            for varname in vars_in_cdm_lite.copy():
                if "|" in varname:
                    varname_notable, table_name = varname.split("|")
                    if varname_notable in SPATIAL_COORDINATES:
                        if table_name == "observations_table":
                            vars_to_rename[varname] = varname_notable
                        elif table_name in ["station_configuration", "header_table"]:
                            name_obs_table = f"{varname_notable}|observations_table"
                            if name_obs_table not in vars_in_cdm_lite:
                                vars_to_rename[varname] = varname_notable
                            else:
                                vars_in_cdm_lite.remove(varname)

            # Filter and save the data for each variable.
            for ivar in vars_in_cdm_lite:
                _filter_and_save_var(
                    incobj,
                    ivar,
                    oncobj,
                    output_current_size,
                    new_size,
                    char_sizes,
                    mask,
                    mask_size,
                    download_all_chunk,
                    rename=vars_to_rename,
                )
        else:
            # Sometimes no data will be found as for example requested station may not
            # have the requested varaibles available.
            logger.debug("No data found in asset for the query paramater.")


def _get_mask(incobj: h5netcdf.File, retrieve_params: RetrieveParams) -> numpy.ndarray:
    """Return a boolean mask with requested observation_ids."""
    logger.debug("Filtering data in retrieved chunk data.")
    retrieve_params_dict = retrieve_params.model_dump()
    masks_combined = numpy.ones(
        shape=(incobj.dimensions["observation_id"].size,), dtype="bool"
    )
    # Filter header table
    if retrieve_params.stations is not None:
        stations_asked = [s.encode("utf-8") for s in retrieve_params.stations]
        stationvar = incobj.variables["primary_station_id"]
        field_len, strlen = stationvar.shape
        stations_in_partition = (
            incobj.variables["primary_station_id"][:]
            .view(f"S{strlen}")
            .reshape(field_len)
        )
        station_mask = numpy.isin(stations_in_partition, stations_asked)
        masks_combined = numpy.logical_and(masks_combined, station_mask)
    # Filter time and space
    time_and_space = ["time_coverage", "longitude_coverage", "latitude_coverage"]
    for param_name in time_and_space:
        coverage_range = retrieve_params_dict[param_name]
        if coverage_range is not None:
            # Get the parameter we went to filter as a pandas.Index()
            param_name_in_data = _get_param_name_in_data(incobj, param_name)
            param_index = incobj.variables[param_name_in_data][:]
            if param_name == "time_coverage":
                # Turn dates into integers with the same units
                units = incobj.variables[param_name_in_data].attrs["units"]
                coverage_range = cftime.date2num(coverage_range, units=units)
            param_mask = _between(param_index, coverage_range[0], coverage_range[1])
            masks_combined = numpy.logical_and(masks_combined, param_mask)
    # Filter days (month and year not needed)
    if retrieve_params_dict["day"] is not None:
        times_index = pandas.to_datetime(
            incobj.variables["report_timestamp"][:],
            unit="s",
            origin=TIME_UNITS_REFERENCE_DATE,
        )
        param_mask = times_index.day.isin(retrieve_params_dict["day"])
        masks_combined = numpy.logical_and(masks_combined, param_mask)

    # Decode variables
    if retrieve_params.variables is not None:
        variables_asked = retrieve_params.variables
        #  Map to codes
        var2code = _get_code_mapping(incobj)
        codes_asked = [var2code[v] for v in variables_asked if v in var2code]
        variables_file = incobj.variables["observed_variable"][:]
        variable_mask = numpy.isin(variables_file, codes_asked)
        masks_combined = numpy.logical_and(masks_combined, variable_mask)

    return masks_combined


def _get_param_name_in_data(retrieved_dataset: h5netcdf.File, param_name: str) -> str:
    match param_name:
        case "time_coverage":
            param_name_in_data = "report_timestamp"
        case "longitude_coverage" | "latitude_coverage":
            coord = param_name.split("_")[0]
            if f"{coord}|header_table" in retrieved_dataset.variables:
                param_name_in_data = f"{coord}|header_table"
            else:
                param_name_in_data = f"{coord}|station_configuration"
        case _:
            raise CadsObsRuntimeError(f"Unknown parameter name {param_name}")
    return param_name_in_data


def _get_vars_in_cdm_lite(
    incobj: h5netcdf.File, cdm_lite_variables: list[str]
) -> list[str]:
    """Return the variables in incobj that are defined in the CDM-lite."""
    vars_in_cdm_lite = [v for v in incobj.variables if v in cdm_lite_variables]
    # This searches for variables with "|cdm_table  in their name."
    vars_with_bar_in_cdm_lite = [
        v
        for v in incobj.variables
        if "|" in v and v.split("|")[0] in cdm_lite_variables
    ]
    vars_in_cdm_lite += vars_with_bar_in_cdm_lite
    return vars_in_cdm_lite


def _filter_and_save_var(
    incobj: h5netcdf.File,
    ivar: str,
    oncobj: h5netcdf.File,
    current_size: int,
    new_size: int,
    char_sizes: dict[str, int],
    mask: numpy.typing.NDArray,
    mask_size: int,
    download_all_chunk: bool,
    rename: dict | None = None,
):
    """
    Filter and save the data for each variable.

    String variables need special treatment as they have an extra dimension.
    """
    ivarobj = incobj.variables[ivar]
    dimensions: tuple[str, ...] = ("index",)
    # Use input chunksize except if it is bigger than get data we are getting.
    chunksize: tuple[int, ...] = (
        (ivarobj.chunks[0],) if ivarobj.chunks[0] < mask_size else (mask_size,)
    )
    dtype = _get_output_dtype(ivar, ivarobj)
    attrs = dict()
    # Set time units
    if ivar in ["report_timestamp", "record_timestamp"]:
        attrs["units"] = ivarobj.attrs["units"]
    # Handle character dimensions
    is_char = len(ivarobj.shape) > 1 or ivar == "observed_variable"
    if is_char:
        chunksize, dimensions = _handle_string_dims(
            char_sizes, chunksize, dimensions, ivar, oncobj
        )
    if rename is not None and ivar in rename:
        ivar = rename[ivar]
    # Create the variable
    if ivar not in oncobj.variables:
        # It is not worth it to go further than complevel 1 and it is much faster
        ovar = oncobj.create_variable(
            ivar,
            dimensions,
            dtype,
            chunks=chunksize,
            compression="gzip",
            compression_opts=1,
        )
    else:
        ovar = oncobj.variables[ivar]
    # Set variable attributes
    ovar.attrs.update(attrs)
    # Dump the data to the file
    if is_char:
        _dump_char_variable(
            current_size,
            incobj,
            ivar,
            ivarobj,
            mask,
            new_size,
            ovar,
            download_all_chunk,
        )
    else:
        if download_all_chunk:
            data = ivarobj[:][mask]
        else:
            data = ivarobj[mask]
        ovar[current_size:new_size] = data


def _dump_char_variable(
    current_size: int,
    incobj: h5netcdf.File,
    ivar: str,
    ivarobj: h5netcdf.Variable,
    mask: numpy.typing.NDArray,
    new_size: int,
    ovar: h5netcdf.Variable,
    download_all_chunk: bool,
):
    if ivar != "observed_variable":
        actual_str_dim_size = ivarobj.shape[-1]
        if download_all_chunk:
            data = ivarobj[:, 0:actual_str_dim_size][mask, :]
        else:
            data = ivarobj[mask, 0:actual_str_dim_size]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data
    else:
        # For observed variable, we use the attributes to decode the integers.
        if download_all_chunk:
            data = ivarobj[:][mask]
        else:
            data = ivarobj[mask]
        code2var = _get_code_mapping(incobj, inverse=True)
        codes_in_data, inverse = numpy.unique(data, return_inverse=True)
        variables_in_data = numpy.array(
            [code2var[c].encode("utf-8") for c in codes_in_data]
        )
        data_decoded = variables_in_data[inverse]
        data_decoded = data_decoded.view("S1").reshape(data.size, -1)
        actual_str_dim_size = data_decoded.shape[-1]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data_decoded


def _get_code_mapping(
    incobj: h5netcdf.File | xarray.Dataset, inverse: bool = False
) -> dict:
    import h5netcdf

    if isinstance(incobj, h5netcdf.File):
        attrs = incobj.variables["observed_variable"].attrs
    elif isinstance(incobj, xarray.Dataset):
        attrs = incobj["observed_variable"].attrs
    else:
        raise CadsObsRuntimeError("Unsupported input type")
    if inverse:
        mapping = {c: v for v, c in zip(attrs["labels"], attrs["codes"])}
    else:
        mapping = {v: c for v, c in zip(attrs["labels"], attrs["codes"])}
    return mapping


def _handle_string_dims(
    char_sizes: dict[str, int],
    chunksize: Tuple[int, ...],
    dimensions: Tuple[str, ...],
    ivar: str,
    oncobj: h5netcdf.File,
) -> Tuple[Tuple[int, ...], Tuple[str, ...]]:
    ivar_str_dim = ivar + "_stringdim"
    ivar_str_dim_size = char_sizes[ivar]
    if ivar_str_dim not in oncobj.dimensions:
        oncobj.dimensions[ivar_str_dim] = ivar_str_dim_size
    dimensions += (ivar_str_dim,)
    chunksize += (ivar_str_dim_size,)
    return chunksize, dimensions


def _remove_table_name_from_coordinates(
    incobj: h5netcdf.File, ivar: str
) -> Tuple[str, str | None]:
    if ivar in ["latitude|header_table", "longitude|header_table"]:
        ovar, cdm_table = ivar.split("|")
    elif (
        ivar in ["latitude|station_configuration", "longitude|station_configuration"]
        and "latitude|header_table" not in incobj.variables
    ):
        ovar, cdm_table = ivar.split("|")
    else:
        ovar = ivar
        cdm_table = None
    return ovar, cdm_table


def _ezclump(mask) -> list[slice]:
    """
    Find the clumps (groups of data with the same values) for a 1D bool array.

    Internal function form numpy.ma.extras

    Returns a series of slices.
    """
    if mask.ndim > 1:
        mask = mask.ravel()
    idx = (mask[1:] ^ mask[:-1]).nonzero()
    idx = idx[0] + 1

    if mask[0]:
        if len(idx) == 0:
            return [slice(0, mask.size)]

        r = [slice(0, idx[0])]
        r.extend((slice(left, right) for left, right in zip(idx[1:-1:2], idx[2::2])))
    else:
        if len(idx) == 0:
            return []

        r = [slice(left, right) for left, right in zip(idx[:-1:2], idx[1::2])]

    if mask[-1]:
        r.append(slice(idx[-1], mask.size))
    return r


def _between(index, start, end):
    return (index >= start) & (index < end)

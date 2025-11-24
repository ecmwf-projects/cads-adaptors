import logging

import cftime
import h5netcdf
import numpy
import pandas
from fsspec.implementations.http import HTTPFileSystem

from cads_adaptors.adaptors.cadsobs.char_utils import (
    concat_str_array,
    dump_char_variable,
    handle_string_dims,
)
from cads_adaptors.adaptors.cadsobs.codes import get_code_mapping
from cads_adaptors.adaptors.cadsobs.constants import (
    MAX_NUMBER_OF_GROUPS,
    TIME_UNITS_REFERENCE_DATE,
)
from cads_adaptors.adaptors.cadsobs.models import RetrieveArgs, RetrieveParams
from cads_adaptors.adaptors.cadsobs.utils import (
    ezclump,
    get_output_dtype,
    get_param_name_in_data,
    get_url_ncobj,
    get_vars_in_cdm_lite,
    handle_coordinate_renaming,
)

logger = logging.getLogger(__name__)


def filter_asset_and_save(
    fs: HTTPFileSystem,
    oncobj: h5netcdf.File,
    retrieve_args: RetrieveArgs,
    url: str,
    char_sizes: dict[str, int],
    cdm_lite_variables: list[str],
):
    """Get the filtered data from the asset and dump it to the output file."""
    with get_url_ncobj(fs, url) as incobj:
        mask = _get_mask(incobj, retrieve_args.params)
        if mask.any():
            number_of_groups = len(ezclump(mask))
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
            vars_in_cdm_lite = get_vars_in_cdm_lite(incobj, cdm_lite_variables)
            # Handle coordinate renaming
            vars_to_rename, vars_in_cdm_lite = handle_coordinate_renaming(
                vars_in_cdm_lite
            )

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
    # Define a mask with ones to update with the different filters.
    masks_combined = numpy.ones(
        shape=(incobj.dimensions["observation_id"].size,), dtype="bool"
    )
    # Filter stations if provided
    masks_combined = _filter_stations(incobj, masks_combined, retrieve_params)
    # Filter time and space
    masks_combined = _filter_time_and_space(incobj, masks_combined, retrieve_params)
    # Decode variables
    masks_combined = _filter_variables(incobj, masks_combined, retrieve_params)
    # Extra filters
    masks_combined = _apply_extra_filters(incobj, masks_combined, retrieve_params)
    return masks_combined


def _apply_extra_filters(
    incobj: h5netcdf.File,
    masks_combined: numpy.ndarray,
    retrieve_params: RetrieveParams,
) -> numpy.ndarray:
    """Apply the mask for requested extra filters if any.

    We use the extra_filters field in the request. The filter support single values
    (text or numeric) and also lists calling "isin".
    """
    # We need this here for mypy not to complain
    if retrieve_params.extra_filters is None:
        return masks_combined
    for field, filter_values in retrieve_params.extra_filters.items():
        if field not in incobj.variables:
            raise RuntimeError(f"{field=} in extra filters not found.")
        field_values = incobj.variables[field][:]
        # Strings need to be concatenated
        if incobj.variables[field].dtype.kind == "S":
            field_values = concat_str_array(field_values)
        if isinstance(filter_values, list):
            field_mask = numpy.isin(field_values, filter_values)
        else:
            field_mask = field_values == filter_values
        masks_combined = numpy.logical_and(masks_combined, field_mask)
    return masks_combined


def _filter_variables(
    incobj: h5netcdf.File,
    masks_combined: numpy.ndarray,
    retrieve_params: RetrieveParams,
) -> numpy.ndarray:
    """Apply the mask for the requested variables."""
    variables_asked = retrieve_params.variables
    if variables_asked is None:
        return masks_combined
    #  Map to codes
    var2code = get_code_mapping(incobj)
    codes_asked = [var2code[v] for v in variables_asked if v in var2code]
    variables_file = incobj.variables["observed_variable"][:]
    variable_mask = numpy.isin(variables_file, codes_asked)
    masks_combined = numpy.logical_and(masks_combined, variable_mask)
    return masks_combined


def _filter_time_and_space(
    incobj: h5netcdf.File,
    masks_combined: numpy.ndarray,
    retrieve_params: RetrieveParams,
) -> numpy.ndarray:
    """Apply the mask for the time and space bounds requested."""
    time_and_space = ["time_coverage", "longitude_coverage", "latitude_coverage"]
    retrieve_params_dict = retrieve_params.model_dump()
    for param_name in time_and_space:
        coverage_range = retrieve_params_dict[param_name]
        if coverage_range is not None:
            # Get the parameter we want to filter as a pandas.Index()
            param_name_in_data = get_param_name_in_data(incobj, param_name)
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
    return masks_combined


def _filter_stations(
    incobj: h5netcdf.File,
    masks_combined: numpy.ndarray,
    retrieve_params: RetrieveParams,
) -> numpy.ndarray:
    """Apply the mask for the requested station ids."""
    if retrieve_params.stations is None:
        return masks_combined
    stations_asked = [s.encode("utf-8") for s in retrieve_params.stations]
    stationvar = incobj.variables["primary_station_id"]
    stations_in_partition = concat_str_array(stationvar[:])
    station_mask = numpy.isin(stations_in_partition, stations_asked)
    masks_combined = numpy.logical_and(masks_combined, station_mask)
    return masks_combined


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
    dtype = get_output_dtype(ivar, ivarobj)
    attrs = dict()
    # Set time units
    if ivar in ["report_timestamp", "record_timestamp"]:
        attrs["units"] = ivarobj.attrs["units"]
    # Handle character dimensions
    is_char = len(ivarobj.shape) > 1 or ivar == "observed_variable"
    if is_char:
        chunksize, dimensions = handle_string_dims(
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
        dump_char_variable(
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


def _between(index, start, end):
    return (index >= start) & (index < end)

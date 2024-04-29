import logging
import tempfile
import uuid
from pathlib import Path
from typing import Tuple

import cftime
import fsspec
import h5netcdf
import numpy
import requests
import xarray
from fsspec.implementations.http import HTTPFileSystem

from cads_adaptors.adaptors.cadsobs.models import RetrieveArgs, RetrieveParams

logger = logging.getLogger(__name__)


def retrieve_data(
    dataset_name: str, mapped_request: dict, object_urls: list[str], obs_api_url: str
) -> Path:
    output_dir = Path(tempfile.mkdtemp())
    output_path_netcdf = _get_output_path(output_dir, dataset_name, "netCDF")
    logger.info(f"Streaming data to {output_path_netcdf}")
    # We first need to loop over the files to get the max size of the strings fields
    # This way we can know the size of the output
    # background cache will download blocks in the background ahead of time using a
    # thread.
    fs = fsspec.filesystem("https", cache_type="background", block_size=10 * (1024**2))
    # Silence fsspec log as background cache does print unformatted log lines.
    logging.getLogger("fsspec").setLevel(logging.WARNING)
    # Get the maximum size of the character arrays
    char_sizes = _get_char_sizes(fs, object_urls)
    variables = mapped_request["variables"]
    char_sizes["observed_variable"] = max([len(v) for v in variables])
    # Open the output file and dump the data from each input file.
    retrieve_args = RetrieveArgs(
        dataset=dataset_name, params=RetrieveParams(**mapped_request)
    )
    with h5netcdf.File(output_path_netcdf, "w") as oncobj:
        oncobj.dimensions["index"] = None
        for url in object_urls:
            filter_asset_and_save(
                fs, oncobj, retrieve_args, url, char_sizes, obs_api_url
            )
        # Check if the resulting file is empty
        if len(oncobj.variables) == 0 or len(oncobj.variables["report_timestamp"]) == 0:
            raise RuntimeError(
                "No data was found, try a different parameter combination."
            )
        # Add atributes
        _add_attributes(oncobj, retrieve_args, obs_api_url)
    # If the user asked for a CSV, we transform the file to CSV
    if retrieve_args.params.format == "netCDF":
        output_path = output_path_netcdf
    else:
        try:
            output_path = _to_csv(output_dir, output_path_netcdf, retrieve_args)
        finally:
            # Ensure that the netCDF is not left behind taking disk space.
            output_path_netcdf.unlink()
    return output_path


def _add_attributes(
    oncobj: h5netcdf.File, retrieve_args: RetrieveArgs, obs_api_url: str
):
    """Add relevant attributes to the output netCDF."""
    if "height_of_station_above_sea_level" in oncobj.variables:
        oncobj.variables["height_of_station_above_sea_level"].attrs["units"] = "m"
    oncobj.variables["longitude"].attrs["standard_name"] = "longitude"
    oncobj.variables["latitude"].attrs["standard_name"] = "latitude"
    oncobj.variables["report_timestamp"].attrs["standard_name"] = "time"
    oncobj.variables["longitude"].attrs["units"] = "degrees_east"
    oncobj.variables["latitude"].attrs["units"] = "degrees_north"
    oncobj.attrs["featureType"] = "point"
    # Global attributes
    service_definition = get_service_definition(retrieve_args.dataset, obs_api_url)
    oncobj.attrs.update(service_definition["global_attributes"])


def _get_char_sizes(fs: HTTPFileSystem, object_urls: list[str]) -> dict[str, int]:
    """Iterate over the input files to get the size of the string variables.

    We need to know this beforehand so we can stream to the output file.
    """
    char_sizes = {}
    for url in object_urls:
        with get_url_ncobj(fs, url) as incobj:
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


def get_service_definition(dataset: str, obs_api_url: str) -> dict:
    return requests.get(f"{obs_api_url}/{dataset}/service_definition").json()


def get_url_ncobj(fs: HTTPFileSystem, url: str) -> h5netcdf.File:
    """Open an URL as a netCDF file object with h5netcdf."""
    fobj = fs.open(url)
    logger.debug(f"Reading data from {url}.")
    # xarray won't read bytes object directly with netCDF4
    ncfile = h5netcdf.File(fobj, "r")
    return ncfile


def _get_output_path(output_dir: Path, dataset: str, format: str) -> Path:
    """Retuen the path of the output file."""
    if format == "csv":
        extension = ".csv"
    else:
        extension = ".nc"
    output_path = Path(output_dir, dataset + "_" + uuid.uuid4().hex + extension)
    return output_path


def _to_csv(
    output_dir: Path, output_path_netcdf: Path, retrieve_args: RetrieveArgs
) -> Path:
    """Transform the output netCDF to CSV format."""
    output_path = _get_output_path(output_dir, retrieve_args.dataset, "csv")
    cdm_lite_dataset = xarray.open_dataset(
        output_path_netcdf, chunks=dict(observation_id=100000), decode_times=True
    )
    logger.info("Transforming netCDF to CSV")
    # with output_path.open("w") as ofileobj:
    #     header = _get_csv_header(retrieve_args, cdm_lite_dataset)
    #     ofileobj.write(header)
    # TODO: Add the header when the dask bug is fixed.
    # https://github.com/dask/dask/issues/10414
    # Alternatively, we can try to refactor the to_cdm_lite code to be able to write
    # the CSV directly without passing through netCDF.
    cdm_lite_dataset.to_dask_dataframe().astype("str").to_csv(
        output_path,
        index=False,
        single_file=True,
        mode="w",
        compute_kwargs={"scheduler": "single-threaded"},
    )
    return output_path


def filter_asset_and_save(
    fs: HTTPFileSystem,
    oncobj: h5netcdf.File,
    retrieve_args: RetrieveArgs,
    url: str,
    char_sizes: dict[str, int],
    obs_api_url: str,
):
    """Get the filtered data from the asset and dump it to the output file."""
    with get_url_ncobj(fs, url) as incobj:
        mask = get_mask(incobj, retrieve_args.params)
        if mask.any():
            # Turn the mask into slices
            slices = ezclump(mask)
            mask_size = mask.sum()
            # Resize dimension needs to be done explicitly in h5netcdf
            current_size = oncobj.dimensions["index"].size
            new_size = current_size + mask.sum()
            oncobj.resize_dimension("index", new_size)
            # Get the variables in the input file that are in the CDM lite specification.
            vars_in_cdm_lite = get_vars_in_cdm_lite(incobj, obs_api_url)
            # Filter and save the data for each variable.
            for ivar in vars_in_cdm_lite:
                filter_and_save_var(
                    current_size,
                    incobj,
                    ivar,
                    slices,
                    new_size,
                    mask_size,
                    oncobj,
                    char_sizes,
                )
        else:
            # Sometimes no data will be found as for example requested station may not
            # have the requested varaibles available.
            logger.debug("No data found in asset for the query paramater.")


def get_mask(incobj: h5netcdf.File, retrieve_params: RetrieveParams) -> numpy.ndarray:
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
            param_name_in_data = get_param_name_in_data(incobj, param_name)
            param_index = incobj.variables[param_name_in_data][:]
            if param_name == "time_coverage":
                # Turn dates into integers with the same units
                units = incobj.variables[param_name_in_data].attrs["units"]
                coverage_range = cftime.date2num(coverage_range, units=units)
            param_mask = between(param_index, coverage_range[0], coverage_range[1])
            masks_combined = numpy.logical_and(masks_combined, param_mask)
    # Decode variables
    if retrieve_params.variables is not None:
        variables_asked = retrieve_params.variables
        #  Map to codes
        var2code = get_code_mapping(incobj)
        codes_asked = [var2code[v] for v in variables_asked if v in var2code]
        variables_file = incobj.variables["observed_variable"][:]
        variable_mask = numpy.isin(variables_file, codes_asked)
        masks_combined = numpy.logical_and(masks_combined, variable_mask)

    return masks_combined


def filter_and_save_var(
    current_size: int,
    incobj: h5netcdf.File,
    ivar: str,
    slices: list[slice],
    new_size: int,
    mask_size: int,
    oncobj: h5netcdf.File,
    char_sizes: dict[str, int],
):
    """Filter and save the data for each variable.

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
    # Remove table name from the coordinates
    ivar, cdm_table = _remove_table_name_from_coordinates(incobj, ivar)
    if cdm_table is not None:
        attrs["cdm_table"] = cdm_table
    # Set time units
    if ivar == "report_timestamp":
        attrs["units"] = ivarobj.attrs["units"]
    # Handle character dimensions
    is_char = len(ivarobj.shape) > 1 or ivar == "observed_variable"
    if is_char:
        chunksize, dimensions = handle_string_dims(
            char_sizes, chunksize, dimensions, ivar, oncobj
        )
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
        dump_char_variable(current_size, incobj, ivar, ivarobj, slices, new_size, ovar)
    else:
        data = numpy.concatenate([ivarobj[s] for s in slices])
        ovar[current_size:new_size] = data


def _get_output_dtype(ivar: str, ivarobj: h5netcdf.Variable) -> str:
    if ivar == "observed_variable":
        dtype = "S1"
    else:
        dtype = ivarobj.dtype
    return dtype


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


def handle_string_dims(char_sizes, chunksize, dimensions, ivar, oncobj):
    ivar_str_dim = ivar + "_stringdim"
    ivar_str_dim_size = char_sizes[ivar]
    if ivar_str_dim not in oncobj.dimensions:
        oncobj.dimensions[ivar_str_dim] = ivar_str_dim_size
    dimensions += (ivar_str_dim,)
    chunksize += (ivar_str_dim_size,)
    return chunksize, dimensions


def dump_char_variable(
    current_size: int,
    incobj: h5netcdf.File,
    ivar: str,
    ivarobj: h5netcdf.Variable,
    slices: list[slice],
    new_size: int,
    ovar: h5netcdf.Variable,
):
    if ivar != "observed_variable":
        actual_str_dim_size = ivarobj.shape[-1]
        data = numpy.concatenate([ivarobj[s, 0:actual_str_dim_size] for s in slices])
        ovar[current_size:new_size, 0:actual_str_dim_size] = data
    else:
        # For observed variable, we use the attributes to decode the integers.
        data = numpy.concatenate([ivarobj[s] for s in slices])
        code2var = get_code_mapping(incobj, inverse=True)
        codes_in_data, inverse = numpy.unique(data, return_inverse=True)
        variables_in_data = numpy.array(
            [code2var[c].encode("utf-8") for c in codes_in_data]
        )
        data_decoded = variables_in_data[inverse]
        data_decoded = data_decoded.view("S1").reshape(data.size, -1)
        actual_str_dim_size = data_decoded.shape[-1]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data_decoded


def get_vars_in_cdm_lite(incobj: h5netcdf.File, obs_api_url: str) -> list[str]:
    """Return the variables in incobj that are defined in the CDM-lite."""
    cdm_lite_variables = get_cdm_lite_variables(obs_api_url)
    vars_in_cdm_lite = [v for v in incobj.variables if v in cdm_lite_variables]
    # This searches for variables with "|cdm_table  in their name."
    vars_with_bar_in_cdm_lite = [
        v
        for v in incobj.variables
        if "|" in v and v.split("|")[0] in cdm_lite_variables
    ]
    vars_in_cdm_lite += vars_with_bar_in_cdm_lite
    return vars_in_cdm_lite


def get_cdm_lite_variables(obs_api_url: str):
    return requests.get(f"{obs_api_url}/cdm/lite_variables").json()


def between(index, start, end):
    return (index >= start) & (index < end)


def get_param_name_in_data(retrieved_dataset, param_name):
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
            raise RuntimeError(f"Unknown parameter name {param_name}")
    return param_name_in_data


def ezclump(mask) -> list[slice]:
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


def get_code_mapping(
    incobj: h5netcdf.File | xarray.Dataset, inverse: bool = False
) -> dict:
    if isinstance(incobj, h5netcdf.File):
        attrs = incobj.variables["observed_variable"].attrs
    elif isinstance(incobj, xarray.Dataset):
        attrs = incobj["observed_variable"].attrs
    else:
        raise RuntimeError("Unsupported input type")
    if inverse:
        mapping = {c: v for v, c in zip(attrs["labels"], attrs["codes"])}
    else:
        mapping = {v: c for v, c in zip(attrs["labels"], attrs["codes"])}
    return mapping


def get_objects_to_retrieve(
    dataset_name: str, mapped_request: dict, obs_api_url: str
) -> list[str]:
    payload = dict(
        retrieve_args=dict(dataset=dataset_name, params=mapped_request),
        config=dict(size_limit=100000),
    )
    objects_to_retrieve = requests.post(
        f"{obs_api_url}/get_object_urls_and_check_size", json=payload
    ).json()
    return objects_to_retrieve

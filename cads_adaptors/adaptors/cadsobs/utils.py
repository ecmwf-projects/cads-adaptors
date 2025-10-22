import logging
import uuid
from pathlib import Path

import h5netcdf
from fsspec.implementations.http import HTTPFileSystem

from cads_adaptors.adaptors.cadsobs.constants import SPATIAL_COORDINATES
from cads_adaptors.adaptors.cadsobs.models import (
    RetrieveFormat,
)
from cads_adaptors.exceptions import CadsObsRuntimeError

logger = logging.getLogger(__name__)


def get_output_path(output_dir: Path, dataset: str, format: RetrieveFormat) -> Path:
    """Return the path of the output file."""
    if format == "csv":
        extension = ".csv"
    else:
        extension = ".nc"
    output_path = Path(output_dir, dataset + "_" + uuid.uuid4().hex + extension)
    return output_path


def add_attributes(
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


def get_output_dtype(ivar: str, ivarobj: h5netcdf.Variable) -> str:
    """Observed variable is a char, as we will decode them."""
    if ivar == "observed_variable":
        dtype = "S1"
    else:
        dtype = ivarobj.dtype
    return dtype


def handle_coordinate_renaming(vars_in_cdm_lite: list[str]) -> tuple[dict, list[str]]:
    """
    Rename spatial coordinates if needed.

    We have coordinates in latitude|observations format and want to rename them to
    just latitude. In case latitude|observations is not available, we rename
    latitude|station_configuration or latitude|header_table, the one available.
    In case both latitude|station_configuration and latitude|header_table are available,
    we won't rename them as we don't know hot to combine them.
    """
    vars_to_rename: dict[str, str] = dict()
    for varname in vars_in_cdm_lite.copy():
        if "|" in varname:
            vars_to_rename, vars_in_cdm_lite = _rename_coordinate(
                varname, vars_in_cdm_lite, vars_to_rename
            )
    return vars_to_rename, vars_in_cdm_lite


def _rename_coordinate(
    varname: str, vars_in_cdm_lite: list[str], vars_to_rename: dict[str, str]
) -> tuple[dict, list[str]]:
    """Rename spatial coordinates with | in their name."""
    varname_notable, table_name = varname.split("|")
    if varname_notable in SPATIAL_COORDINATES:
        if table_name == "observations_table":
            # latitude|observations table is renamed to latitude (also longitude)
            vars_to_rename[varname] = varname_notable
        elif table_name in ["station_configuration", "header_table"]:
            # if latitude|station_configuration or header table exist
            name_obs_table = f"{varname_notable}|observations_table"
            if table_name == "station_configuration":
                other_table = "header_table"
            else:
                other_table = "station_configuration"
            other = f"{table_name}|{other_table}"
            if name_obs_table not in vars_in_cdm_lite:
                # if latitude|observations exists does not exist, rename to
                # latitude/longitude.
                if other in vars_in_cdm_lite:
                    logger.info(f"Both {varname} and {other} exist,keeping them.")
                else:
                    vars_to_rename[varname] = varname_notable
            else:
                logger.info(
                    f"{name_obs_table} is set as {varname_notable} and {varname} is "
                    f"kept as it is."
                )
    return vars_to_rename, vars_in_cdm_lite


def get_param_name_in_data(retrieved_dataset: h5netcdf.File, param_name: str) -> str:
    match param_name:
        case "time_coverage":
            param_name_in_data = "report_timestamp"
        case "longitude_coverage" | "latitude_coverage":
            coord = param_name.split("_")[0]
            if f"{coord}|header_table" in retrieved_dataset.variables:
                param_name_in_data = f"{coord}|header_table"
            elif f"{coord}|station_configuration" in retrieved_dataset.variables:
                param_name_in_data = f"{coord}|station_configuration"
            else:
                param_name_in_data = coord
        case _:
            raise CadsObsRuntimeError(f"Unknown parameter name {param_name}")
    return param_name_in_data


def get_vars_in_cdm_lite(
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


def ezclump(mask) -> list[slice]:
    """
    Find the clumps (groups of data with the same values) for a 1D bool array.

    Internal function from numpy.ma.extras

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


def get_url_ncobj(fs: HTTPFileSystem, url: str) -> h5netcdf.File:
    """Open an URL as a netCDF file object with h5netcdf."""
    fobj = fs.open(url)
    logger.debug(f"Reading data from {url}.")
    # xarray won't read bytes object directly with netCDF4
    ncfile = h5netcdf.File(fobj, "r")
    return ncfile

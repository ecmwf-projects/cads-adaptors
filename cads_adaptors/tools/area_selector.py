from copy import deepcopy
import xarray as xr
from earthkit import data
from earthkit.aggregate import tools as eka_tools

from cads_adaptors.adaptors import Context


def incompatible_area_error(
    dim_key: str,
    start: float,
    end: float,
    coord_range: list,
    context: Context = Context(),
    thisError=ValueError,
):
    error_message = (
        "Your area selection is not yet compatible with this dataset.\n"
        f"Range selection for {dim_key}: [{start}, {end}].\n"
        f"Coord range from dataset: {coord_range}"
    )
    context.add_user_visible_error(error_message)
    raise thisError(error_message)


def wrap_longitudes(
    dim_key: str,
    start: float,
    end: float,
    coord_range: list,
    context: Context = Context(),
) -> list:
    start_in = deepcopy(start)
    end_in = deepcopy(end)

    start_shift_east = start_shift_west = end_shift_east = end_shift_west = False
    # Check if start/end are too low for crs:
    if start < coord_range[0]:
        start += 360
        if start > coord_range[1]:
            incompatible_area_error(dim_key, start_in, end_in, coord_range, context)
        start_shift_east = True
    if end < coord_range[0]:
        end += 360
        if end > coord_range[1]:
            incompatible_area_error(dim_key, start_in, end_in, coord_range, context)
        end_shift_east = True

    if start_shift_east and end_shift_east:
        return [slice(start, end)]
    elif start_shift_east and not end_shift_east:
        return [slice(start, coord_range[-1]), slice(coord_range[0], end)]
    elif end_shift_east or start_shift_east:
        incompatible_area_error(dim_key, start_in, end_in, coord_range, context)

    # Check if start/end are too high for crs:
    if start > coord_range[1]:
        start -= 360
        if start < coord_range[0]:
            incompatible_area_error(dim_key, start_in, end_in, coord_range, context)
        start_shift_west = True
    if end > coord_range[1]:
        end -= 360
        if end < coord_range[0]:
            incompatible_area_error(dim_key, start_in, end_in, coord_range, context)
        end_shift_west = True

    if start_shift_west and end_shift_west:
        return [slice(start, end)]
    elif end_shift_west and not start_shift_west:
        return [slice(start, coord_range[-1]), slice(coord_range[0], end)]
    elif end_shift_west or start_shift_west:
        incompatible_area_error(dim_key, start_in, end_in, coord_range, context)

    return [slice(start, end)]


def get_dim_slices(
    ds: xr.Dataset,
    dim_key: str,
    start: float,
    end: float,
    context: Context = Context(),
    longitude: bool = False,
) -> list:
    da_coord = ds[dim_key]

    direction = bool(da_coord[0] < da_coord[1])  # True = ascending, False = descending
    coord_del = (da_coord[1] - da_coord[0]).values
    if direction:
        coord_range = [
            da_coord[0].values - coord_del,
            da_coord[-1].values + coord_del,
        ]
    else:
        coord_range = [
            da_coord[-1].values + coord_del,
            da_coord[0].values - coord_del,
        ]

    # First see if requested range is within limits, if so, just ensure direction
    if all(
        [
            start >= coord_range[0],
            start <= coord_range[1],
            end >= coord_range[0],
            end <= coord_range[1],
        ]
    ):
        if direction:
            return [slice(start, end)]
        else:
            return [slice(end, start)]

    # If longitude, try wrapping:
    if longitude:
        return wrap_longitudes(dim_key, start, end, coord_range, context)

    incompatible_area_error(
        dim_key, start, end, coord_range, context, thisError=NotImplementedError
    )
    raise


def area_selector(
    infile: str,
    context: Context = Context(),
    area: list = [-90, -180, -90, +180],
    to_xarray_kwargs: dict = dict(),
    **kwargs,
):
    north, east, south, west = area

    # open object as earthkit data object
    ek_d = data.from_source("file", infile)

    ds = ek_d.to_xarray(**to_xarray_kwargs)

    spatial_info = eka_tools.get_spatial_info(ds)
    lon_key = spatial_info["lon_key"]
    lat_key = spatial_info["lat_key"]

    # Handle simple regular case:
    if spatial_info["regular"]:
        # Longitudes could return multiple slice in cases where the area wraps the "other side"
        lon_slices = get_dim_slices(
            ds,
            lon_key,
            east,
            west,
            context,
            longitude=True,
        )
        # We assume that latitudes won't be wrapped
        lat_slice = get_dim_slices(
            ds,
            lat_key,
            south,
            north,
            context,
        )[0]

        context.logger.debug(f"lat_slice: {lat_slice}\nlon_slices: {lon_slices}")

        sub_selections = []
        for lon_slice in lon_slices:
            sub_selections.append(
                ds.sel(
                    **{
                        spatial_info["lat_key"]: lat_slice,
                        spatial_info["lon_key"]: lon_slice,
                    }
                )
            )
        context.logger.debug(f"selections: {sub_selections}")

        ds_area = xr.concat(sub_selections, dim=lon_key)
        context.logger.debug(f"ds_area: {ds_area}")
        return ds_area

    else:
        context.add_user_visible_error(
            "Area selection not available for data projection"
        )
        raise NotImplementedError("Area selection not available for data projection")


def area_selector_paths(
    paths: list, area: list, context: Context, out_format: str = "netcdf"
):
    # We try to select the area for all paths, if any fail we return the original paths
    out_paths = []
    for path in paths:
        ds_area = area_selector(path, context, area=area)
        if out_format in ["nc", "netcdf"]:
            out_fname = ".".join(
                path.split(".")[:-1] + ["area-subset"] + [str(a) for a in area] + ["nc"]
            )
            context.logger.debug(f"out_fname: {out_fname}")
            ds_area.to_netcdf(out_fname)
            out_paths.append(out_fname)
        else:
            raise NotImplementedError(f"Output format not recognised {out_format}")
    return out_paths

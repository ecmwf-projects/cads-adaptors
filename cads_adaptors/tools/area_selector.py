import os
from copy import deepcopy
from typing import Any

import dask
import numpy as np
import xarray as xr
from earthkit.transforms import tools as eka_tools

from cads_adaptors.adaptors import Context
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools import adaptor_tools, convertors


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


def points_inside_range(points, point_range, how=any):
    return how(
        [point >= point_range[0] and point <= point_range[1] for point in points]
    )


def wrap_longitudes(
    dim_key: str,
    start: float,
    end: float,
    coord_range: list,
    context: Context = Context(),
) -> list:
    start_in = deepcopy(start)
    end_in = deepcopy(end)

    start_shift_east = end_shift_east = False
    # Check if need to shift bbox east
    if start < coord_range[0] and start + 360 < coord_range[1]:
        start += 360
        start_shift_east = True
        if end < coord_range[0]:
            end += 360
            end_shift_east = True
        # Things have been shifted, check if at least one point is within range
        if not points_inside_range([start, end], coord_range, how=any):
            incompatible_area_error(dim_key, start_in, end_in, coord_range, context)

    if start_shift_east and end_shift_east:
        return [slice(start, end)]
    elif start_shift_east and not end_shift_east:
        return [slice(start, coord_range[-1]), slice(coord_range[0], end)]

    start_shift_west = end_shift_west = False
    # Check if need to shift bbox west
    if end > coord_range[1] and end - 360 > coord_range[0]:
        end -= 360
        end_shift_west = True
        if start > coord_range[1]:
            start -= 360
            start_shift_west = True
        # Things have been shifted, check if at least one point is within range
        if not points_inside_range([start, end], coord_range, how=any):
            incompatible_area_error(dim_key, start_in, end_in, coord_range, context)

    if start_shift_west and end_shift_west:
        return [slice(start, end)]
    elif end_shift_west and not start_shift_west:
        return [slice(start, coord_range[-1]), slice(coord_range[0], end)]

    return [slice(start, end)]


def get_dim_slices(
    ds: xr.Dataset,
    dim_key: str,
    start: float,
    end: float,
    context: Context = Context(),
    longitude: bool = False,
    precision: int = 2,
) -> list:
    da_coord = ds[dim_key]

    ascending = bool(da_coord[0] < da_coord[1])  # True = ascending, False = descending
    coord_del = (da_coord[1] - da_coord[0]).values
    if ascending:
        coord_range = [
            np.round(da_coord[0].values - coord_del / 2.0, precision),
            np.round(da_coord[-1].values + coord_del / 2.0, precision),
        ]
    else:
        coord_range = [
            np.round(da_coord[-1].values + coord_del / 2.0, precision),
            np.round(da_coord[0].values - coord_del / 2.0, precision),
        ]

    if (
        # Requested range is within limits
        points_inside_range([start, end], coord_range, how=all)
        or
        # Requested range is encompasses limits
        (start <= coord_range[0] and end >= coord_range[1])
    ):
        if ascending:
            return [slice(start, end)]
        else:
            return [slice(end, start)]

    # If longitude, try wrapping:
    if longitude:
        return wrap_longitudes(dim_key, start, end, coord_range, context)

    # A final check that there is at least an overlap
    if not points_inside_range([start, end], coord_range):
        incompatible_area_error(
            dim_key, start, end, coord_range, context, thisError=NotImplementedError
        )

    return [slice(start, end)]


def area_selector(
    ds: xr.Dataset,
    area: list = [+90, -180, -90, +180],
    context: Context = Context(),
    **_kwargs,
) -> xr.Dataset:
    north, east, south, west = area

    # Get any area_selector_kwargs from adaptor config, take a copy as they will be updated here
    kwargs = deepcopy(_kwargs)

    spatial_info = eka_tools.get_spatial_info(
        ds,
        **{k: kwargs.pop(k) for k in ["lat_key", "lon_key"] if k in kwargs},
    )
    lon_key = spatial_info["lon_key"]
    lat_key = spatial_info["lat_key"]

    # Handle simple regular case:
    if spatial_info["regular"]:
        extra_kwargs = {k: kwargs.pop(k) for k in ["precision"] if k in kwargs}
        # Longitudes could return multiple slice in cases where the area wraps the "other side"
        lon_slices = get_dim_slices(
            ds, lon_key, east, west, context, longitude=True, **extra_kwargs
        )
        # We assume that latitudes won't be wrapped
        lat_slice = get_dim_slices(ds, lat_key, south, north, context, **extra_kwargs)[
            0
        ]

        context.debug(f"lat_slice: {lat_slice}\nlon_slices: {lon_slices}")

        sub_selections = []
        for lon_slice in lon_slices:
            sub_selections.append(
                ds.sel(
                    **kwargs,  # Any remaining kwargs are used for selection
                    **{
                        spatial_info["lat_key"]: lat_slice,
                        spatial_info["lon_key"]: lon_slice,
                    },
                )
            )
        context.debug(f"selections: {sub_selections}")

        ds_area = xr.concat(
            sub_selections, dim=lon_key, data_vars="minimal", coords="minimal"
        )
        context.debug(f"ds_area: {ds_area}")

        # Ensure that there are no length zero dimensions
        for dim in [lat_key, lon_key]:
            if len(ds_area[dim]) == 0:
                message = (
                    f"Area selection resulted in a dataset with zero length dimension for: {dim}.\n"
                    "Please ensure that your area selection covers at least one point in the data."
                )
                context.add_user_visible_error(message)
                raise InvalidRequest(message)

        return ds_area

    else:
        context.add_user_visible_error(
            "Area selection not available for data projection"
        )
        raise NotImplementedError("Area selection not available for data projection")


def area_selector_path(
    infile: str,
    area: list,
    context: Context,
    out_format: str | None = None,
    area_selector_kwargs: dict[str, Any] = {},
    open_datasets_kwargs: list[dict[str, Any]] | dict[str, Any] = {},
    **kwargs,
):
    # Deduce input format from infile
    in_ext = infile.split(".")[-1]
    in_format = adaptor_tools.handle_data_format(in_ext)
    if out_format is None:
        out_format = in_format

    # Set decode_times to False to avoid any unnecessary issues with decoding time coordinates
    if isinstance(open_datasets_kwargs, list):
        for _open_dataset_kwargs in open_datasets_kwargs:
            _open_dataset_kwargs.setdefault("decode_times", False)
    else:
        open_datasets_kwargs.setdefault("decode_times", False)

    ds_dict = convertors.open_file_as_xarray_dictionary(
        infile,
        **{
            **kwargs,
            "open_datasets_kwargs": open_datasets_kwargs,
        },
    )

    ds_area_dict = {
        ".".join([fname_tag, "area-subset"] + [str(a) for a in area]): area_selector(
            ds, area=area, context=context, **area_selector_kwargs
        )
        for fname_tag, ds in ds_dict.items()
    }

    # TODO: Consider using the write to file methods in convertors sub-module
    if out_format in ["nc", "netcdf"]:
        out_paths = [
            ds_area.compute().to_netcdf(os.path.join(fname_tag, "nc"))
            for fname_tag, ds_area in ds_area_dict.items()
        ]
    else:
        context.add_user_visible_error(
            f"Cannot write area selected data to {out_format}, writing to netcdf."
        )
        out_paths = [
            ds_area.compute().to_netcdf(os.path.join(fname_tag, "nc"))
            for fname_tag, ds_area in ds_area_dict.items()
        ]

    return out_paths


def area_selector_paths(
    paths: list,
    area: list,
    context: Context,
    **kwargs,
) -> list[str]:
    with dask.config.set(scheduler="threads"):
        # We try to select the area for all paths, if any fail we return the original paths
        out_paths = []
        for path in paths:
            try:
                out_paths += area_selector_path(path, area, context, **kwargs)
            except NotImplementedError:
                context.logger.debug(
                    f"could not convert {path} to xarray; returning the original data"
                )
                out_paths.append(path)
    return out_paths

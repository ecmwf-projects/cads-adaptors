import xarray as xr
from earthkit import aggregate, data


def incompatible_area_error(dim_key, start, end, spatial_info):
    raise NotImplementedError (
        "Your area selection is not yet compatible with this dataset.\n"
        f"Range selection for {dim_key}: [{start}, {end}].\n"
        f"Spatial definition of dataset: {spatial_info}"
    )


def wrap_longitudes(dim_key, start, end, coord_range, spatial_info=dict()) -> list:

    # Check if start/end are too low for crs:
    if start < coord_range[0]:
        start += 360
        if start > coord_range[1]:
            incompatible_area_error(dim_key, start, end, spatial_info)
        start_shift_east = True
    if end < coord_range[0]:
        end += 360
        if end > coord_range[1]:
            incompatible_area_error(dim_key, start, end, spatial_info)
        end_shift_east = True
    
    if start_shift_east and end_shift_east:
        return [slice(start, end)]
    elif start_shift_east and not end_shift_east:
        return [slice(start, coord_range[-1]), slice(coord_range[0], end)]
    elif end_shift_east or start_shift_east:
        incompatible_area_error(dim_key, start, end, spatial_info)

    # Check if start/end are too high for crs:
    if start > coord_range[1]:
        start -= 360
        if start < coord_range[0]:
            incompatible_area_error(dim_key, start, end, spatial_info)
        start_shift_west = True
    if end > coord_range[1]:
        end -= 360
        if end < coord_range[0]:
            incompatible_area_error(dim_key, start, end, spatial_info)
        end_shift_west = True
    
    if start_shift_west and end_shift_west:
        return [slice(start, end)]
    elif end_shift_west and not start_shift_west:
        return [slice(start, coord_range[-1]), slice(coord_range[0], end)]
    elif end_shift_west or start_shift_west:
        incompatible_area_error(dim_key, start, end, spatial_info)


def get_dim_slices(ds, dim_key, start, end, longitude=False, spatial_info=dict()) -> list:
    da_coord = ds[dim_key]

    direction = bool(da_coord[0] < da_coord[1])  # True = ascending, False = descending
    if direction:
        coord_range = da_coord[[0, -1]].values
    else:
        coord_range = da_coord[[-1, 0]].values

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
        return wrap_longitudes(dim_key, start, end, coord_range)
    
    incompatible_area_error(dim_key, start, end, spatial_info)


def area_selector(
    infile: str,
    area: list = [-90, -180, -90, +180],
    to_xarray_kwargs: dict = dict(),
    out_format: str = "netcdf",
    **kwargs,
):
    north, east, south, west = area

    # open object as earthkit data object
    ek_d = data.from_source("file", infile)

    ds = ek_d.to_xarray(**to_xarray_kwargs)

    spatial_info = aggregate.tools.get_spatial_info(ds)
    lon_key = spatial_info["lon_key"]
    lat_key = spatial_info["lat_key"]

    # Handle simple regular case:
    if spatial_info["regular"]:
        # Longitudes could return multiple slice in cases where the area wraps the "other side"
        lon_slices = get_dim_slices(ds, lon_key, east, west, longitude=True, spatial_info=spatial_info)
        # We assume that latitudes won't be wrapped
        lat_slice = get_dim_slices(ds, lat_key, south, north, spatial_info=spatial_info)[0]

        sub_selections = []
        for lon_slice in lon_slices:
            sub_selections.append(
                ds.sel(
                    **{
                        spatial_info["lat_key"]: lon_slice,
                        spatial_info["lon_key"]: lat_slice,
                    }
                )
            )

        ds_area = xr.concat(sub_selections, dim=lon_key)

    else:
        raise NotImplementedError("Area selection not available for data projection")

    if out_format in ["nc", "netcdf"]:
        out_fname = ".".join(
            infile.split(".")[:-1] + ["area-subset"] + [str(a) for a in area] + ["nc"]
        )
        ds_area.to_netcdf(out_fname)
        return out_fname


def area_selector_paths(paths: list, area: list):
    # We try to select the area for all paths, if any fail we return the original paths
    try:
        return [area_selector(path, area=area) for path in paths]
    except Exception:
        return paths

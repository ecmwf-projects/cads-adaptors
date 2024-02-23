import xarray as xr
from earthkit import aggregate, data


def get_dim_slices(ds, dim_key, start, end) -> list:
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
        lon_slices = get_dim_slices(ds, lon_key, east, west)
        # We assume that latitudes won't be wrapped
        lat_slice = get_dim_slices(ds, lat_key, south, north)[0]

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
            infile.split(".")[:1] + ["area-subset"] + [str(a) for a in area] + ["nc"]
        )
        ds_area.to_netcdf(out_fname)
        return out_fname


def area_selector_paths(paths: list, area: list):
    # We try to select the area for all paths, if any fail we return the original paths
    try:
        return [area_selector(path, area=area) for path in paths]
    except Exception:
        return paths

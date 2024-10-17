"""Code to extract an area subset from a regional model grib2 file."""

from collections import deque

from eccodes import (
    codes_clone,
    codes_get_array,
    codes_get_long,
    codes_grib_new_from_file,
    codes_release,
    codes_set,
    codes_set_array,
    codes_write,
)


def area_subset(area, infile, outfile):
    """Area-subset the grib2 fields in infile and write to outfile."""
    with open(infile, "rb") as fin, open(outfile, "wb") as fout:
        area_subset_fileobj(area, fin, fout)


def area_subset_fileobj(area, fin, fout):
    """Area-subset the grib2 fields in fin and write to fout."""
    # Loop over grib fields
    while True:
        hndl = codes_grib_new_from_file(fin)
        if hndl is None:
            break

        try:
            hndl2 = area_subset_handle(hndl, area)
        finally:
            codes_release(hndl)

        try:
            codes_write(hndl2, fout)
        finally:
            codes_release(hndl2)


def area_subset_handle(hndl, area):
    """Return a clone of the input grib2 handle containing only grid-points
    within the specified [N, W, S, E] area in degrees.
    """
    # Input grid details. Note that the first/last lat/lons (in integer
    # microdegrees) were set slightly incorrectly in many cases so we snap them
    # to a whole number of millidegrees to correct.
    grid_limits = [
        snap(codes_get_long(hndl, "latitudeOfFirstGridPoint")),
        snap(codes_get_long(hndl, "longitudeOfFirstGridPoint")),
        snap(codes_get_long(hndl, "latitudeOfLastGridPoint")),
        snap(codes_get_long(hndl, "longitudeOfLastGridPoint")),
    ]
    grid_incr = [
        codes_get_long(hndl, "jDirectionIncrement"),
        codes_get_long(hndl, "iDirectionIncrement"),
    ]
    grid_size = [codes_get_long(hndl, "Nj"), codes_get_long(hndl, "Ni")]
    if codes_get_long(hndl, "scanningMode") != 0:
        raise Exception("Code assumes scanning mode 0")

    # The input area in integer microdegrees and longitudes in the range
    # -180->180.  Using integer values means we don't have to rely on floating
    # point comparisons when determining if a point lies inside the area or not.
    area_md = [round(x * 1000000) for x in area]
    for x in area_md:
        assert type(x) is int, "Input type for area does not round() to int"
    area_md[1] = shift_lon(area_md[1], -180000000)
    area_md[3] = shift_lon(area_md[3], -180000000)

    # Construct lists of the row lats and column lons (integer microdegrees)
    lat_dirn = 1 if grid_limits[2] > grid_limits[0] else -1
    lats = [
        irow * grid_incr[0] * lat_dirn + grid_limits[0] for irow in range(grid_size[0])
    ]
    lons = [
        shift_lon(icol * grid_incr[1] + grid_limits[1], -180000000)
        for icol in range(grid_size[1])
    ]

    # Find the subset of rows and columns inside the area
    area_extracted = [x * 1000000 for x in [90, 180, -90, -180]]
    grid_size_new = [0, 0]
    for lat in lats:
        if lat >= area_md[2] and lat <= area_md[0]:
            grid_size_new[0] += 1
            area_extracted[0] = min(lat, area_extracted[0])
            area_extracted[2] = max(lat, area_extracted[2])
    for lon in lons:
        if lon >= area_md[1] and lon <= area_md[3]:
            grid_size_new[1] += 1
            area_extracted[1] = min(lon, area_extracted[1])
            area_extracted[3] = max(lon, area_extracted[3])
    npnts_new = grid_size_new[0] * grid_size_new[1]
    if npnts_new == 0:
        raise Exception("No grid points inside area")

    # Extract the values that are inside the area
    values = deque()
    for iv, value in enumerate(codes_get_array(hndl, "values")):
        latmd = lats[iv // grid_size[1]]
        lonmd = lons[iv % grid_size[1]]
        if (
            latmd >= area_md[2]
            and latmd <= area_md[0]
            and lonmd >= area_md[1]
            and lonmd <= area_md[3]
        ):
            values.append(value)
    if len(values) != npnts_new:
        raise Exception(
            "Did not extract expected number of points: "
            + "{} vs {}".format(len(values), npnts_new)
        )

    # Clone the input field, change grid details and insert new values
    hndl2 = codes_clone(hndl)
    frstLat = area_extracted[0 if lat_dirn == 1 else 2]
    lastLat = area_extracted[0 if lat_dirn != 1 else 2]
    frstLon = shift_lon(area_extracted[1], 0)
    lastLon = shift_lon(area_extracted[3], 0)
    codes_set(hndl2, "latitudeOfFirstGridPoint", frstLat)
    codes_set(hndl2, "longitudeOfFirstGridPoint", frstLon)
    codes_set(hndl2, "latitudeOfLastGridPoint", lastLat)
    codes_set(hndl2, "longitudeOfLastGridPoint", lastLon)
    codes_set(hndl2, "Nj", grid_size_new[0])
    codes_set(hndl2, "Ni", grid_size_new[1])
    codes_set(hndl2, "numberOfValues", npnts_new)
    codes_set_array(hndl2, "values", list(values))

    return hndl2


def snap(integer):
    """Round lat/lon in microdegrees to nearest millidegree."""
    # print(str(integer) + ' => ' + str(round(integer / 1000) * 1000))
    return round(integer / 1000) * 1000


def shift_lon(lon, min):
    """Return input longitude in microdegrees after shifting by n*360 degrees.
    Do this so it lies in the range min + 360 degrees > lon >= min.
    """
    while lon >= min + 360000000:
        lon -= 360000000
    while lon < min:
        lon += 360000000
    return lon


if __name__ == "__main__":
    area_subset([70.0, 0, 69.95, 0.05], "one_field.grib", "a.grib")

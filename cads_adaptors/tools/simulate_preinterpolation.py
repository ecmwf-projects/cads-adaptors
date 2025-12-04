from copy import deepcopy
from math import ceil, floor

from cads_adaptors.exceptions import CdsConfigError, InvalidRequest
from cads_adaptors.validation import enforce


def simulate_preinterpolation(request, cfg, context):
    """Return a copy of the request suitably altered to simulate pre-
    interpolation of the data to a regular grid. This is useful for
    maintaining consistency of output between GRIB fields which have been
    pre-interpolated and those which remain on a Gaussian grid, since MARS
    treats sub-areas differently for these two grid types if the grid
    keyword is not present.
    """
    request = deepcopy(request)
    keys = {k.lower().strip(): k for k in request.keys()}

    context.info(f'xxx keys={keys!r}')

    # Do nothing if the grid keyword is present, because then MARS will always
    # interpolate from the SW point for both regular and irregular grids
    if "grid" not in keys:

        # Set the grid to the notional pre-interpolation grid resolution
        try:
            request["grid"] = [
                str(cfg["grid"]["delta_lon"]),
                str(cfg["grid"]["delta_lat"]),
            ]
        except Exception:
            raise CdsConfigError(
                "Missing details in simulate_preinterpolation config"
            ) from None

        # If area is present then snap the corners inwards to the nearest
        # points on the notional pre-interpolation grid point
        area_key = keys.get("area")
        if area_key:
            area_orig = request[area_key]
            rr = enforce_sane_area(request)
            request[area_key] = [str(ll) for ll in snap_area(rr[area_key], cfg)]
            context.info(
                f"Area snapped from {area_orig!r} to {request[area_key]!r}"
            )

        context.info(f'xxx request={request!r}')

    return request


def enforce_sane_area(request, context, area_key=None):
    """Return a copy of the request having ensured that the area key, if
    present, is a list of four strings which represent valid numbers.
    """
    if not area_key:
        # Allow for varying case and whitespace in area keyword string
        keys = [k for k in request if k.lower().strip() == "area"]
        if len(keys) == 1:
            area_key = keys[0]
        elif len(keys) > 1:
            raise InvalidRequest("More than one area key in request")
        else:
            area_key = "area"

    lat = {"type": "number", "_splitOn": "/", "minimum": -90.0, "maximum": 90.0}
    lon = {"type": "number", "_splitOn": "/"}

    schema = {
        "_draft": "7",
        "type": "object",  # A dict...
        "properties": {
            area_key: {  # ...perhaps having an area key
                "type": "array",  # ...which is a list
                "minItems": 4,  # ...of 4 items
                "maxItems": 4,
                # Note that if draft 2020-12 is used, "items" should be
                # renamed "prefixItems"
                "items": [lat, lon, lat, lon],  # ...of numeric strings
            }
        },
    }

    return enforce.enforce(request, schema, context.logger)


def snap_area(area, cfg):
    """Return the input area after snapping the corners inwards to the nearest
    points of a grid whose details are provided in cfg.
    """
    area = [float(ll) for ll in area]
    north, west, south, east = area

    # Enforce numerical order: N > S and W+360 > E >= W
    south, north = sorted([south, north])
    if east != west:
        while east <= west:
            east += 360
        while east > west + 360:
            east -= 360

    # Extract details of grid to snap to, plus any other options
    try:
        dlon = float(cfg["grid"]["delta_lon"])
        dlat = float(cfg["grid"]["delta_lat"])
        if dlon <= 0 or dlat <= 0:
            raise Exception("delta_lon and delta_lat must be positive")
        lon0 = float(cfg["grid"].get("lon0", 0))
        lat0 = float(cfg["grid"].get("lat0", 0))
        round_ndp = int(cfg.get("round_ndigits", 9))
    except Exception as e:
        raise CdsConfigError(f"Invalid snap area: {cfg}: {e!r}")

    # Snap to grid points points inside area
    north = floor(round((north - lat0) / dlat, round_ndp)) * dlat + lat0
    west = ceil(round((west - lon0) / dlon, round_ndp)) * dlon + lon0
    south = ceil(round((south - lat0) / dlat, round_ndp)) * dlat + lat0
    east = floor(round((east - lon0) / dlon, round_ndp)) * dlon + lon0
    if north < south or east < west:
        raise InvalidRequest("request area contains no grid points")

    # Shift longitudes back into their original numerical ranges /
    # orders. It's not really the job of this function to be "fixing" those
    west -= floor((west - area[1] + 180) / 360) * 360
    east -= floor((east - area[3] + 180) / 360) * 360
    south, north = (south, north) if (area[0] > area[2]) else (north, south)

    # Round to cope with floating point errors that generate values like
    # 10.05000000000001
    return [round(ll, round_ndp) for ll in [north, west, south, east]]

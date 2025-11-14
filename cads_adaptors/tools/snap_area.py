from math import ceil, floor

from cads_adaptors.exceptions import CdsConfigError, InvalidRequest


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

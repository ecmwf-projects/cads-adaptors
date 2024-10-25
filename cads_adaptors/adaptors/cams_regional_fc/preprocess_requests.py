from copy import deepcopy
from math import ceil, floor

from cds_common import date_tools, hcube_tools

from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.validation.enforce import enforce as enforce_schema
from .formats import Formats


def preprocess_requests(context, requests, regapi):
    # Enforce basic type conformance
    requests = apply_schema(requests, context)

    # Get output format and remove from requests
    format = requests[0]["format"][0]
    for r in requests:
        r.pop("format", None)

    # Snap any requested area to the grid and replace 4-element list with
    # separate N/S/E/W keys or remove entirely if it's set to the full
    # area. Also get the set of all grids that will be present in the retrieved
    # fields.
    reqs2 = []
    model_grids = {}
    requested_grids = []
    for r in requests:
        area = r.pop("area", None)
        # Split the request into groups of sub-requests which all exist on the
        # same grid
        for grid, reqs_on_grid in regapi.split_request_by_grid(r).items():
            model_grids[grid] = model_grids.get(grid, []) + reqs_on_grid
            # Set the area using separate N/S/E/W keywords for the parts of this
            # request that are on this grid and get the grids that fields will
            # be returned on, which may be a subset of the full grid.
            for x in reqs_on_grid:
                x, requested_grid = set_area(x, area, dict(grid), context)
                reqs2.append(x)
                if requested_grid not in requested_grids:
                    requested_grids.append(requested_grid)
    requests = reqs2

    # We cannot convert to NetCDF format if the retrieved fields will be on
    # different grids
    if format != Formats.grib and len(requested_grids) > 1:
        raise InvalidRequest(
            "The model grid changed during the period requested. Fields on "
            + "different grids cannot be combined in one NetCDF file. "
            + "Please either request grib format, make separate requests or "
            + "explicitly specify an area that will result in output on a "
            + "single grid\n\n"
            + model_grids_table(model_grids, regapi),
            "",
        )

    # Ensure date lists are not in compressed form
    for r in requests:
        r["date"] = sorted(date_tools.expand_dates_list(r["date"]))

    # If extracting a sub-area locally rather than using the MF backend,
    # extract and remove from requests
    area, requests = locally_extracted_area(requests)

    info = {"format": format, "area": area, "stages": ["merge_grib"]}
    if format != Formats.grib:
        info["stages"].append("convert")

    return requests, info


def apply_schema(requests, context):
    """Enforce basic type conformance of the requests according to a schema."""
    mandatory_keys = [
        "variable",
        "model",
        "level",
        "date",
        "type",
        "time",
        "step",
        "format",
    ]
    recognised_keys = sorted(
        set(mandatory_keys).union(
            ["area", "no_cache", "__in_adaptor_no_cache", "_local_subarea"]
        )
    )

    schema = {
        "_draft": "7",
        "type": "array",
        "minItems": 1,
        "items": {
            "type": "object",
            "required": mandatory_keys,
            "additionalProperties": False,
            "properties": {},
            "_defaults": {"format": "grib"},
        },
    }

    # Sub-schemas for individual keys
    key_schema = {
        None: {"type": "array", "minItems": 1, "items": {"type": "string"}},
        "date": {
            "type": "array",
            "minItems": 1,
            "items": {"type": "string", "format": "date or date range"},
        },
        "area": {
            "type": "array",
            "minItems": 4,
            "maxItems": 4,
            "items": [
                {"type": "number", "minimum": -90.0, "maximum": 90.0},
                {"type": "number"},
                {"type": "number", "minimum": -90.0, "maximum": 90.0},
                {"type": "number"},
            ],
        },
        "format": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": {"type": "string", "enum": Formats.all},
        },
    }

    for key in recognised_keys:
        schema["items"]["properties"][key] = key_schema.get(key, key_schema[None])

    return enforce_schema(requests, schema, context)


def set_area(request, area_list, grid, context):
    """Adjust the area format.
    If a requested area is provided and is not the full model area then
    insert it in the request using separate north, south, east, west keywords.
    """
    # If no area is supplied, give it default values
    if area_list is None:
        area_list = [None, None, None, None]

    # Replace None's with default full-area values. None is the value supplied
    # by the UI when a restricted area is selected but no value is typed.
    default_area = {k: grid[k] for k in ["north", "west", "south", "east"]}
    area_list = [
        (v if v is not None else d) for v, d in zip(area_list, default_area.values())
    ]

    # Convert to dict and sanity-check
    area = {}
    for ia, (key, value) in enumerate(zip(default_area.keys(), area_list)):
        area[key] = float(value)
    if area["north"] <= area["south"]:
        raise InvalidRequest(
            "area north limit must be greater than " + "south limit"
        )
    if area["east"] <= area["west"]:
        raise InvalidRequest(
            "area east limit must be greater than " + "west limit"
        )

    # Snap to the grid
    area["north"] = snap_to_grid(area["north"], grid["south"], grid["dlat"], floor)
    area["west"] = snap_to_grid(area["west"], grid["west"], grid["dlon"], ceil)
    area["south"] = snap_to_grid(area["south"], grid["south"], grid["dlat"], ceil)
    area["east"] = snap_to_grid(area["east"], grid["west"], grid["dlon"], floor)
    if area["north"] < area["south"] or area["east"] < area["west"]:
        raise InvalidRequest(
            "requested area does not contain a " + "grid point"
        )

    # Only insert area in request if it's not the full area (for caching
    # reasons)
    request_out = deepcopy(request)
    for k, incr in [
        ("north", -grid["dlat"] / 2.0),
        ("south", grid["dlat"] / 2.0),
        ("east", -grid["dlon"] / 2.0),
        ("west", grid["dlon"] / 2.0),
    ]:
        if area[k] != grid[k] + incr:
            request_out.update({k: [v] for k, v in area.items()})

        # Also check the area lies within the grid or the Meteo France API will
        # return an error code.
        direction = 1 if incr < 0 else -1
        if area[k] * direction > (grid[k] + incr) * direction:
            raise InvalidRequest(
                "Area "
                + k
                + " value lies outside model grid limit of "
                + str(grid[k])
                + " for date(s)="
                + repr(request["date"])
            )

    # Return the requested grid, whether inserted into the request or not
    area["dlat"] = grid["dlat"]
    area["dlon"] = grid["dlon"]
    return request_out, area


def dirty_manual_tabulate(rows, headers, separator=",   "):
    """We implement this by hand to avoid issues with the tabulate dependency."""
    table = separator.join(headers) + "\n"
    for row in rows:
        table += separator.join(row) + "\n"
    return table


def model_grids_table(grids, regapi):
    """Return the text of a table summarising the regional model grids in use for the requested fields."""
    # Loop over each grid and the fields that were requested on that grid
    strings = []
    for grid, requested in grids.items():
        # Hypercubes detailing the fields that have this grid
        available = regapi.grids[grid]

        # For the requested fields on this grid, remove keys that do not appear
        # in available
        available_keys = sum([list(hcube.keys()) for hcube in available], ["date"])
        requested = [
            {k: v for k, v in r.items() if k in available_keys} for r in requested
        ]
        hcube_tools.hcubes_merge(requested)

        # Make nice strings to show the area, resolution and requested fields
        grid = dict(grid)
        area = (
            "["
            + ",".join(
                ["{:g}".format(grid[k]) for k in ["north", "west", "south", "east"]]
            )
            + "]"
        )
        resn = "[" + ",".join("{:g}".format(grid[k]) for k in ["dlon", "dlat"]) + "]"
        requested_strings = []
        for r in requested:
            if "date" in r:
                r["date"] = date_tools.compress_dates_list(r["date"])
            requested_strings.append(
                "{"
                + ", ".join([k + ": [" + ", ".join(v) + "]" for k, v in r.items()])
                + "}"
            )
        strings.append((area, resn, "[" + ",\n ".join(requested_strings) + "]"))

    return dirty_manual_tabulate(
        strings, headers=["Area [N,W,S,E]", "Resolution [dlon,dlat]", "Fields"]
    )


def snap_to_grid(coord, minl, incr, rounder):
    """Snap a lat or lon to the regional grid where the lat/lon min is minl and the grid length is incr."""
    raw = rounder((coord - (minl + incr / 2)) / incr) * incr + (minl + incr / 2)
    # Rounding error can lead to spurious significant figures. This is a
    # poor-man's attempt at getting the number of decimal places the result
    # should be rounded to.
    ndp = len(str(incr / 2).split(".")[1])
    return round(raw, ndp)


def locally_extracted_area(requests):
    """If extracting a sub-area locally (rather than having the backend do it)
    then remove the area from the requests (so the area downloded from the
    backend will be the full area) and return it. Otherwise leave it in the
    requests (if present) and return None.
    """
    area = None
    requests = deepcopy(requests)

    # Extracting sub-area locally?
    local_subarea = False
    for r in requests:
        local_subarea = local_subarea or bool(r.get("_local_subarea", [1])[0])
        r.pop("_local_subarea", None)

    # If extracting the sub-area locally, get the area as a list and remove from
    # requests
    if local_subarea:
        areas = set()
        for r in requests:
            if "north" in r:
                areas.add(
                    tuple([r.pop(k)[0] for k in ["north", "west", "south", "east"]])
                )
        if len(areas) > 1:
            raise Exception("More than one area specified???")
        if areas:
            area = list(areas.pop())

    return area, requests

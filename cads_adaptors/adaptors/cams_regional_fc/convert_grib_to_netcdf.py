from datetime import datetime, timedelta
from itertools import product

import numpy as np
from cds_common.message_iterators import grib_file_iterator
from eccodes import codes_get, codes_get_values, codes_set
from netCDF4 import Dataset

# Replicate flaws in existing Meteo France NetCDF?
replicate_flaws = False


def convert_grib_to_netcdf(requests, gribfile, ncfile, regfc_defns):
    """Convert CAMS regional model data from grib to NetCDF which is
    identical (or as near as sensible) to the NetCDF created and
    distributed by the old Meteo France API
    """
    # Get the hypercube envelope of all requests
    envelope = {"request": envelope_request(requests)}

    # Grib for no more than one model and type should be supplied
    assert len(envelope["request"]["model"]) == 1
    assert len(envelope["request"]["type"]) == 1

    # Information associated with each of the ADS variables
    vdefns = {d["backend_api_name"]: d for d in regfc_defns["variable"]}
    envelope["species"] = [vdefns[var] for var in envelope["request"]["variable"]]

    # Compute a list of all requested validity times
    envelope["dates"] = sorted(
        [datetime.strptime(d, "%Y-%m-%d") for d in envelope["request"]["date"]]
    )
    for t in envelope["request"]["time"]:
        assert len(t) == 4
    envelope["vtimes"] = sorted(
        [
            d + timedelta(hours=int(t[0:2]) + int(t[2:4]) / 60.0 + int(s))
            for d, t, s in product(
                envelope["dates"],
                envelope["request"]["time"],
                envelope["request"]["step"],
            )
        ]
    )
    assert len(set(envelope["vtimes"])) == len(
        envelope["vtimes"]
    ), "Input request validity times are not unique!"

    envelope["levels"] = sorted([float(x) for x in envelope["request"]["level"]])

    # print('Creating ' + ncfile)
    with Dataset(ncfile, "w", format="NETCDF3_CLASSIC") as nc:
        _convert_grib_to_netcdf(envelope, gribfile, nc, regfc_defns)


def _convert_grib_to_netcdf(envelope, gribfile, nc, regfc_defns):
    # The grib keys to read from each field. If provided as a tuple then
    # the second element represents the type to read it as.
    keys2read = [
        "parameterNumber",
        "constituentType",
        "productDefinitionTemplateNumber",
        ("dataDate", str),
        ("dataTime", str),
        "typeOfProcessedData",
        "forecastTime",
        "level",
        "Ni",
        "Nj",
        "latitudeOfFirstGridPointInDegrees",
        "latitudeOfLastGridPointInDegrees",
        "longitudeOfFirstGridPointInDegrees",
        "longitudeOfLastGridPointInDegrees",
    ]

    # Grib keys which should be the same for all fields. This will be checked.
    constant_keys = [
        "Ni",
        "Nj",
        "latitudeOfFirstGridPointInDegrees",
        "latitudeOfLastGridPointInDegrees",
        "longitudeOfFirstGridPointInDegrees",
        "longitudeOfLastGridPointInDegrees",
    ]

    first = True
    const_hdr = None
    written = np.zeros(
        (len(envelope["vtimes"]), len(envelope["levels"]), len(envelope["species"])),
        dtype=bool,
    )
    for msg in grib_file_iterator(gribfile):
        # Read the field header
        hdr = {}
        for k in keys2read:
            k, ktype = k if isinstance(k, tuple) else (k, None)
            hdr[k] = codes_get(msg, k, ktype=ktype)
            # Temporary fix because of small rounding errors in Meteo France
            # grib
            if "latitude" in k or "longitude" in k:
                hdr[k] = round(hdr[k], 4)

        # Check all fields have same values for constant keys
        const = {k: hdr[k] for k in constant_keys}
        if const_hdr is None:
            const_hdr = const
        elif const != const_hdr:
            raise Exception(
                "Some of these grib keys are not the same for "
                + "every field: "
                + repr(const_hdr.keys())
                + "\n    First field: "
                + repr(const_hdr)
                + "\n    This field: "
                + repr(const)
            )

        # Initialise the output file on the first iteration
        if first:
            first = False
            ncinit(envelope, nc, hdr, regfc_defns)

        # Write the grib message to file
        try:
            write_msg(msg, hdr, envelope, nc, written)
        except Exception as e:
            raise Exception(
                "Encountered exception when processing grib "
                + "field "
                + repr(hdr)
                + ": "
                + repr(e)
            )


def write_msg(msg, hdr, envelope, nc, written):
    """Write the grib message to the file"""
    # Construct the validity time
    vtime = datetime.strptime(
        hdr["dataDate"] + " " + hdr["dataTime"], "%Y%m%d %H%M"
    ) + timedelta(hours=hdr["forecastTime"])

    # Get the index of this variable in envelope['species']
    ispecies = None
    for ii, defn in enumerate(envelope["species"]):
        # Does the message match any of the possible GRIB encodings for this
        # variable?
        for gribdef in defn["grib_representations"]:
            if all([hdr[k] == gribdef[k] for k in gribdef.keys()]):
                # hdr matches gribdef
                ispecies = ii
                break
        if ispecies is not None:
            break
    if ispecies is None:
        raise Exception("Unrecognised variable: " + repr(hdr))

    # Get the netCDF variable for this species
    species_ncdef = envelope["species"][ispecies]["netcdf"]
    var = nc.variables[species_ncdef["varname"]]

    # Ensure codes_get_values() returns missing data with values that will be
    # mapped to the variable fill value.
    codes_set(msg, "missingValue", var._FillValue / species_ncdef["scale"])

    # Fill the appropriate slice of the main variable arrays
    itime = envelope["vtimes"].index(vtime)
    ilev = envelope["levels"].index(hdr["level"])
    var[itime, ilev, :, :] = codes_get_values(msg) * species_ncdef["scale"]

    # Check no duplicates
    if written[itime, ilev, ispecies]:
        raise Exception("Duplicate field in input file")
    written[itime, ilev, ispecies] = True


def envelope_request(requests):
    """Return the envelope hypercube of all requests"""
    # Ensure that requests is a list and its values are lists
    if not isinstance(requests, list):
        requests = [requests]
    for req in requests:
        for k, v in list(req.items()):
            if not isinstance(v, list):
                req[k] = [v]

    # Make the envelope. Ignore area because, if the requests span a period in
    # which the model grid changes, some requests may have it while others
    # don't in order that all retrieved fields end up on the same grid.
    def keyset(req):
        return set(
            [k for k in req.keys() if k not in ["north", "south", "east", "west"]]
        )

    keys = keyset(requests[0])
    envelope_req = {k: set() for k in keys}
    for req in requests:
        if keyset(req) != keys:
            raise Exception(
                "Input requests do not all have same keys: "
                + repr(keys)
                + " "
                + repr(keyset(req))
            )
        for k in keys:
            envelope_req[k].update(set(req[k]))
    for k in keys:
        envelope_req[k] = sorted(list(envelope_req[k]))

    return envelope_req


def ncinit(envelope, nc, hdr, regfc_defns):
    """Initialise the NetCDF file"""
    typename = envelope["request"]["type"][0].upper()

    set_globatts(nc, envelope, typename, regfc_defns)

    nc.createDimension("longitude", hdr["Ni"])
    nc.createDimension("latitude", hdr["Nj"])
    nc.createDimension("level", len(envelope["levels"]))
    nc.createDimension("time", None)

    vlon = nc.createVariable("longitude", "f4", ("longitude",))
    vlon.long_name = "longitude"
    vlon.units = "degrees_east"

    vlat = nc.createVariable("latitude", "f4", ("latitude",))
    vlat.long_name = "latitude"
    vlat.units = "degrees_north"

    vlev = nc.createVariable("level", "f4", ("level",))
    vlev.long_name = "level"
    vlev.units = "m"

    vbasetime = envelope["dates"][0]
    vtime = nc.createVariable("time", "f4", ("time",))
    vtime.long_name = typename + " time from " + vbasetime.strftime("%Y%m%d")
    vtime.units = "hours"

    for species in envelope["species"]:
        atts = species["netcdf"]
        vdata = nc.createVariable(
            atts["varname"],
            "f4",
            (
                "time",
                "level",
                "latitude",
                "longitude",
            ),
            fill_value=-999.0,
        )
        vdata.species = atts["species"]
        vdata.units = atts["units"]
        vdata.value = "hourly values"
        sn = atts.get("standard_name", species.get("standard_name"))
        if not sn:
            sn = "Not Defined"
        vdata.standard_name = sn

    vtime[:] = [(t - vbasetime).total_seconds() / 3600.0 for t in envelope["vtimes"]]
    vlev[:] = envelope["levels"]

    # Compute grid latitudes
    if hdr["Nj"] > 1:
        dlat = (
            hdr["latitudeOfLastGridPointInDegrees"]
            - hdr["latitudeOfFirstGridPointInDegrees"]
        ) / (hdr["Nj"] - 1)
    else:
        dlat = 0
    lats = np.array(
        [hdr["latitudeOfFirstGridPointInDegrees"] + i * dlat for i in range(hdr["Nj"])]
    )

    # Compute grid longitudes
    while (
        hdr["longitudeOfFirstGridPointInDegrees"]
        > hdr["longitudeOfLastGridPointInDegrees"]
    ):
        hdr["longitudeOfFirstGridPointInDegrees"] -= 360.0
    if hdr["Ni"] > 1:
        dlon = (
            hdr["longitudeOfLastGridPointInDegrees"]
            - hdr["longitudeOfFirstGridPointInDegrees"]
        ) / (hdr["Ni"] - 1)
    else:
        dlon = 0
    lons = np.array(
        [hdr["longitudeOfFirstGridPointInDegrees"] + i * dlon for i in range(hdr["Ni"])]
    )
    # Ensure lons are in the range 0 to 360
    lons = lons - np.floor(lons / 360.0) * 360.0
    assert not np.any((lons < 0) | (lons > 360))

    vlat[:] = np.round(lats, 7)
    vlon[:] = np.round(lons, 7)


def set_globatts(nc, envelope, typename, regfc_defns):
    """Set global attributes"""
    envreq = envelope["request"]
    mdefns = {d["backend_api_name"]: d for d in regfc_defns["model"]}
    model_atts = mdefns[envreq["model"][0]]["netcdf"]
    if replicate_flaws and typename == "FORECAST":
        model_name2 = ""
    else:
        model_name2 = model_atts["name2"]
    if envreq["level"] == ["0"]:
        levstr = "the Surface"
    else:
        levstr = str(len(envelope["levels"])) + " levels"
    if len(envelope["dates"]) == 1:
        date_string = envelope["dates"][0].strftime("%Y%m%d")
    else:
        date_string = "-".join(
            [envelope["dates"][i].strftime("%Y%m%d") for i in [0, -1]]
        )
    if typename == "ANALYSIS":
        times = [int(x[0:2]) for x in envreq["time"]]
        time_string = "{}+[{}H_{}H]".format(date_string, min(times), max(times))
    elif typename == "FORECAST":
        steps = [int(x) for x in envreq["step"]]
        time_string = "{}+[{}H_{}H]".format(date_string, min(steps), max(steps))
    else:
        raise Exception("Unrecognised type: " + typename)

    shortnames = "/".join(
        [species["netcdf"]["shortname"] for species in envelope["species"]]
    )
    title_template = "{species} Air Pollutant {fc_an} at {levels}"
    if replicate_flaws and envreq["level"] != ["0"]:
        title_template += ": "
    nc.title = title_template.format(species=shortnames, fc_an=typename, levels=levstr)
    nc.institution = "Data produced by " + model_atts["institution"]
    nc.source = "Data from " + model_atts["name"] + " model"
    nc.history = "Model " + model_name2 + " " + typename
    if typename == "ANALYSIS":
        nc.ANALYSIS = "Europe, " + time_string
    elif typename == "FORECAST":
        nc.FORECAST = "Europe, " + time_string
    else:
        raise Exception("Unrecognised type: " + typename)
    nc.summary = (
        "{modelname} model hourly {fc_an} of {species} "
        + "concentration at {levels} from {date_steps} on "
        + "Europe"
    ).format(
        modelname=model_name2,
        fc_an=typename,
        species=shortnames,
        levels=levstr,
        date_steps=time_string,
    )
    nc.project = "MACC-RAQ (http://macc-raq.gmes-atmosphere.eu)"

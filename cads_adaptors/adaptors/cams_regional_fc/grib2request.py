"""Code that takes a grib message, reverse engineers and returns the associated ADS API request dictionary."""

from eccodes import codes_get

grib_key_types = {}
field_data: dict = {}


def grib2request_init(regfc_defns):
    """Initialise global variables: grib_key_types and field_data. This is so
    that it doesn't need to be done multiple times or in grib2request(),
    which is called from places where the path to the dataset directory is
    not easily available.
    """
    # Do not execute twice
    if field_data:
        return

    # Link grib representations to API request values
    field_data.update(
        {
            "variable": {d["backend_api_name"]: d for d in regfc_defns["variable"]},
            "model": {d["backend_api_name"]: d for d in regfc_defns["model"]},
            "type": {
                "FORECAST": {"grib_representations": [{"typeOfProcessedData": "fc"}]},
                "ANALYSIS": {
                    "grib_representations": [
                        {"typeOfProcessedData": "an", "forecastTime": 0}
                    ]
                },
            },
        }
    )

    # Get all required grib keys and their associated types from field_data
    grib_key_types.update({"dataDate": int, "dataTime": int, "forecastTime": int})
    for api_key, key_info in field_data.items():
        for backend_value, value_info in key_info.items():
            for grib_defn in value_info["grib_representations"]:
                for grib_key, grib_value in grib_defn.items():
                    if grib_key not in grib_key_types:
                        grib_key_types[grib_key] = type(grib_value)

                    elif grib_key_types[grib_key] is not type(grib_value):
                        raise Exception(
                            "All values for a given grib key "
                            + "should have same type: "
                            + grib_key
                        )


def grib2request(msg):
    """Return the ADS API request dict that corresponds to the input grib message."""
    if not field_data:
        raise Exception(
            "You must call the initialisation function before this " "function"
        )

    request = {"level": codes_get(msg, "level", ktype=str)}

    # Read required grib keys for the message
    fld = {}
    for grib_key, ktype in grib_key_types.items():
        try:
            fld[grib_key] = codes_get(msg, grib_key, ktype=ktype)
        except Exception as e:
            raise Exception('Failed to get grib key "' + grib_key + '": ' + str(e))

    # Loop over API request keys listed in field_data
    for api_key, key_info in field_data.items():
        # Loop over possible API values for this keyword
        for backend_value, value_info in key_info.items():
            # Loop over possible GRIB encodings for this API value
            for grib_defn in value_info["grib_representations"]:
                # Check if all associated grib keys have correct value
                for grib_key, grib_value in grib_defn.items():
                    if fld[grib_key] != grib_value:
                        break
                else:
                    # All grib keys matched
                    request[api_key] = backend_value
                    break

            if api_key in request:
                break

        if api_key not in request:
            # Not finding a match is an error
            raise Exception(
                "Field does not match any known ADS " + api_key + ": " + repr(fld)
            )

    # Set the date and time
    date = fld["dataDate"]
    time = fld["dataTime"]
    request["date"] = "{}-{:02d}-{:02d}".format(
        date // 10000, (date // 100) % 100, date % 100
    )
    request["time"] = "{:02d}{:02d}".format(time // 100, time % 100)

    # Set the step
    if request["type"] == "FORECAST":
        request["step"] = str(fld["forecastTime"])
    elif request["type"] == "ANALYSIS":
        request["step"] = "0"
    else:
        raise Exception("Unrecognised request type: " + repr(request["type"]))

    return request


if __name__ == "__main__":
    from eccodes import codes_grib_new_from_file

    grib2request_init(
        "/home/nal/cds/cds-forms-cams/" "cams-europe-air-quality-forecasts"
    )
    # with open('/home/rd/nal/cds/regional/interp2cities/' +
    #          'one_param_one_day.grib') as f:
    with open("a.grib") as f:
        while True:
            msg = codes_grib_new_from_file(f)
            if msg is None:
                break
            print(grib2request(msg))

import hashlib
import logging
import os
import re
import time
import traceback


class BadRequest(Exception):
    pass


class NoData(Exception):
    pass


def solar_rad_retrieve(
    request, outfile=None, user_id="0", ntries=10, logger=logging.getLogger(__name__)
):
    """Execute a CAMS solar radiation data retrieval."""
    # Hash the user ID just in case it contains anything private. Then encode it so
    # the data provider can verify it's from us.
    user_id_hash = hashlib.md5(str(user_id).encode()).hexdigest()
    req = {"username": encode(user_id_hash)}
    logger.info(f'Encoded user ID is {req["username"]!r}')
    req.update(request)

    # Set expert_mode depending on format
    req["expert_mode"] = {True: "true", False: "false"}.get(
        req["data_format"] == "csv_expert"
    )

    # Set the MIME type from the format
    if req["data_format"].startswith("csv"):
        req["mimetype"] = "text/csv"
    elif req["data_format"] == "netcdf":
        req["mimetype"] = "application/x-netcdf"
    else:
        raise BadRequest(f'Unrecognised format: "{req["data_format"]}"')

    # We could use the URL API or the WPS API. Only WPS has the option for
    # NetCDF and it has better error handling.
    # retrieve_by_url(req, outfile, logger)
    retrieve_by_wps(req, outfile, ntries, logger)


def encode(user_id):
    """Encode a user ID in a way that could only be done by someone who knows a
    secret string. The string is only known by ECMWF and the contractor. This
    allows the contractor have confidence that the request has come from us.
    """
    hash = hashlib.md5(
        (user_id + os.environ["CAMS_SOLAR_SECRET_STRING"]).encode()
    ).hexdigest()
    return user_id + hash


def verify(encoded):
    """Given an encoded user ID encoded (created by encode(user_id)), verify
    that it is valid. This function is not used in the adaptor but is included
    here to demonstrate how the contractor can validate the string at their
    end. It's important that verify(encode(any_string)) always returns True, but
    that verify(any_string) in general returns False.
    """
    user_id = encoded[:-32]
    return encode(user_id) == encoded


def retrieve_by_wps(req, outfile, ntries, logger):
    """Execute a CAMS solar radiation data retrieval through the WPS API."""
    # Construct the XML to pass
    import jinja2

    xml = jinja2.Template(template_xml()).render(req)
    logger.debug("request=" + repr(req))
    logger.debug("xml=" + xml)
    xml = xml.replace("\n", "")

    # Execute WPS requests in a retry-loop, cycling through available servers.
    # Nowadays the only supported server is the load-balancing server:
    # api.soda-solardata.com.
    servers = ["api.soda-solardata.com"]
    attempt = 0
    exc_txt = ""
    while attempt < ntries:
        attempt += 1
        if attempt > 1:
            logger.info(f"Attempt #{attempt}...")

        # Cycle through available servers on each attempt
        server = servers[(attempt - 1) % len(servers)]
        url = f"https://{server}/service/wps"

        try:
            wps_execute(url, xml, outfile, logger)

        except (BadRequest, NoData):
            # Do not retry
            raise

        except Exception as ex:
            exc_txt = ": " + repr(ex)
            tbstr = "".join(traceback.format_tb(ex.__traceback__))
            logger.error(
                f"Execution attempt #{attempt} from {server} "
                f"failed: {ex!r}    \n" + "    \n".join(tbstr.split("\n"))
            )
            # Only start sleeping when we've tried all servers
            if attempt >= len(servers):
                time.sleep(3)
            logger.debug("Retrying...")

        else:
            break

    else:
        logger.error("Request was " + repr(req))
        logger.error("XML was " + xml)
        raise Exception(f"Failed to retrieve data after {attempt} attempts" + exc_txt)
    if attempt > 1:
        logger.info(f"Succeeded after {attempt} attempts")


def wps_execute(url, xml, outfile, logger):
    # Execute WPS. This can throw an immediate exception if the service is
    # down
    from owslib.wps import WebProcessingService

    wps = WebProcessingService(url, skip_caps=True, timeout=3600)
    execution = wps.execute(None, [], request=bytes(xml, encoding="utf-8"))

    # Wait for completion
    while not execution.isComplete():
        execution.checkStatus(sleepSecs=1)
        logger.debug("Execution status: %s" % execution.status)

    # Save the output if succeeded
    if execution.isSucceded():
        if outfile is not None:
            execution.getOutput(outfile)

    else:
        # Certain types of error are due to bad requests. Distinguish these
        # from unrecognised system errors.
        known_user_errors = {
            NoData: [
                "Error: incorrect dates",
                "Error: no data available for the period",
            ],
            BadRequest: [
                "outside of the satellite field of view",
                "Maximum number of daily requests reached",
                "Unknown string format",
            ],
        }  # Bad date string
        user_error = None
        for error in execution.errors:
            logger.error("WPS error: " + repr([error.code, error.locator, error.text]))

            for extype, strings in known_user_errors.items():
                for string in strings:
                    if string.lower() in error.text.lower():
                        user_error = (
                            extype,
                            re.sub(r"^Process error: *(.+)", r"\1", error.text),
                        )

        # If there was just one, familiar type of error then raise the
        # associated exception type. Otherwise raise Exception.
        if len(execution.errors) == 1 and user_error:
            raise user_error[0](tidy_error(user_error[1]))
        elif len(execution.errors) > 0:
            raise Exception("\n".join([e.text for e in execution.errors]))
        else:
            logger.error("WPS failed but gave no errors?")
            raise Exception("Unspecified WPS error")


def tidy_error(text):
    lines = [line.strip() for line in text.split("\n")]
    text = "; ".join([line for line in lines if line])
    return re.sub(r"^ *Failed to execute WPS process \[\w+\]: *", "", text)


def template_xml():
    """Return a Jinja2 template XML string that can be used to obtain data via WPS."""
    return """
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<wps:Execute service="WPS" version="1.0.0"
    xmlns:ows="http://www.opengis.net/ows/1.1"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../schemas/wps/1.0.0/wpsExecute_request.xsd"
    xmlns:wps="http://www.opengis.net/wps/1.0.0">
    <ows:Identifier>{{ sky_type }}</ows:Identifier>
    <wps:DataInputs>
            <wps:Input>
                <ows:Identifier>latitude</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ "{0:.5f}".format(location["latitude"]) }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>longitude</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ "{0:.5f}".format(location["longitude"]) }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>altitude</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ altitude }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>date_begin</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ date[0:10] }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>date_end</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>
                            {% if date|length > 10 %}{{ date[11:] }}{% else %}{{ date[0:10] }}{% endif %}
                        </wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>time_ref</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ time_reference }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>summarization</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ time_step }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>verbose</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ expert_mode }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
        <wps:Input>
            <ows:Identifier>username</ows:Identifier>
                <wps:Data>
                        <wps:LiteralData>{{ username }}</wps:LiteralData>
                </wps:Data>
            </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
            <wps:ResponseDocument storeExecuteResponse="false">
                <wps:Output mimeType="{{ mimetype }}" asReference="true">
                        <ows:Identifier>irradiation</ows:Identifier>
                </wps:Output>
            </wps:ResponseDocument>
    </wps:ResponseForm>
</wps:Execute>
    """


def test_solar_rad_retrieve():
    sky_types = ["get_cams_radiation", "get_mcclear"]
    time_refs = ["UT", "TST"]
    formats = ["csv", "csv_expert", "netcdf"]
    request = {
        "altitude": "-999",
        "date": "2022-02-20/2022-02-20",
        "location": {"latitude": 0.0, "longitude": 0.0},
        "sky_type": sky_types[0],
        "time_reference": time_refs[0],
        "time_step": "PT15M",
        "data_format": formats[0],
    }
    solar_rad_retrieve(request, "a.dat", user_id=123, ntries=1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_solar_rad_retrieve()

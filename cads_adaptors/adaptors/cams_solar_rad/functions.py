import copy
import hashlib
import logging
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
    user_id = anonymised_user_id(user_id)
    req = {"username": user_id}
    req.update(request)

    # Set expert_mode depending on format
    req["expert_mode"] = {True: "true", False: "false"}.get(
        req["format"] == "csv_expert"
    )

    # Set the MIME type from the format
    if req["format"].startswith("csv"):
        req["mimetype"] = "text/csv"
    elif req["format"] == "netcdf":
        req["mimetype"] = "application/x-netcdf"
    else:
        raise BadRequest(f'Unrecognised format: "{req["format"]}"')

    # We could use the URL API or the WPS API. Only WPS has the option for
    # NetCDF and it has better error handling.
    # retrieve_by_url(req, outfile, logger)
    retrieve_by_wps(req, outfile, ntries, logger)


def to_scalars_values(request):
    request = copy.deepcopy(request)
    is_possible = True
    for key in request:
        if isinstance(request[key], (list, tuple, set)):
            if len(request[key]) == 1:
                request[key] = list(request[key])[0]
            else:
                is_possible = False
        elif isinstance(request[key], dict):
            request[key], part_is_possible = to_scalars_values(request[key])
            is_possible &= part_is_possible
    return request, is_possible


def determine_result_filename(config, request):
    EXTENSIONS = {"csv": "csv", "csv_expert": "csv", "netcdf": "nc"}
    request_uid = config.get("request_uid", "no-request-uid")
    extension = EXTENSIONS.get(request["format"], "csv")
    return f"result-{request_uid}.{extension}"


def get_numeric_user_id(ads_user_id):
    return str(int(ads_user_id.replace("-", ""), 16) % 10**6)


def anonymised_user_id(ads_user_id):
    # We have to pass a unique user ID to the provider but, for privacy reasons,
    # we want it to be as anonymous as possible. Use a hash of the ADS user ID,
    # which as an integer is already pretty anonymous but this takes it further
    # for safety. Note that we include an additional string in the hash because
    # if it's known to be a hash of an integer then it's trivial to reverse.
    user_id = hashlib.md5(
        (f"__%SecretFromDataProvider%__{ads_user_id}").encode()
    ).hexdigest()

    # The data provider needs to be confident that this request actually is
    # coming from the ADS though, so a valid user_id should not be easy to
    # guess. To this end, we attach one of a number of fixed prefixes which are
    # known to the provider.
    # These are 100 ten-character random strings generated with
    #   [''.join([random.choice(string.ascii_lowercase+string.digits)
    #             for x in range(10)])
    #    for y in range(100)]
    prefixes = [
        "p1przq6umd",
        "u3b0kpo03n",
        "p7040vspzp",
        "li3p20bdeu",
        "bzrd6fxi1k",
        "0wi1278hmc",
        "wy97an8lvb",
        "5uc8v70tjd",
        "z363vyfmsx",
        "7fwqcqnbkj",
        "y0vhbbuf45",
        "vcjb3ywu4v",
        "dfnel8yw9e",
        "14jtrrluo9",
        "z6ttwnmqup",
        "2vwm55v58m",
        "e991ro2y08",
        "te18dbyva5",
        "03r00ip9by",
        "db3yyauvke",
        "0jugwocwea",
        "z9lqg6ht69",
        "opfosf4e14",
        "jrkm2lnww4",
        "j1n0vu1eew",
        "d87j1lu4kc",
        "9m90b12ood",
        "kdqm2yikbd",
        "rowzooxxgs",
        "e4pp6g7oef",
        "43u1r9r09b",
        "v277x86ddz",
        "efc8hfpda1",
        "2djtds47ss",
        "5sioewwsia",
        "kgmvklxmhf",
        "kksqxkadvw",
        "1vnyn2a8u0",
        "8uz3qvs4rt",
        "w51gmulgne",
        "g3ry4uo2mv",
        "15w92afblk",
        "wsa6ewkfrq",
        "c72ppq2oae",
        "9f1v8xnqva",
        "dnhqvtifoi",
        "ufjq1lx8v6",
        "c7v5jfre33",
        "p1x0fbq5mg",
        "adv5727kly",
        "j7ite32koa",
        "da5dpm9ugj",
        "jzz9ziydir",
        "6k8qrjxswv",
        "zjqlv1q0x2",
        "ip0ovw6baa",
        "7qkcb4ten8",
        "ga6ou1rna9",
        "rn0tbw5ibw",
        "yskwayh2a7",
        "2f6dauhbh3",
        "00oi3eszof",
        "59airwqq2f",
        "fvoqdb9aos",
        "x3eqha4ak5",
        "w7213ekoai",
        "v6pgpppvns",
        "iw03lggz5k",
        "ajlhquzk1x",
        "ez0fxx2nk4",
        "1gtusg605e",
        "5fhbxnzcs3",
        "1n6b0jmife",
        "yd3dfx81yt",
        "pfwadqtfbx",
        "wbpbfksq8m",
        "0txq9kslkd",
        "71o3dzo4vg",
        "i40of4zgbb",
        "ta7vdzcre3",
        "t3e4had0k2",
        "6vju23ec1n",
        "ezar2s1xto",
        "mleasglelq",
        "xlsdqwzsaj",
        "k4ax97a69w",
        "tsff0rbjih",
        "ukvi7df5p0",
        "tpjb14yfch",
        "jel8nmb9o5",
        "4g00awsv54",
        "a3tt2oexus",
        "ci2s0raubc",
        "nsl4ryf90p",
        "7ouih3sl43",
        "g5f1llhozy",
        "fewisaav0z",
        "hjvce61cs7",
        "pxnl2by0qn",
        "l5w89ffcty",
    ]
    # The prefixes have been given to the user in the order above. That means if
    # we choose the prefix to use in too simple a way from the integer user ID
    # then the data provider could make a guess at that integer, so use the
    # prefixes in a random order. These are the integers 0 to 99 in a random
    # order not known to the data provider, generated with
    # sorted(range(0,100),key=lambda X: random.random())
    order = [
        74,
        69,
        26,
        28,
        71,
        20,
        50,
        98,
        10,
        55,
        41,
        81,
        94,
        2,
        85,
        84,
        22,
        60,
        93,
        48,
        27,
        12,
        7,
        3,
        6,
        45,
        56,
        25,
        21,
        53,
        14,
        73,
        19,
        65,
        18,
        83,
        15,
        86,
        36,
        62,
        58,
        16,
        9,
        13,
        96,
        35,
        0,
        66,
        44,
        24,
        1,
        89,
        46,
        78,
        49,
        57,
        39,
        11,
        54,
        4,
        82,
        80,
        29,
        42,
        8,
        90,
        43,
        64,
        61,
        5,
        40,
        97,
        70,
        63,
        30,
        32,
        88,
        68,
        33,
        75,
        37,
        31,
        47,
        17,
        51,
        99,
        59,
        76,
        95,
        34,
        91,
        87,
        52,
        77,
        67,
        23,
        79,
        92,
        38,
        72,
    ]
    prefix = prefixes[order[(int(ads_user_id) % len(prefixes))]]

    return prefix + user_id


def retrieve_by_wps(req, outfile, ntries, logger):
    """Execute a CAMS solar radiation data retrieval through the WPS API."""
    import jinja2

    # Construct the XML to pass
    xml = jinja2.Template(template_xml()).render(req)
    logger.debug("request=" + repr(req))
    logger.debug("xml=" + xml)
    xml = xml.replace("\n", "")

    # Execute WPS requests in a retry-loop, cycling through available
    # servers. Nowadays the only supported server is the load-balancing
    # server: api.soda-solardata.com.
    servers = ["api.soda-solardata.com"]
    # servers = ['api.soda-solardata.com', 'www.soda-is.com',
    #           'pro.soda-is.com']
    # servers = ['vhost5.soda-is.com']
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
    from owslib.wps import WebProcessingService

    # Execute WPS. This can throw an immediate exception if the service is
    # down
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

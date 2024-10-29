import logging
import os
import time
import zipfile

from cds_common import date_tools, hcube_tools, tree_tools
from cds_common.cams.regional_fc_api import regional_fc_api
from cds_common.url2.downloader import Downloader

from cads_adaptors.exceptions import InvalidRequest
from .assert_valid_grib import assert_valid_grib
from .cacher import Cacher
from .convert_grib import convert_grib
from .create_file import create_file, temp_file
from .formats import Formats
from .grib2request import grib2request_init
from .nc_request_groups import nc_request_groups
from .preprocess_requests import preprocess_requests
from .process_grib_files import process_grib_files
from .which_fields_in_file import which_fields_in_file
from .subrequest_main import subrequest_main


# Used to temporarily disable access to archived data in an emergency, e.g.
# when too many archived requests are blocking access to latest data
ARCHIVED_OFF = False


class NoDataException(Exception):
    pass


def cams_regional_fc(context, config, requests):

    # Using the Meteo France test (aka "integration") server?
    integration_server = config.get("regional_fc", {}).get("integration_server", False)

    # Get an object which will give us information/functionality associated
    # with the Meteo France regional forecast API
    regapi = regional_fc_api(
        integration_server=integration_server,
        logger=context
    )

    # Pre-process requests
    requests, info = preprocess_requests(context, requests, regapi)
    info["config"] = config

    # If converting to NetCDF then different groups of grib files may need to be
    # converted separately. Split and group the requests into groups that can be
    # converted together.
    if "convert" in info["stages"]:
        grps = nc_request_groups(context, requests, info)
        req_groups = [{"group_id": k, "requests": v} for k, v in grps.items()]
    else:
        req_groups = [{"group_id": None, "requests": requests}]

    # Output a zip file if creating >1 NetCDF file or if requested
    if len(req_groups) > 1 or info["format"] in (
        Formats.netcdf_zip,
        Formats.netcdf_cdm,
    ):
        info["stages"].append("zip")

    # Initialisation for function that can understand GRIB file contents
    grib2request_init(config["regional_fc"]["definitions"])

    # Get locally stored fields
    get_local(req_groups, integration_server, config, context)

    # Divide non-local fields betwen latest and archived
    set_backend(req_groups, regapi, context)

    # Retrieve non-local latest (fast-access) fields
    get_latest(req_groups, config, context)

    # Retrieve non-local archived (slow-access) fields
    get_archived(req_groups, config, context)

    # Remove groups that had no matching data
    req_groups = [x for x in req_groups if "retrieved_files" in x]
    if not req_groups:
        raise NoDataException("No data found for this request", "")

    for req_group in req_groups:
        for file_index in range(len(req_group["retrieved_files"])):
            if not isinstance(req_group["retrieved_files"][file_index], str):
                req_group["retrieved_files"][file_index] = req_group["retrieved_files"][
                    file_index
                ].path

    # Process and merge grib files
    process_grib_files(req_groups, info, context)

    # Convert to netCDF?
    if "convert" in info["stages"]:
        convert_grib(req_groups, info, config["regional_fc"]["definitions"],
                     context)

    # Zip output files?
    if "zip" in info["stages"]:
        zip_files(req_groups, info, context)

    try:
        return info["result_file"]
    except KeyError:
        raise Exception("Bug: result_file not set") from None


def set_backend(req_groups, regapi, context):
    """Divide requests between "latest" and "archived" and set their "_backend"
    attribute accordingly."""
    for req_group in req_groups:
        online, offline = split_latest_from_archived(
            req_group["uncached_requests"], regapi, context
        )
        for r in online:
            r["_backend"] = ["latest"]
        for r in offline:
            r["_backend"] = ["archived"]
        req_group["uncached_latest_requests"] = online
        req_group["uncached_archived_requests"] = offline


def split_latest_from_archived(requests, regapi, context):
    """Split requests into "latest" and "archived" groups."""
    if requests:
        # Get the catalogue that lists all fields that are currently in the
        # fast "synopsis" part of the backend
        try:
            online_cat = regapi.get_catalogue("latest", retry={"timeout": 120})
        except Exception:
            # We could make a basic guess at which fields were in the latest
            # catalogue based on date, such as assuming all fields from the
            # last N days are, but for now we'll just consider this terminal
            raise

        # Split latest from archived fields
        lcat = tree_tools.to_list(online_cat)
        latest, archived, _ = hcube_tools.hcubes_intdiff2(requests, lcat)

        context.debug("Latest fields: " + repr(latest))
        context.debug("Archived fields: " + repr(archived))

    else:
        latest = []
        archived = []

    return (latest, archived)


def get_local(req_groups, integration_server, config, context):
    """Retrieve only the fields which are stored locally (in the cache or on
    the datastore) and identify the remaining non-local fields.
    """
    # Cacher has knowledge of cache locations
    with Cacher(integration_server, logger=context) as cacher:
        for req_group in req_groups:
            _get_local(req_group, cacher, config, context)


def _get_local(req_group, cacher, config, context):
    """Retrieve only the fields which are already stored locally (in the cache
    or on the datastore) and identify non-local fields.
    """
    # Date lists must not be compressed
    reqs = [r.copy() for r in req_group["requests"]]
    for r in reqs:
        r["date"] = date_tools.expand_dates_list(r["date"])

    # Download local fields
    urls = (
        {"url": cacher.cache_file_url(field), "req": field}
        for field in hcube_tools.unfactorise(reqs)
    )
    downloader = Downloader(
        max_rate=50,
        max_simultaneous=15,
        combine_method="cat",
        target_suffix=".grib",
        response_checker=assert_valid_grib,
        response_checker_threadsafe=False,
        combine_in_order=False,
        request_timeout=[60, 300],
        max_attempts={404: 1, "default": 3},
        nonfatal_codes=[404, "exception"],
        retry_wait=5,
        allow_no_data=True,
        logger=context,
        min_log_level=logging.INFO,
    )
    grib_file = temp_file(config, suffix='.grib')
    if not config.get('regional_fc', {}).get('no_cache_downloads'):
        downloader.execute(urls, target=grib_file)

    # Identify uncached fields - the ones not present in the file
    cached, uncached = which_fields_in_file(reqs, grib_file, config, context)
    req_group["uncached_requests"] = uncached
    context.info("Retrieved " + str(hcube_tools.count_fields(cached)) + " local fields")
    context.info(
        "Number of remaining uncached fields = "
        + str(hcube_tools.count_fields(uncached))
    )

    # File will be empty if no fields found
    if os.path.getsize(grib_file) > 0:
        req_group["retrieved_files"] = req_group.get("retrieved_files", []) + [
            grib_file
        ]


def get_latest(req_groups, config, context):
    """Retrieve uncached latest fields."""
    for req_group in req_groups:
        if not req_group["uncached_latest_requests"]:
            continue

        # Uncached fields are retrieved in sub-requests which are limited to
        # one-at-a-time by QOS, thus one long request could hog the resource
        # for a long period. Prevent this by retrieving in chunks.
        for reqs in hcube_tools.hcubes_chunk(
            req_group["uncached_latest_requests"], 5000
        ):
            grib_file = get_uncached(reqs, req_group, config, context)

            # Fields may have expired from the latest backend by the time the
            # request was made. Reassign any missing fields to the archive
            # backend.
            reassign_missing_to_archive(reqs, grib_file, req_group, config, context)


def get_archived(req_groups, config, context):
    """Retrieve uncached slow-access archived fields."""

    for req_group in req_groups:
        if not req_group["uncached_archived_requests"]:
            continue

        if ARCHIVED_OFF:
            raise Exception(
                "Access to archived data is temporarily "
                + "suspended. Only the latest few days are available"
            )

        # Archived fields are retrieved in sub-requests which are limited to
        # one-at-a-time by QOS, thus one long request could hog the resource
        # for a long period. Prevent this by retrieving in 900 field chunks
        # which, at 1 field/second, limits each sub-request to 15 minutes.
        for reqs in hcube_tools.hcubes_chunk(
            req_group["uncached_archived_requests"], 900
        ):
            get_uncached(reqs, req_group, config, context)


MAX_SUBREQUEST_RESULT_DOWNLOAD_RETRIES = 3


def get_uncached(requests, req_group, config, context):
    """Retrieve chunk of uncached fields"""

    backend = requests[0]["_backend"][0]
    assert backend in ["latest", "archived"]

    # Retrieve the fields in a sub-request or directly? The latter is only used for
    # testing. Generally you want a sub-request.
    cfg = config.get("regional_fc", {})
    if str(cfg.get('no_subrequests')) != '1':
        path = retrieve_subrequest(backend, requests, req_group, config, context)

    else:
        # No sub-request - call code directly. For testing.
        f = subrequest_main(backend,
                            {"requests": requests, "parent_config": config},
                            config,
                            context)
        f.close()
        path = f.name

    # The target can legitimately be empty as the code currently accepts 404s
    size = os.path.getsize(path)
    if size > 0:
        req_group["retrieved_files"] = req_group.get("retrieved_files", []) + [path]
    else:
        context.add_stdout("Sub-request target file is empty")

    return path


def retrieve_subrequest(backend, requests, req_group, config, context):
    from cdsapi import Client

    """Retrieve chunk of uncached fields in a sub-request"""

    # Is this backend expecting issues due to maintenance?
    cfg = config.get("regional_fc", {})
    maintenance_msg = cfg.get('backend_maintenance', {}).get(backend)

    # Construct a target file name
    target = temp_file(config, suffix='.grib')

    # Get a client
    context.info("Executing sub-request to retrieve uncached fields: " + repr(requests))
    t0 = time.time()
    client = Client(
        url="https://" + os.environ["ADS_SERVER_NAME"] + os.environ["API_ROOT_PATH"],
        key=os.environ["HIGH_PRIORITY_CADS_API_KEY"],
        wait_until_complete=False)

    # Launch the sub-request
    response = None
    sub_request_uid = None
    dataset = f"cams-europe-air-quality-forecasts-{backend}"
    try:
        response = client.retrieve(
            dataset,
            {"requests": requests, "parent_config": config},
        )
    except Exception as e:
        sub_request_uid = 'none' if response is None else response.request_uid
        context.add_stderr(
            "Sub-request " +
            ("" if response is None else f"({response.request_uid}) ") +
            f"failed: {e!r}"
        )
        if maintenance_msg:
            raise InvalidRequest(maintenance_msg) from None
        else:
            raise RuntimeError(f"Failed to retrieve data from {backend} remote server. "
                               "Please try again later.") from None
    else:
        sub_request_uid = response.request_uid
        message = f"Sub-request {sub_request_uid} has been launched (via the CDSAPI)."
        context.add_stdout(message)

    # Download the result
    exc = None
    for i_retry in range(MAX_SUBREQUEST_RESULT_DOWNLOAD_RETRIES):
        try:
            response.download(target)
            break
        except Exception as e:
            exc = e
            context.add_stdout(
                f"Attempt {i_retry+1} to download the result of sub-request "
                f"{sub_request_uid} failed: {e!r}"
            )
    else:
        context.add_stderr(f"Failed to download sub-request result: {exc!r}")
        raise RuntimeError(message) from None

    size = os.path.getsize(target)
    context.info(f"... sub-request downloaded {size} bytes in " + str(time.time() - t0)
                 + "s")

    return target


def reassign_missing_to_archive(reqs, grib_file, req_group, config, context):
    """Re-assign fields which are in reqs but not in grib_file to the archived backend.
    """
    # Which are in the file and which aren't?
    present, missing = which_fields_in_file(reqs, grib_file, config, context)
    if missing:
        context.info(
            "Resorting to archived backend for missing fields: " + repr(missing)
        )
    for r in missing:
        r["_backend"] = ["archived"]
    req_group["uncached_archived_requests"].extend(missing)


def zip_files(req_groups, info, context):
    assert info["format"] != Formats.grib

    path = create_file("zip", ".zip", info)
    with zipfile.ZipFile(path, "w") as zf:
        for req_group in req_groups:
            zf.write(
                req_group["nc_file"], arcname="_".join(req_group["group_id"]) + ".nc"
            )

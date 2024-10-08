import time
from copy import deepcopy
from itertools import product
from threading import Semaphore, Timer

from cds_common import date_tools, hcube_tools
from cds_common.url2.downloader import Downloader, RequestFailed
from cds_common.url2.requests_to_urls import requests_to_urls

from .assert_valid_grib import assert_valid_grib
from .cacher import Cacher
from .grib2request import grib2request_init


def api_retrieve(context, requests, regapi, dataset_dir, no_cache_put=False, **kwargs):
    """Download the fields from the Meteo France API."""
    # Keyword argument options to Downloader that depend on the backend
    # (archived/latest)
    backend_specific = {
        "archived": {
            # Statistics show that if an archived request fails once, it's very
            # unlikely to succeed on a subsequent attempt, and since many of the
            # requests are time-outs, retrying over and over in vain wastes a
            # lot of time, so only retry if we have not already spent a lot of
            # time making attempts.
            # Update: We have been seeing some failures due to "connection reset
            # by peer" type exceptions though which can succeed on retry, so we
            # allow retries for those.
            "max_attempts": {
                "default": lambda req: (
                    req["total_duration_secs"] < 10 and req["attempt"] < 10
                ),
                "exception": lambda req: (
                    "ConnectionResetError" in str(req["status"]) and req["attempt"] < 10
                ),
                404: 1,
                400: 1,
            },
            "request_timeout": [60, 600],  # The read timeout is set to 600s
            # to match same timeout on MF side
        },
        "latest": {
            # Latest backend failures (non-404) are rare so hard to know the
            # best thing to do - assume retries are a good idea.
            "max_attempts": {
                "default": 50,
                404: 1,
                400: 10,
            },  # 400's can get raised at 2am
            "request_timeout": [60, 300],
        },
    }

    backend = requests[0]["_backend"][0]

    # Process requests according to the abilities of the Meteo France API
    requests = make_api_hypercubes(requests, regapi, context)

    # Initialisation for function that can understand GRIB file contents
    grib2request_init(dataset_dir)

    # By default Downloader would use requests.get to make requests. Provide
    # an alternative function that takes care of the Meteo France API
    # authorisation tokens.
    getter = regapi.get_fields_url

    # Objects to limit the rate and maximum number of simultaneous requests
    rate_limiter = CamsRegionalFcApiRateLimiter(regapi)
    number_limiter = CamsRegionalFcApiNumberLimiter(regapi)

    # Translate requests into URLs as dicts with a 'url' and a 'req' key
    urlreqs = list(requests_to_urls(requests, regapi.url_patterns))

    # Create an object that will handle the caching
    with Cacher(context, no_put=no_cache_put) as cacher:
        # Create an object that will allow URL downloading in parallel
        context.create_result_file(".json")
        downloader = Downloader(
            context,
            getter=getter,
            max_rate=rate_limiter,
            max_simultaneous=number_limiter,
            combine_method="cat",
            target_suffix=".grib",
            response_checker=assert_valid_grib,
            response_checker_threadsafe=False,
            combine_in_order=False,
            write_to_temp=False,
            nonfatal_codes=[404],
            allow_no_data=True,
            cacher=cacher,
            **backend_specific[backend],
            **kwargs,
        )

        t0 = time.time()

        try:
            # Returns None if no data is found
            file = downloader.execute(urlreqs)
        except RequestFailed:
            # req = {x["url"]: x["req"] for x in urlreqs}[e.url]
            # raise Exception(
            #     'Failed to retrieve data for ' + str(req) +
            #     f' (code {e.status_code}). Please try again later') \
            #     from None
            return None

        # Ensure the next call to this routine does not happen less than
        # 1/max_rate seconds after the last API request
        rate_limiter.block({"req": {"_backend": backend}})

        nfields = hcube_tools.count_fields(requests)
        context.info(
            f"Attempted download of {nfields} fields took "
            + f"{time.time() - t0} seconds"
        )

    context.info("download finished")  #!

    return file


def make_api_hypercubes(requests, regapi, context):
    """Process request hypercubes into the dicts required for url2 input.
    For requests for latest data, for which each field must be fetched with a
    separate URL request, this is a null op. Archived data URL requests can
    fetch multiple fields at a time however. The values of these multi-field
    hypercubes must be concatenated into single comma-separated strings to be
    retrieved as a single URL.
    """

    # A single archived data URL request cannot...
    #     * span a grid-change date
    #     * contain multiple models, types or variables
    #     * mix surface and upper levels
    #     * represent >120 fields
    # ... so chop up accordingly.
    def levgroups(levels):
        return [
            g
            for g in [
                [level for level in levels if level == "0"],
                [level for level in levels if level != "0"],
            ]
            if g
        ]

    output = []
    for request in requests:
        # Null op for latest data as stated above
        if request["_backend"][0] == "latest":
            output.append(request)
            continue

        # Date lists must not be compressed
        request = request.copy()
        request["date"] = date_tools.expand_dates_list(request["date"])

        # Chop up by model grid
        for r1 in sum(regapi.split_request_by_grid(request).values(), []):
            # Chop up by model/type/variable/level-type
            for model, xtype, variable, levels in product(
                r1["model"], r1["type"], r1["variable"], levgroups(r1["level"])
            ):
                r2 = deepcopy(r1)
                r2["model"] = [model]
                r2["type"] = [xtype]
                r2["variable"] = [variable]
                r2["level"] = levels

                # Chop up into chunks of no more than 50 fields (judged to be
                # the optimal request size for speed)
                for r3 in hcube_tools.hcubes_split(r2, 50):
                    # Concatenate all value-lists into single strings
                    output.append(
                        {k: ",".join([str(x) for x in v]) for k, v in r3.items()}
                    )

    return output


class CamsRegionalFcApiLimiter:
    """Abstract base class for controlling the URL requests.
    It controls rate and max number of simultaneous requests to the regional forecast API.
    """

    def __init__(self, regapi):
        # Maximum URL request rates for online and offline data
        self._max_rate = regapi.max_rate

        # Maximum number of simultaneous requests for online and offline data
        self._max_simultaneous = regapi.max_simultaneous

        # The time duration that data stays online. Nominally this is 5 days
        # but we subtract a small amount of time for safety
        #### self._online_duration = timedelta(days=5) - timedelta(minutes=10)

        self._rate_semaphores = {k: Semaphore() for k in self._max_rate.keys()}
        self._number_semaphores = {
            k: Semaphore(v) for k, v in self._max_simultaneous.items()
        }

    # def _type(self, req):
    #     """Return whether a request field is expected to be online or offline
    #        depending on the validity time."""
    #     r = req['req']
    #     vtime = datetime.strptime(r['date'] + ' ' + r['time'],
    #                               '%Y-%m-%d %H%M') + \
    #         timedelta(hours=int(r['step']))
    #     if (datetime.now() - vtime) < self._online_duration:
    #         return 'latest'
    #     else:
    #         return 'archived'


class CamsRegionalFcApiRateLimiter(CamsRegionalFcApiLimiter):
    """Class to limit the URL request rate to the regional forecast API."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def block(self, req):
        """Block as required to ensure there is at least 1/max_rate seconds
        between calls for the same type of data, where max_rate depends on
        the type in question.
        """
        type = req["req"]["_backend"]
        self._rate_semaphores[type].acquire()
        Timer(1 / self._max_rate[type], self._rate_semaphores[type].release).start()


class CamsRegionalFcApiNumberLimiter(CamsRegionalFcApiLimiter):
    """Class to limit the number of simultaneously executing URL requests to the regional forecast API."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def block(self, req):
        """Block as required to ensure there are no more than N ongoing
        requests for the same type of data, where N depends on the type in
        question. Return a function that will unblock when called.
        """
        type = req["req"]["_backend"]
        self._number_semaphores[type].acquire()
        return lambda X: self._number_semaphores[type].release()

    @property
    def max_simultaneous(self):
        """Return the total number of simultaneous URL requests allowed, of
        any type.
        """
        return sum(self._max_rate.values())

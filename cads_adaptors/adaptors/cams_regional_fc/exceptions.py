# COPIED OVER FROM (entire file): https://git.ecmwf.int/projects/CDS/repos/cdsinf/browse/cdsclient/exceptions.py
"""This contains most CDS client library exception definitions"""

class CDSException(Exception):
    """All CDS exceptions inherit from this.

    Several fields are available:

    If 'permanent' is true then no attempt should be made to rerun a request
    automatically.

    If 'locally_permanent' is true then it may be rerun, but not from the same
    location (this will happen if you attempt a download which is larger than
    your disk - if the disk is large enough but has too little space you'll
    get permanent=False and locally_permanent=False).

    'msg' is meant for logs and developers, 'usermsg' may be displayed to a
    user.

    'uri' is a URI (currently under http://copernicus-climate.eu/exc/) which
    uniquely identifies the cause of the error. This URI isn't meant to work
    in current versions of CDS - it's simply a unique identifier for the error.
    If you need to make a support request please include this URI.
    """
    publish_traceback = False

    def __init__(self, permanent, locally_permanent, msg, usermsg, uri):
        super().__init__(msg)

        self.permanent = bool(permanent)
        self.locally_permanent = bool(locally_permanent)
        self.usermsg = usermsg
        self.uri = uri

class OutOfSpaceException(CDSException):
    """This is thrown when there isn't enough disk space for data.
    locally_permanent=true if the entire disk is too small, otherwise false.
    """
    def __init__(self, perm, locally_perm, msg):
        super().__init__(
            perm,
            locally_perm,
            msg,
            "your is too full to process your request",
            "http://copernicus-climate.eu/exc/client/disk-full")

class DataProviderError(CDSException):
    """This is thrown when the data provider doesn't like our request. This
    doesn't include failures of the data provider - this is meant for problems
    such as invalid requests or missing data. For that reason, permanent=True"""
    def __init__(self, msg, usermsg, uri):
        super().__init__(
            True,
            True,
            msg,
            usermsg,
            uri)

    @staticmethod
    def from_400_response(response):
        """This creates a DataProviderError from a 4xx HTTP status code in
        a 'requests' library response object."""
        cls = (DataProviderFileNotFoundError if response.status_code == 404
               else DataProviderError)
        return cls(
            response.reason,
            "an internal error occurred processing your request",
            "http://copernicus-climate.eu/exc/data-supplier-4xx")

    @classmethod
    def from_unexpected_code(cls, response):
        """This creates a DataProviderError from any HTTP status code in
        a 'requests' library response object."""
        return cls(
            response.reason,
            "an internal error occurred processing your request",
            "http://copernicus-climate.eu/exc/data-supplier-unexpect-http-status")

class DataProviderFileNotFoundError(DataProviderError):
    """This is a kind of data provider error specific to the case of the target
    data not existing - HTTP 404 and equivalents."""
    pass

class DataProviderFailed(CDSException):
    """This is thrown when the data provider failed in a way which is not
    our fault or the request's fault (eg, server down)."""
    def __init__(self, msg, usermsg, uri):
        super().__init__(
            False,
            False,
            msg,
            usermsg,
            uri)

    @classmethod
    def from_500_response(cls, response):
        """This creates a DataProviderFailed from a 5xx HTTP status code in
        a 'requests' library response object."""
        return cls(
            response.reason,
            "data supplier's server is currently unavailable",
            "http://copernicus-climate.eu/exc/data-supplier-5xx")

class CDSComponentError(CDSException):
    """This defines the additional fields we get when an error has been passed
    to us using the format defined in CDS's API.
    """
    def __init__(self, code, context):
        """SUBCLASS USE ONLY! Subclasses should also inherit from another
        CDSException subclass and call its constructor after this one
        to initialize fields common to all errors"""
        self._code = code
        self._context = context

    @staticmethod
    def _is_permanent(code):
        """Returns True if a result code is 'permanent'. If so, there's no point
        trying this request again."""
        # Which codes are permanent depends on the decision of code in
        # brokerprotocol.py (where the error has come from an adaptor) or in
        # the broker more generally (where it never got to/from an adaptor).
        if code < 500:
            return code != 400 and code != 404 # Defined in brokerprotocol.py
        else:
            return code == 500

    @staticmethod
    def _is_locally_permanent(code):
        """Returns True if a result code is 'permanent' for the particular broker it
        was submitted to. If so, there's no point trying this request again with
        the same broker."""
        if code < 500:
            return code != 404
        else:
            return code == 501 or code == 500

    # This maps from result flags to a V1 equivalent error code.
    # Use as _flags_to_code[permanent][who], where permanent and who can be
    # None.
    _flags_to_code = {
        # Permanent errors
        True:  {
            "client": 400,
            "server": 500,
            None:     500,
        },

        # Temporary errors
        False: {
            "client": 404, # Not HTTPly correct, but what else?
            "server": 503,
            None:     500
        },

        None: {
            "client": 200, # Shouldn't get here
            "server": 200, # Shouldn't get here, either
            None:     200
        },
    }

    @staticmethod
    def _v2_error_flags_to_code(permanent, who):
        """This reconstructs a V1 API error code from V2 API error data."""
        return CDSComponentError._flags_to_code[permanent][who]

    @property
    def code(self):
        """Returns a broker notional HTTP status code, as defined in the
        V1 API. The code is not present in the V2 API so it's reconstructed.

        Use of this code is discouraged as it may be removed.
        """
        if self._code is None:
            return CDSComponentError._v2_error_flags_to_code(
                self.permanent, self.who)
        else:
            return self._code

    @property
    def context(self):
        """Returns the error context, usually a stacktrace in text form."""
        return self._context

    @classmethod
    def from_apiv1_error(cls, error_data):
        """Givern an API v1 error, return either a BrokerError or BrokerFailed
        depending on whether the error appears to be the client's fault
        (BrokerError) or the broker's (BrokerFailed).

        Call this using a subclass if you want to specify which subclass to use
        instead of auto-detecting it.
        """
        if cls == CDSComponentError:
            if error_data["code"] < 500:
                cls = BrokerError
            else:
                cls = BrokerFailed

        return cls(
            CDSComponentError._is_permanent(error_data["code"]),
            CDSComponentError._is_locally_permanent(error_data["code"]),
            error_data["data"]["reason"],
            error_data["message"],
            error_data["data"]["url"],
            error_data["code"],
            {"traceback": error_data["data"].get("context")}
                if "context" in error_data["data"]
                else {})

    @classmethod
    def from_apiv2_error(cls, error_data):
        """Givern an error from the V2 API, return either a BrokerError or
        BrokerFailed depending on whether the error appears to be the
        client's fault (BrokerError) or the broker's (BrokerFailed).
        """
        if cls == CDSComponentError:
            if error_data.get("who") == "client":
                cls = BrokerError
            else:
                cls = BrokerFailed

        perm = error_data.get("permanent", False)
        return cls(
            perm,
            perm, # V2 API has no permanent/locally-permanent distinction
            error_data.get("reason"),
            error_data.get("message"),
            error_data.get("url"),
            CDSComponentError._v2_error_flags_to_code(
                error_data.get("permanent"),
                error_data.get("who")),
            error_data.get("context"))

    @classmethod
    def from_api_error(cls, error_data, version):
        """Turns an API error in to an exception given the response and API
        version as parameters. Returns None if it doesn't look like an
        exception."""
        if error_data is not None:
            if (version == 1
                and "error" in error_data
                and "data" in error_data["error"]):
                return cls.from_apiv1_error(error_data["error"])
            elif version == 2 and "permanent" in error_data:
                return cls.from_apiv2_error(error_data)

        return None

    @classmethod
    def from_msgs(cls, code, msg, usermsg, url):
        """Convenience method to construct an error from a code and
        messages."""
        return cls(
            CDSComponentError._is_permanent(code),
            CDSComponentError._is_locally_permanent(code),
            msg,
            usermsg,
            url,
            code,
            {})

class BrokerError(CDSComponentError, DataProviderError):
    """This is thrown/returned when a call to the CDS API (or a task executed
    via the API) was erronous in some way. Do not repeat the request."""
    def __init__(self, perm, localperm, msg, usermsg, uri, code, context):
        CDSComponentError.__init__(self, code, context)
        DataProviderError.__init__(self, msg, usermsg, uri)

        self.permanent = perm
        self.locally_permanent = localperm

class BrokerFailed(CDSComponentError, DataProviderFailed):
    """This is thrown/returned when a call to the CDS API failed with an error
    which is the fault of the API processing or task dispatch."""
    def __init__(self, perm, localperm, msg, usermsg, uri, code, context):
        CDSComponentError.__init__(self, code, context)
        DataProviderFailed.__init__(self, msg, usermsg, uri)

        self.permanent = perm
        self.locally_permanent = localperm

class BrokerQoSDenied(DataProviderError):
    """This is thrown/returned when a call to the CDS API failed because it's
    too large for CDS to accept."""
    def __init__(self, msg):
        super().__init__(msg, msg, "http://copernicus-climate.eu/exc/qos-denied")
        self.code = 403

class InternalError(CDSException):
    """Internal errors have no useful user-visible message. They represent
    typically bugs in our system or client library or its configuration."""
    publish_traceback = True

    def __init__(self, msg, uri):
        super().__init__(
            True,
            True,
            msg,
            "an internal error occurred processing your request",
            uri)
        
        
        


# COPIED OVER FROM (partially): https://git.ecmwf.int/projects/CDS/repos/cdsinf/browse/cdsinf/exceptions.py
"""This contains most CDS infrastructure library exception definitions

Note that subclasses of CDSException can be turned in to responses to
requestors (usually the broker), so they're treated differently by the CDS
runner.
"""

class BadRequestException(CDSException):
    """This is thrown when an incoming request is bad. It isn't used when an
    outgoing request is bad."""
    def __init__(self, msg, uri):

        super().__init__(
            True,
            True,
            msg,
            "the request you have submitted is not valid",
            uri)

class NoDataException(CDSException):
    """This is thrown when an incoming request requests no data and the upstream
    provider gives us no way to return an empty dataset."""
    def __init__(self, msg, uri):
        super().__init__(
            True,
            True,
            msg,
            "no data is available within your requested subset",
            "http://copernicus-climate.eu/exc/empty-dataset")

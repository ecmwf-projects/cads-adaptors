import abc
import contextlib
from typing import Any, BinaryIO

import cads_adaptors.tools.logger

Request = dict[str, Any]


class Context:
    def __init__(
        self,
        job_id: str = "job_id",
        logger: Any | None = None,
        write_type: str = "stdout",
    ):
        self.job_id = job_id
        if not logger:
            self.logger = cads_adaptors.tools.logger.logger
        else:
            self.logger = logger
        self.write_type = write_type
        self.messages_buffer = ""

    def write(self, message: str) -> None:
        """Use the logger as a file-like object. Needed by tqdm progress bar."""
        self.messages_buffer += message

    def flush(self) -> None:
        """Write to the logger the content of the buffer."""
        if self.messages_buffer:
            if self.write_type == "stdout":
                self.add_stdout(self.messages_buffer)
            elif self.write_type == "stderr":
                self.add_stderr(self.messages_buffer)
            self.messages_buffer = ""

    def add_user_visible_log(self, message: str, session: Any | None = None) -> None:
        pass

    def add_user_visible_error(self, message: str, session: Any | None = None) -> None:
        pass

    def add_stdout(
        self, message: str, log_type: str = "info", session: Any | None = None, **kwargs
    ) -> None:
        self.logger.info(message)

    def add_stderr(
        self,
        message: str,
        log_type: str = "exception",
        session: Any | None = None,
        **kwargs,
    ) -> None:
        self.logger.exception(message)

    @property
    def session_maker(self) -> Any:
        return contextlib.nullcontext

    def info(self, *args, **kwargs):
        self.add_stdout(*args, log_type="info", **kwargs)

    def debug(self, *args, **kwargs):
        self.add_stdout(*args, log_type="debug", **kwargs)

    def warn(self, *args, **kwargs):
        self.add_stdout(*args, log_type="warn", **kwargs)

    def warning(self, *args, **kwargs):
        self.add_stdout(*args, log_type="warning", **kwargs)

    def error(self, *args, **kwargs):
        self.add_stderr(*args, log_type="error", **kwargs)

    def exception(self, *args, **kwargs):
        self.add_stderr(*args, log_type="exception", **kwargs)


import inspect
import os
import mimetypes

mimetypes.add_type("application/x-grib", ".grib")
mimetypes.add_type("application/x-bufr", ".bufr")
mimetypes.add_type("application/x-odb", ".odb")
mimetypes.add_type("application/binary", ".bin")
mimetypes.add_type("application/json", ".json")
mimetypes.add_type("image/jpeg", ".jpg")
mimetypes.add_type("image/gif", ".gif")
mimetypes.add_type("image/png", ".png")
mimetypes.add_type("application/pdf", ".pdf")

MIME = {}  # Cache


def guess_extension(mime_type):
    """A workaround for http://bugs.python.org/issue1043134

    """
    if mime_type in ("text/plain", u"text/plain"):
        return ".txt"

    if mime_type in ("application/postscript", u"application/postscript"):
        return ".ps"

    return mimetypes.guess_extension(mime_type)


class ResultFile(dict):

    """
    ResultFile is-a dict so it can be serialised by the json module
    """

    def __init__(self, cdsinf, extension, size=0):
        self.cdsinf = cdsinf
        self.extension = extension

        name = "unknown"
        for s in inspect.stack():
            if s[3] == 'execute':
                name = os.path.splitext(os.path.basename(s[1]))[0]
                break

        with cdsinf.get_output_space(name, "%s.data" % (name,), size) as space:
            basename = "%s%s" % (cdsinf._request_context["request_id"], self.extension,)
            self.path = os.path.join(space.location, basename)

            mime = MIME.get(extension)
            if mime is None:
                mime = (mimetypes.guess_type(self.path)[0] or
                        "application/octet-stream")
                MIME[extension] = mime

            # Update dict part

            self.update({
                "resultType": "url",
                "location": space.url + "/" + basename,
                "contentType": mime,
            })

    def ensure_size(self):
        if "contentLength" not in self:
            st = os.stat(self.path)
            self["contentLength"] = st.st_size

    def __getitem__(self, key):
        self.ensure_size()
        return dict.__getitem__(self, key)

    def items(self):
        self.ensure_size()
        return dict.items(self)

    def keys(self):
        self.ensure_size()
        return dict.keys(self)

    def values(self):
        self.ensure_size()
        return dict.values(self)

    def iteritems(self):
        self.ensure_size()
        return dict.iteritems(self)

    def iterkeys(self):
        self.ensure_size()
        return dict.iterkeys(self)

    def itervalues(self):
        self.ensure_size()
        return dict.itervalues(self)


class AbstractAdaptor(abc.ABC):
    resources: dict[str, int] = {}

    def __init__(
        self,
        form: list[dict[str, Any]] | dict[str, Any] | None,
        context: Context | None = None,
        **config: Any,
    ) -> None:
        self.form = form
        self.config = config
        if context is None:
            self.context = Context()
        else:
            self.context = context

    @abc.abstractmethod
    def normalise_request(self, request: Request) -> Request:
        """Apply any normalisation to the request before validation.

        Parameters
        ----------
        request : Request
            Incoming request.

        Returns
        -------
        Request
            Normalised request.

        Raises
        ------
        cads_adaptors.exceptions.InvalidRequest
            If the request is invalid.
        """
        pass

    @abc.abstractmethod
    def validate(self, request: Request) -> bool:
        pass

    @abc.abstractmethod
    def apply_constraints(self, request: Request) -> dict[str, Any]:
        """Apply constraints to the request.

        Parameters
        ----------
        request : Request
            Incoming request.

        Returns
        -------
        dict[str, Any]
            Further parameters' values compatible with the submitted request.

        Raises
        ------
        cads_adaptors.exceptions.ParameterError
            If a request's parameter is invalid.
        cads_adaptors.exceptions.InvalidRequest
            If the request is invalid.

        """
        pass

    @abc.abstractmethod
    def estimate_costs(self, request: Request) -> dict[str, int]:
        pass

    @abc.abstractmethod
    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        pass

    @abc.abstractmethod
    def retrieve(self, request: Request) -> Any:
        pass


class DummyAdaptor(AbstractAdaptor):
    def validate(self, request: Request) -> bool:
        return True

    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return {}

    def estimate_costs(self, request: Request) -> dict[str, int]:
        size = int(request.get("size", 0))
        time = int(request.get("time", 0.0))
        return {"size": size, "time": time}

    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        return []

    def normalise_request(self, request: Request) -> dict[str, Any]:
        return request

    def retrieve(self, request: Request) -> BinaryIO:
        import datetime
        import time

        size = int(request.get("size", 0))
        elapsed = request.get("elapsed", "0:00:00.000")
        try:
            time_elapsed = datetime.time.fromisoformat("0" + elapsed)
            time_sleep = datetime.timedelta(
                hours=time_elapsed.hour,
                minutes=time_elapsed.minute,
                seconds=time_elapsed.second,
                microseconds=time_elapsed.microsecond,
            ).total_seconds()
        except Exception:
            time_sleep = 0

        time.sleep(time_sleep)
        with open("dummy.grib", "wb") as fp:
            with open("/dev/urandom", "rb") as random:
                while size > 0:
                    length = min(size, 10240)
                    fp.write(random.read(length))
                    size -= length
        return open("dummy.grib", "rb")

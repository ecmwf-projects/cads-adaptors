import abc
import contextlib
import datetime
import itertools
import pathlib
import time
import zipfile
from typing import Any, BinaryIO

import cads_adaptors.tools.general
import cads_adaptors.tools.logger

Request = dict[str, Any]
CHUNK_SIZE = 10240


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


class AbstractAdaptor(abc.ABC):
    resources: dict[str, int] = {}

    def __init__(
        self,
        form: list[dict[str, Any]] | dict[str, Any] | None,
        context: Context | None = None,
        cache_tmp_path: pathlib.Path | None = None,
        **config: Any,
    ) -> None:
        self.form = form
        self.config = config
        self.context = Context() if context is None else context
        self.cache_tmp_path = (
            pathlib.Path() if cache_tmp_path is None else cache_tmp_path
        )

    @abc.abstractmethod
    def check_validity(self, request: Request) -> Request:
        """Check the validity of the request.

        Parameters
        ----------
        request : Request
            Incoming request.

        Returns
        -------
        Request
            Valid request.

        Raises
        ------
        cads_adaptors.exceptions.InvalidRequest
            If the request is invalid.
        """
        pass

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
    def estimate_costs(self, request: Request, **kwargs: Any) -> dict[str, int]:
        """
        Estimate the costs associated with the request.

        Parameters
        ----------
        request : Request
            Incoming request.
        **kwargs : Any
            Additional parameters, specific to the particular method's implementation.

        Returns
        -------
        dict[str, int]
            Estimated costs,
            where the key is the cost name/type (e.g. size/precise_size)
            and the value is the maximum cost for that type.
        """
        pass

    @abc.abstractmethod
    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        """
        Get licences associated with the request.

        Parameters
        ----------
        request : Request
            Incoming request.

        Returns
        -------
        list[tuple[str, int]]
            List of tuples with licence ID and version.
        """
        pass

    @abc.abstractmethod
    def retrieve(self, request: Request) -> BinaryIO:
        """
        Retrieve file associated with the request.

        Parameters
        ----------
        request : Request
            Incoming request.

        Returns
        -------
        BinaryIO
            Opened file.
        """
        pass


class DummyAdaptor(AbstractAdaptor):
    def apply_constraints(self, request: Request) -> dict[str, Any]:
        return {}

    def check_validity(self, request: Request) -> Request:
        return request

    def estimate_costs(self, request: Request, **kwargs: Any) -> dict[str, int]:
        size = int(request.get("size", 0))
        time = int(request.get("time", 0.0))
        return {"size": size, "time": time}

    def get_licences(self, request: Request) -> list[tuple[str, int]]:
        return []

    def normalise_request(self, request: Request) -> dict[str, Any]:
        size = request.get("size", 0)

        elapsed = request.get("elapsed", 0)
        if isinstance(elapsed, str):
            if len(elapsed.split(":", 1)[0]) == 1:
                elapsed = "0" + elapsed
            elapsed = datetime.time.fromisoformat(elapsed)
            elapsed = datetime.timedelta(
                hours=elapsed.hour,
                minutes=elapsed.minute,
                seconds=elapsed.second,
                microseconds=elapsed.microsecond,
            ).total_seconds()

        format = request.get("format", "grib")

        request = request | {
            "size": int(size),
            "elapsed": float(elapsed),
            "format": str(format),
        }
        return dict(sorted(request.items()))

    def cached_retrieve(self, request: Request) -> BinaryIO:
        import cacholote

        cache_kwargs = {"collection_id": self.config.get("collection_id")}
        request = self.normalise_request(request)
        with cacholote.config.set(return_cache_entry=False):
            return cacholote.cacheable(self.retrieve, **cache_kwargs)(request)

    def retrieve(self, request: Request) -> BinaryIO:
        request = self.normalise_request(request)
        size = request["size"]
        elapsed = request["elapsed"]
        format = request["format"]

        match format:
            case "grib":
                # Write and cache grib file
                dummy_file = self.cache_tmp_path / "dummy.grib"
                self.context.add_stdout(f"Sleeping {elapsed} s")
                time.sleep(elapsed)

                self.context.add_stdout(f"Writing {size} B to {dummy_file!s}")
                tic = time.perf_counter()
                with dummy_file.open("wb") as netcdf_fp:
                    with open("/dev/urandom", "rb") as random:
                        while size > 0:
                            length = min(size, CHUNK_SIZE)
                            netcdf_fp.write(random.read(length))
                            size -= length
                toc = time.perf_counter()
                self.context.add_stdout(
                    f"Elapsed time to write the file: {toc - tic} s"
                )
            case "netcdf":
                # Retrieve cached grib and convert
                dummy_file = self.cache_tmp_path / "dummy.nc"
                grib_fp = self.cached_retrieve(request | {"format": "grib"})
                with dummy_file.open("wb") as netcdf_fp:
                    while True:
                        if not (data := grib_fp.read(CHUNK_SIZE)):
                            break
                        netcdf_fp.write(data)
            case "zip":
                # Retrieve cached gribs and zip
                dummy_file = self.cache_tmp_path / "dummy.zip"
                request = {
                    k: cads_adaptors.tools.general.ensure_list(v)
                    for k, v in request.items()
                }
                requests = [
                    dict(zip(request.keys(), values))
                    for values in itertools.product(*request.values())
                ]
                grib_size = size // len(requests)
                grib_elapsed = elapsed / len(requests)
                with zipfile.ZipFile(dummy_file, "w") as zip_fp:
                    for i, request in enumerate(requests):
                        request["format"] = "grib"
                        request["size"] = grib_size + (size % len(requests)) * (not i)
                        request["elapsed"] = grib_elapsed
                        grib_fp = self.cached_retrieve(request)
                        with zip_fp.open(f"dummy_{i}.grib", "w") as zip_grib_fp:
                            while True:
                                if not (data := grib_fp.read(CHUNK_SIZE)):
                                    break
                                zip_grib_fp.write(data)
            case _:
                raise NotImplementedError(f"{format=}")
        return dummy_file.open("rb")

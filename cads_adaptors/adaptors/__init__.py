import abc
import contextlib
from typing import Any, BinaryIO

import cads_adaptors.tools.logger

Request = dict[str, Any]


class Context:
    def __init__(self, job_id: str = "job_id", logger: Any = cads_adaptors.tools.logger.logger):
        self.job_id = job_id
        self.logger = logger

    def add_user_visible_log(self, message: str, session: Any) -> None:
        pass

    def add_user_visible_error(self, message: str, session: Any) -> None:
        pass

    def add_stdout(self, message: str, session: Any) -> None:
        self.logger.info(message)

    def add_stderr(self, message: str, session: Any) -> None:
        self.logger.exception(message)

    @property
    def session_maker(self) -> Any:
        return contextlib.nullcontext

    
class AbstractAdaptor(abc.ABC):
    resources: dict[str, int] = {}

    def __init__(self, form: dict[str, Any], context: Context = None, **config: Any) -> None:
        self.form = form
        self.config = config
        if context is None:
            self.context = Context()
        else:
            self.context = context

    @abc.abstractmethod
    def validate(self, request: Request) -> bool:
        pass

    @abc.abstractmethod
    def apply_constraints(self, request: Request) -> dict[str, Any]:
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

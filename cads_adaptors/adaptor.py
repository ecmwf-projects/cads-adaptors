import abc


class AbstractAdaptor(abc.ABC):
    def __init__(self, form, **config):
        self.form = form
        self.config = config

    @abc.abstractmethod
    def validate(self, request):
        pass

    @abc.abstractmethod
    def apply_constraints(self, request):
        pass

    @abc.abstractmethod
    def estimate_costs(self, request):
        pass

    @abc.abstractmethod
    def get_licences(self, request):
        pass

    @abc.abstractmethod
    def retrieve(self, request):
        pass


class DummyAdaptor(AbstractAdaptor):
    def validate(self, request):
        return True

    def apply_constraints(self, request):
        return {}

    def estimate_costs(self, request):
        size = int(request.get("size", 0))
        time = float(request.get("time", 0.0))
        return {"size": size, "time": time}

    def get_licences(self, request):
        return []

    def retrieve(self, request):
        import time

        size = int(request.get("size", 0))
        time_sleep = float(request.get("time", 0.0))

        time.sleep(time_sleep)
        with open("dummy.grib", "wb") as fp:
            with open("/dev/urandom", "rb") as random:
                while size > 0:
                    length = min(size, 10240)
                    fp.write(random.read(length))
                    size -= length
        return open("dummy.grib", "rb")

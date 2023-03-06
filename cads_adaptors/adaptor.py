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

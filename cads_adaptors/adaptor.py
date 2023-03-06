import abc


class AbstractAdaptor(abc.ABC):
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

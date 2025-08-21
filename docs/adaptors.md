# Adaptation design

Adaptors contain all custom logic for a catalogue entry.

Long-term implicit adaptor code:

```python
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


class AbstractCdsAdaptor(CADSAdaptor):
    # implement CDS defaults
    def retrieve(self, request):
        ...
```

```python
from cads_adaptors import adaptor_cds


class MyAdaptor(adaptor_cds.CdsAdaptor):
    def retrieve(self, request, metadata):
        # parse input options
        with cads.add_step_metrics("process inputs", metadata):
            request, format = cads.extract_format_options(request)
            request, reduce = cads.extract_reduce_options(request)
            mars_request = cads.map_to_mars_request(request)

        # retrieve data
        with cads.add_step_metrics("download data", metadata):
            data = cads.mars_retrieve(mars_request)

        # post-process data
        if reduce is not None:
            with cads.add_step_metrics("reduce data", metadata):
                data = cads.apply_reduce(data, reduce)

        if format is not None:
            with cads.add_step_metrics("reformat data", metadata):
                data = cads.translate(data, format)

        return data
```

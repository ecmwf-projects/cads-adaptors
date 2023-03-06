from . import adaptor, constraints, costing, mapping, url_tools


class CdsAdaptor(adaptor.AbstractAdaptor):
    def __init__(self, form, **config):
        self.form = form
        self.constraints = config.pop("constraints", [])
        self.mapping = config.pop("mapping", {})
        self.licences = config.pop("licences", [])
        self.config = config

    def validate(self, request):
        return True

    def apply_constraints(self, request):
        return constraints.apply_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request):
        costs = {"size": costing.estimate_size(self.form, request, self.constraints)}
        return costs

    def get_licences(self, request):
        return self.licences


class UrlCdsAdaptor(CdsAdaptor):
    def retrieve(self, request):
        data_format = request.pop("format", "zip")

        if data_format not in {"zip", "tgz"}:
            raise ValueError(f"{data_format=} is not supported")

        mapped_request = mapping.apply_mapping(request, self.mapping_config)

        requests_urls = url_tools.requests_to_urls(
            mapped_request, patterns=self.config["patterns"]
        )

        path = url_tools.download_from_urls(
            [ru["url"] for ru in requests_urls], data_format=data_format
        )
        return open(path, "rb")

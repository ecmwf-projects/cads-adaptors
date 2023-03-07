from . import adaptor, constraints, costing, mapping


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
        return constraints.validate_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request):
        costs = {"size": costing.estimate_size(self.form, request, self.constraints)}
        return costs

    def get_licences(self, request):
        return self.licences


class UrlCdsAdaptor(CdsAdaptor):
    def retrieve(self, request):
        from . import url_tools

        data_format = request.pop("format", "zip")

        if data_format not in {"zip", "tgz"}:
            raise ValueError(f"{data_format=} is not supported")

        mapped_request = mapping.apply_mapping(request, self.mapping)

        requests_urls = url_tools.requests_to_urls(
            mapped_request, patterns=self.config["patterns"]
        )

        path = url_tools.download_from_urls(
            [ru["url"] for ru in requests_urls], data_format=data_format
        )
        return open(path, "rb")


class LegacyCdsAdaptor(CdsAdaptor):
    def retrieve(self, request):
        import cdsapi

        # parse input options
        collection_id = self.config.pop("collection_id", None)
        if not collection_id:
            raise ValueError("collection_id is required in request")

        # retrieve data
        client = cdsapi.Client(self.config["url"], self.config["key"], retry_max=1)
        result_path = client.retrieve(collection_id, request).download()
        return open(result_path, "rb")

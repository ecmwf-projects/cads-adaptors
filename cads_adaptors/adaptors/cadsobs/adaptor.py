import logging

from cads_adaptors.adaptors.cadsobs.utils import get_objects_to_retrieve, retrieve_data
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor

logger = logging.getLogger(__name__)


class ObservationsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request):
        # Maps observation_type to source. This sets self.mapped_request
        self._pre_retrieve(request)
        # Assignment to avoid repeating self too many times
        mapped_request = self.mapped_request
        # Catalogue credentials are in config, which is parsed from adaptor.json
        obs_api_url = self.config["obs_api_url"]
        # Dataset name is in this config too
        dataset_name = self.config["collection_id"]
        # dataset_source must be a string, asking for two sources is unsupported
        dataset_source = mapped_request["dataset_source"]
        dataset_source = self.handle_sources_list(dataset_source)
        mapped_request["dataset_source"] = dataset_source
        mapped_request = self.adapt_parameters(mapped_request)
        # Request parameters validation happens here, not sure about how to move this to
        # validate method
        object_urls = get_objects_to_retrieve(dataset_name, mapped_request, obs_api_url)
        logger.debug(f"The following objects are going to be filtered: {object_urls}")
        output_path = retrieve_data(
            dataset_name, mapped_request, object_urls, obs_api_url
        )
        return open(output_path, "rb")

    def adapt_parameters(self, mapped_request: dict) -> dict:
        # We need these changes right now to adapt the parameters to what we need
        # Turn single values into length one lists
        for key_to_listify in ["variables", "stations", "year", "month", "day"]:
            if key_to_listify in mapped_request and not isinstance(
                mapped_request[key_to_listify], list
            ):
                mapped_request[key_to_listify] = [mapped_request[key_to_listify]]
        # Turn year, month, day strings into integers
        for key_to_int in ["year", "month", "day"]:
            mapped_request[key_to_int] = [int(v) for v in mapped_request[key_to_int]]
        # Turn area into latitude and longitude coverage
        if "area" in mapped_request:
            area = mapped_request.pop("area")
            mapped_request["latitude_coverage"] = [area[2], area[0]]
            mapped_request["longitude_coverage"] = [area[1], area[3]]
        return mapped_request

    def handle_sources_list(self, dataset_source: list | str) -> str:
        """Raise error if many, extract if list."""
        if isinstance(dataset_source, list):
            if len(dataset_source) > 1:
                self.context.add_user_visible_error(
                    "Asking for more than one observation_types in the same"
                    "request is currently unsupported."
                )
                raise RuntimeError(
                    "Asking for more than one observation_types in the same"
                    "request is currently unsupported."
                )
            else:
                # Get the string if there is only one item in the list.
                dataset_source = dataset_source[0]
        return dataset_source

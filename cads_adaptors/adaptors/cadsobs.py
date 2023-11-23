from pathlib import Path

from cads_adaptors import AbstractCdsAdaptor, mapping
from cdsobs.retrieve.api import retrieve_observations
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.utils.utils import get_database_session


class ObservationsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request):
        # Maps observation_type to source
        mapped_request = mapping.apply_mapping(request, self.mapping)
        # Catalogue credentials are in config, which is parsed from adaptor.json
        catalogue_url = self.config["catalogue_url"]
        storage_url = self.config["storage_url"]
        # Dataset name is in this config too
        dataset_name = self.config["collection_id"]
        # dataset_source must be a string, asking for two sources is unsupported
        dataset_source = mapped_request["dataset_source"]
        if isinstance(dataset_source, list):
            if len(dataset_source) > 1:
                raise RuntimeError(
                    "Asking for more than one observation_types in the same"
                    "request is currently unsupported."
                )
            else:
                # Get the string if there is only one item in the list.
                dataset_source = dataset_source[0]
        mapped_request["dataset_source"] = dataset_source
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
        # Request parameters validation happens here, not sure about how to move this to
        # validate method
        retrieve_args = RetrieveArgs(dataset=dataset_name, params=mapped_request)
        with get_database_session(catalogue_url) as session:
            output_file = retrieve_observations(
                session,
                storage_url,
                retrieve_args,
                Path("."),
                size_limit=1000000000000,
            )
            return open(output_file, "rb")

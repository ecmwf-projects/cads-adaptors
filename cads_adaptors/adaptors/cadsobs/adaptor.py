import tempfile
from pathlib import Path

from cads_adaptors.adaptors import Request
from cads_adaptors.adaptors.cadsobs.api_client import CadsobsApiClient
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import CadsObsRuntimeError, InvalidRequest


class ObservationsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.download_format = "as_source"

    def retrieve(self, request):
        try:
            output = super().retrieve(request)
        except KeyError as e:
            self.context.add_user_visible_error(repr(e))
            raise InvalidRequest(repr(e))
        except CadsObsRuntimeError as e:
            self.context.add_user_visible_error(repr(e))
            raise InvalidRequest(repr(e))
        except Exception as e:
            self.context.add_user_visible_error(repr(e))
            raise e
        return output

    def retrieve_list_of_results(self, request: Request) -> list[str]:
        from cads_adaptors.adaptors.cadsobs.retrieve import retrieve_data

        # Maps observation_type to source. This sets self.mapped_requests
        request = self.normalise_request(request)
        # TODO: handle lists of requests, normalise_request has the power to implement_constraints
        #  which produces a list of complete hypercube requests.
        try:
            assert len(self.mapped_requests) == 1
        except AssertionError:
            self.context.add_user_visible_log(
                f"WARNING: More than one request was mapped: {self.mapped_requests}, "
                f"returning the first one only:\n{self.mapped_requests[0]}"
            )
        self.mapped_request = self.mapped_requests[0]

        # Catalogue credentials are in config, which is parsed from adaptor.json
        obs_api_url = self.config["obs_api_url"]
        # Dataset name is in this config too
        dataset_name = self.config.get("dataset_name", self.collection_id)
        # dataset_source must be a string, asking for two sources is unsupported
        dataset_source = self.handle_sources_list(self.mapped_request["dataset_source"])
        self.mapped_request["dataset_source"] = dataset_source

        self.mapped_request = self.adapt_parameters()
        # Get CDM lite variables as a dict with mandatory, optional and auxiliary
        cadsobs_client = CadsobsApiClient(obs_api_url, self.context)
        cdm_lite_variables_dict = cadsobs_client.get_cdm_lite_variables()
        cdm_lite_variables = (
            cdm_lite_variables_dict["mandatory"] + cdm_lite_variables_dict["optional"]
        )
        # Get the objects that match the request
        object_urls = cadsobs_client.get_objects_to_retrieve(
            dataset_name, self.mapped_request
        )
        # Get the service definition file
        service_definition = cadsobs_client.get_service_definition(dataset_name)
        field_attributes = cdm_lite_variables_dict["attributes"]
        global_attributes = service_definition["global_attributes"]
        # TODO: Get licences from the config passed to the adaptor
        self.context.debug(
            f"The following objects are going to be filtered: {object_urls}"
        )
        output_dir = Path(tempfile.mkdtemp())
        output_path = retrieve_data(
            dataset_name,
            self.mapped_request,
            output_dir,
            object_urls,
            cdm_lite_variables,
            field_attributes,
            global_attributes,
            self.context,
        )
        return [str(output_path)]

    def adapt_parameters(self) -> dict:
        # We need these changes right now to adapt the parameters to what we need
        # Turn single values into length one lists
        for key_to_listify in ["variables", "stations", "year", "month", "day"]:
            if key_to_listify in self.mapped_request and not isinstance(
                self.mapped_request[key_to_listify], list
            ):
                self.mapped_request[key_to_listify] = [
                    self.mapped_request[key_to_listify]
                ]
        # Turn year, month, day strings into integers
        for key_to_int in ["year", "month", "day"]:
            self.mapped_request[key_to_int] = [
                int(v) for v in self.mapped_request[key_to_int]
            ]
        # Turn area into latitude and longitude coverage
        if "area" in self.mapped_request:
            area = self.mapped_request.pop("area")
            self.mapped_request["latitude_coverage"] = [area[2], area[0]]
            self.mapped_request["longitude_coverage"] = [area[1], area[3]]
        # Handle auxiliary variables such as uncertainty, which now are metadata
        return self.mapped_request

    def handle_sources_list(self, dataset_source: list | str) -> str:
        """Raise error if many, extract if list."""
        if isinstance(dataset_source, list):
            if len(dataset_source) > 1:
                error_message = (
                    "Asking for more than one observation_types in the same"
                    "request is currently unsupported."
                )
                raise InvalidRequest(error_message)
            else:
                # Get the string if there is only one item in the list.
                dataset_source_str = dataset_source[0]
        else:
            dataset_source_str = dataset_source
        return dataset_source_str

    def estimate_costs(self, request, **kwargs):
        """Estimate costs weighting by area."""
        costs = super().estimate_costs(request, **kwargs)
        if "area" in request:
            self.weight_by_area(costs, request)
        return costs

    def weight_by_area(self, costs, request):
        maxlat, minlon, minlat, maxlon = (float(i) for i in request["area"])
        # This "area" is in degrees^2. Is not a real area but area in the
        # lat-lon (PlateCarree) projection.
        requested_area = (maxlon - minlon) * (maxlat - minlat)
        # It is not realistic to have very little cost for small areas, so we set a
        # minimum value of 1000 square degrees (aprox a 30 by 30 box).
        # Ideally we should be making queries to the catalogue, but it would be too
        # costly to do this for every click.
        min_area = 1000
        requested_area = max(requested_area, min_area)
        total_area = 64800  # 360 * 180
        area_weight = requested_area / total_area
        for k, v in costs.items():
            costs[k] = v * area_weight

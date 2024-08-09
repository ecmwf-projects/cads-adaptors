import tempfile
from pathlib import Path

from cads_adaptors.adaptors import Request
from cads_adaptors.adaptors.cadsobs.api_client import CadsobsApiClient
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import CadsObsRuntimeError, InvalidRequest


class ObservationsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request):
        try:
            output = self._retrieve(request)
        except KeyError as e:
            self.context.add_user_visible_error(repr(e))
            raise InvalidRequest(repr(e))
        except Exception as e:
            self.context.add_user_visible_error(repr(e))
            raise e
        return output

    def _retrieve(self, request):
        from cads_adaptors.adaptors.cadsobs.retrieve import retrieve_data

        # Maps observation_type to source. This sets self.mapped_request
        self._pre_retrieve(request)

        # Catalogue credentials are in config, which is parsed from adaptor.json
        obs_api_url = self.config["obs_api_url"]
        # Dataset name is in this config too
        dataset_name = self.config["collection_id"]
        # Max size in bytes can be set in the adaptor.json. If not, it is set to 1 GB
        size_limit = self.config.get("size_limit", 1073741824)
        # dataset_source must be a string, asking for two sources is unsupported
        dataset_source = self.handle_sources_list(self.mapped_request["dataset_source"])
        self.mapped_request["dataset_source"] = dataset_source

        self.mapped_request = self.adapt_parameters()
        # Get CDM lite variables as a dict with mandatory, optional and auxiliary
        # Auxiliary variables are, generally, statistics associated with the mandatory
        # and/or optional variables. They are identified by matches with the full
        # variable name, e.g. VAR_standard_deviation, where standard_deviation is the
        # auxiliary variable. The will be included as additional columns in the output
        # file.
        cadsobs_client = CadsobsApiClient(obs_api_url, self.context)
        cdm_lite_variables_dict = cadsobs_client.get_cdm_lite_variables()
        cdm_lite_variables = (
            cdm_lite_variables_dict["mandatory"] + cdm_lite_variables_dict["optional"]
        )
        # Handle auxiliary variables
        aux_var_mapping = cadsobs_client.get_aux_var_mapping(
            dataset_name, dataset_source
        )
        # Note that this mutates mapped_request
        requested_auxiliary_variables = self.handle_auxiliary_variables(aux_var_mapping)
        cdm_lite_variables = cdm_lite_variables + list(requested_auxiliary_variables)
        # Get the objects that match the request
        object_urls = cadsobs_client.get_objects_to_retrieve(
            dataset_name, self.mapped_request, size_limit=size_limit
        )
        # Get the service definition file
        service_definition = cadsobs_client.get_service_definition(dataset_name)
        global_attributes = service_definition["global_attributes"]
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
            global_attributes,
            self.context,
        )
        return open(output_path, "rb")

    def handle_auxiliary_variables(
        self, aux_var_mapping: dict
    ) -> set[str]:
        """Remove auxiliary variables from the request and add them as extra fields."""
        requested_variables = self.mapped_request["variables"].copy()
        regular_variables = [v for v in requested_variables if v in aux_var_mapping]
        auxiliary_variables = [
            v for v in requested_variables if v not in aux_var_mapping
        ]
        requested_metadata_fields = set()
        for regular_variable in regular_variables:
            for auxvar_dict in aux_var_mapping[regular_variable]:
                auxvar = auxvar_dict["auxvar"]
                if auxvar in auxiliary_variables:
                    metadata_field = auxvar_dict["metadata_name"]
                    self.context.warning(
                        f"{auxvar} is an auxiliary variable, it will be included"
                        f"as an extra {metadata_field} column in the output file, not as a "
                        f"regular variable."
                    )
                    requested_variables.remove(auxvar)
                    requested_metadata_fields.add(metadata_field)
        # Check that there are no orphan auxiliary variables without its regular
        # variable and if any, add the regular variable
        inverse_aux_var_mapping = {
            auxvar_dict["auxvar"]: regular_var
            for regular_var, auxiliary_vars in aux_var_mapping.items()
            for auxvar_dict in auxiliary_vars
        }
        for auxvar in auxiliary_variables:
            regular_variable = inverse_aux_var_mapping[auxvar]
            if regular_variable not in requested_variables:
                self.context.warning(
                    f"{auxvar} is auxiliary metadata of variable {regular_variable}, "
                    f"adding ir to the request."
                )
                requested_variables.append(regular_variable)
        self.mapped_request["variables"] = requested_variables

        return requested_metadata_fields

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
                self.context.add_user_visible_error(error_message)
                raise CadsObsRuntimeError(error_message)
            else:
                # Get the string if there is only one item in the list.
                dataset_source_str = dataset_source[0]
        else:
            dataset_source_str = dataset_source
        return dataset_source_str

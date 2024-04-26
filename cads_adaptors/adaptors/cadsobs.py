import logging
import tempfile
from pathlib import Path

import fsspec
import h5netcdf
import requests
from cdsobs.retrieve.api import (
    _add_attributes,
    _get_char_sizes,
    _get_output_path,
    _to_csv,
    filter_asset_and_save,
)
from cdsobs.retrieve.models import RetrieveArgs, RetrieveParams

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor

logger = logging.Logger(__name__)


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
        output_path = retrieve_data(dataset_name, mapped_request, object_urls)
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


def get_objects_to_retrieve(
    dataset_name: str, mapped_request: dict, obs_api_url: str
) -> list[str]:
    payload = dict(
        retrieve_args=dict(dataset=dataset_name, params=mapped_request),
        config=dict(size_limit=100000),
    )
    objects_to_retrieve = requests.post(
        f"{obs_api_url}/get_object_urls_and_check_size", json=payload
    ).json()
    return objects_to_retrieve


def retrieve_data(
    dataset_name: str, mapped_request: dict, object_urls: list[str]
) -> Path:
    output_dir = Path(tempfile.mkdtemp())
    output_path_netcdf = _get_output_path(output_dir, dataset_name, "netCDF")
    logger.info(f"Streaming data to {output_path_netcdf}")
    # We first need to loop over the files to get the max size of the strings fields
    # This way we can know the size of the output
    # background cache will download blocks in the background ahead of time using a
    # thread.
    fs = fsspec.filesystem("https", cache_type="background", block_size=10 * (1024**2))
    # Silence fsspec log as background cache does print unformatted log lines.
    logging.getLogger("fsspec").setLevel(logging.WARNING)
    # Get the maximum size of the character arrays
    char_sizes = _get_char_sizes(fs, object_urls)
    variables = mapped_request["variables"]
    char_sizes["observed_variable"] = max([len(v) for v in variables])
    # Open the output file and dump the data from each input file.
    retrieve_args = RetrieveArgs(
        dataset=dataset_name, params=RetrieveParams(**mapped_request)
    )
    with h5netcdf.File(output_path_netcdf, "w") as oncobj:
        oncobj.dimensions["index"] = None
        for url in object_urls:
            filter_asset_and_save(fs, oncobj, retrieve_args, url, char_sizes)
        # Check if the resulting file is empty
        if len(oncobj.variables) == 0 or len(oncobj.variables["report_timestamp"]) == 0:
            raise RuntimeError(
                "No data was found, try a different parameter combination."
            )
        # Add atributes
        _add_attributes(oncobj, retrieve_args)
    # If the user asked for a CSV, we transform the file to CSV
    if retrieve_args.params.format == "netCDF":
        output_path = output_path_netcdf
    else:
        try:
            output_path = _to_csv(output_dir, output_path_netcdf, retrieve_args)
        finally:
            # Ensure that the netCDF is not left behind taking disk space.
            output_path_netcdf.unlink()
    return output_path

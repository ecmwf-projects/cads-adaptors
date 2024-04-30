import logging
from pathlib import Path

import fsspec
import h5netcdf

from cads_adaptors.adaptors.cadsobs.csv import to_csv
from cads_adaptors.adaptors.cadsobs.models import RetrieveArgs, RetrieveParams
from cads_adaptors.adaptors.cadsobs.utils import (
    _add_attributes,
    _filter_asset_and_save,
    _get_char_sizes,
    _get_output_path,
)

logger = logging.getLogger(__name__)


def retrieve_data(
    dataset_name: str,
    mapped_request: dict,
    output_dir: Path,
    object_urls: list[str],
    cdm_lite_variables: list[str],
    global_attributes: dict,
) -> Path:
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
            _filter_asset_and_save(
                fs, oncobj, retrieve_args, url, char_sizes, cdm_lite_variables
            )
        # Check if the resulting file is empty
        if len(oncobj.variables) == 0 or len(oncobj.variables["report_timestamp"]) == 0:
            raise RuntimeError(
                "No data was found, try a different parameter combination."
            )
        # Add atributes
        _add_attributes(oncobj, global_attributes)
    # If the user asked for a CSV, we transform the file to CSV
    if retrieve_args.params.format == "netCDF":
        output_path = output_path_netcdf
    else:
        try:
            output_path = to_csv(output_dir, output_path_netcdf, retrieve_args)
        finally:
            # Ensure that the netCDF is not left behind taking disk space.
            output_path_netcdf.unlink()
    return output_path

from pathlib import Path

import fsspec

from cads_adaptors import Context
from cads_adaptors.adaptors.cadsobs.csv import to_csv, to_zip
from cads_adaptors.adaptors.cadsobs.models import RetrieveArgs, RetrieveParams
from cads_adaptors.adaptors.cadsobs.utils import (
    add_attributes,
    get_output_path,
)
from cads_adaptors.adaptors.cadsobs.char_utils import get_char_sizes
from cads_adaptors.adaptors.cadsobs.filter import filter_asset_and_save
from cads_adaptors.exceptions import CadsObsRuntimeError


def retrieve_data(
    dataset_name: str,
    mapped_request: dict,
    output_dir: Path,
    object_urls: list[str],
    cdm_lite_variables: list[str],
    field_attributes: dict,
    global_attributes: dict,
    context: Context,
) -> Path:
    """Loop over the netCDFs in the storage, open and filter the requested data.

    The data requested is saved to the output file. The index dimensionis resized each
    time in order to append the new data found in each file. Finally the data is
    transformed to CSV if this is the format requested.
    """
    import h5netcdf

    output_path_netcdf = get_output_path(output_dir, dataset_name, "netCDF")
    context.add_stdout(f"Streaming data to {output_path_netcdf}")

    # We first need to loop over the files to get the max size of the strings fields
    # This way we can know the size of the output
    # background cache will download blocks in the background ahead of time using a
    # thread.
    fs = fsspec.filesystem("https", cache_type="background", block_size=10 * (1024**2))
    # Silence fsspec log as background cache does print unformatted log lines.
    # Get the maximum size of the character arrays
    char_sizes = get_char_sizes(fs, object_urls)
    variables = mapped_request["variables"]
    char_sizes["observed_variable"] = max([len(v) for v in variables])
    # Open the output file and dump the data from each input file.
    retrieve_args = RetrieveArgs(
        dataset=dataset_name, params=RetrieveParams(**mapped_request)
    )
    with h5netcdf.File(output_path_netcdf, "w") as oncobj:
        oncobj.dimensions["index"] = None
        for url in object_urls:
            filter_asset_and_save(
                fs, oncobj, retrieve_args, url, char_sizes, cdm_lite_variables
            )
        # Check if the resulting file is empty
        if len(oncobj.variables) == 0 or len(oncobj.variables["report_timestamp"]) == 0:
            message = "No data was found, try a different parameter combination."
            # context.add_user_visible_error(message)
            raise CadsObsRuntimeError(message)
        # Add atributes
        add_attributes(oncobj, field_attributes, global_attributes)
    # If the user asked for a CSV, we transform the file to CSV and zip it
    if retrieve_args.params.format == "netCDF":
        output_path = output_path_netcdf
    else:
        output_path_csv = get_output_path(output_dir, retrieve_args.dataset, "csv")
        output_zip_path = output_path_csv.with_suffix(".zip")
        try:
            output_path_csv = to_csv(output_path_csv, output_path_netcdf, retrieve_args)
            output_path = to_zip(output_path_csv, output_zip_path)
        finally:
            # Ensure that the netCDF is not left behind taking disk space.
            output_path_netcdf.unlink()
            output_path_csv.unlink(missing_ok=True)
    return output_path

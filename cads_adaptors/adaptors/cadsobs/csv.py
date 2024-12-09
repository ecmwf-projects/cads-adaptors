import logging
import zipfile
from pathlib import Path

import xarray

from cads_adaptors.adaptors.cadsobs.models import RetrieveArgs
from cads_adaptors.adaptors.cadsobs.utils import _get_output_path

logger = logging.getLogger(__name__)


def to_csv(
    output_dir: Path, output_path_netcdf: Path, retrieve_args: RetrieveArgs
) -> Path:
    """Transform the output netCDF to CSV format."""
    output_path = _get_output_path(output_dir, retrieve_args.dataset, "csv")
    # Beware xarray will silently ignore the chunk size if the dimension does not exist
    cdm_lite_dataset = xarray.open_dataset(
        output_path_netcdf, chunks=dict(index=50000), decode_times=True
    )
    logger.info("Transforming netCDF to CSV")
    with output_path.open("w") as ofileobj:
        header = get_csv_header(retrieve_args, cdm_lite_dataset)
        ofileobj.write(header)
    # Beware this will not work with old dask versions because of a bug
    # https://github.com/dask/dask/issues/10414
    cdm_lite_dataset.to_dask_dataframe().astype("str").to_csv(
        output_path, index=False, single_file=True, mode="a"
    )
    return output_path


def get_csv_header(
    retrieve_args: RetrieveArgs, cdm_lite_dataset: xarray.Dataset
) -> str:
    """Return the header of the CSV file."""
    template = """
########################################################################################
# This file contains data retrieved from the CDS https://cds.climate.copernicus.eu/cdsapp#!/dataset/{dataset}
# This is a C3S product under the following licences:
#     - licence-to-use-copernicus-products
#     - woudc-data-policy
# This is a CSV file following the CDS convention cdm-obs
# Data source: {dataset_source}
# Version:
# Time extent: {time_start} - {time_end}
# Geographic area (minlat/maxlat/minlon/maxlon): {area}
# Variables selected and units
{varstr}
########################################################################################
"""
    if "latitude|station_configuration" in cdm_lite_dataset:
        coord_table = "station_configuration"
    else:
        coord_table = "header_table"
    area = "{:.2f}/{:.2f}/{:.2f}/{:.2f}".format(
        cdm_lite_dataset[f"latitude|{coord_table}"].min().compute().item(),
        cdm_lite_dataset[f"latitude|{coord_table}"].max().compute().item(),
        cdm_lite_dataset[f"longitude|{coord_table}"].min().compute().item(),
        cdm_lite_dataset[f"longitude|{coord_table}"].max().compute().item(),
    )
    time_start = "{:%Y%m%d}".format(
        cdm_lite_dataset.report_timestamp[0].compute().dt.date
    )
    time_end = "{:%Y%m%d}".format(
        cdm_lite_dataset.report_timestamp[-1].compute().dt.date
    )
    # Subset the dataset to get variables and units, drop duplicates, encode and convert
    # to tuples.
    vars_and_units = list(
        cdm_lite_dataset[["observed_variable", "units"]]
        .to_dataframe()
        .drop_duplicates()
        .astype("U")
        .itertuples(index=False, name=None)
    )
    varstr = "\n".join([f"# {v} [{u}]" for v, u in vars_and_units])
    header_params = dict(
        dataset=retrieve_args.dataset,
        dataset_source=retrieve_args.params.dataset_source,
        area=area,
        time_start=time_start,
        time_end=time_end,
        varstr=varstr,
    )
    header = template.format(**header_params)
    return header


def to_zip(input_file_path: Path) -> Path:
    """Zips the given file into a .zip archive."""
    # Determine output zip path
    output_zip_path = input_file_path.with_suffix(".zip")

    # Create zip archive
    with zipfile.ZipFile(output_zip_path, "w") as zipf:
        zipf.write(input_file_path, arcname=input_file_path.name)

    return output_zip_path

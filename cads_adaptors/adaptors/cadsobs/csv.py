import logging
import zipfile
from pathlib import Path

import xarray

from cads_adaptors.adaptors.cadsobs.models import RetrieveArgs
from cads_adaptors.tools.general import ensure_list

logger = logging.getLogger(__name__)


def to_csv(
    output_path: Path, output_path_netcdf: Path, retrieve_args: RetrieveArgs
) -> Path:
    """Transform the output netCDF to CSV format."""
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
    template = """########################################################################################
# This file contains data retrieved from the CDS https://cds.climate.copernicus.eu/datasets/{dataset}
# This is a C3S product under the following licences:
{licence_list}
# This is a CSV file following the CDS convention cdm-obs
# Data source: {dataset_source}
# Time extent: {time_start} - {time_end}
# Geographic area (minlat/maxlat/minlon/maxlon): {area}
# Variables selected and units:
{varstr}
# Uncertainty legend:
{uncertainty_str}
########################################################################################
"""
    area = "{:.2f}/{:.2f}/{:.2f}/{:.2f}".format(
        cdm_lite_dataset["latitude"].min().compute().item(),
        cdm_lite_dataset["latitude"].max().compute().item(),
        cdm_lite_dataset["longitude"].min().compute().item(),
        cdm_lite_dataset["longitude"].max().compute().item(),
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
    # Uncertainty documentation
    uncertainty_vars = [
        str(v) for v in cdm_lite_dataset.data_vars if "uncertainty_value" in str(v)
    ]
    if len(uncertainty_vars) > 0:
        unc_vars_and_names = [
            (u, get_long_name(cdm_lite_dataset, u)) for u in uncertainty_vars
        ]
        uncertainty_str = "\n".join([f"# {u} {n}" for u, n in unc_vars_and_names])
    else:
        uncertainty_str = "# No uncertainty columns available for this dataset."
    # List of licences
    license_list = ensure_list(cdm_lite_dataset.attrs["licence_list"])
    licence_list_str = "\n".join(f"# {licence}" for licence in license_list)
    # Render the header
    header_params = dict(
        dataset=retrieve_args.dataset,
        dataset_source=retrieve_args.params.dataset_source,
        area=area,
        time_start=time_start,
        time_end=time_end,
        varstr=varstr,
        uncertainty_str=uncertainty_str,
        licence_list=licence_list_str,
    )
    header = template.format(**header_params)
    return header


def to_zip(input_file_path: Path, output_zip_path: Path) -> Path:
    """Zips the given file into a .zip archive."""
    # Create zip archive
    with zipfile.ZipFile(output_zip_path, "w") as zipf:
        zipf.write(input_file_path, arcname=input_file_path.name)

    return output_zip_path


def get_long_name(cdm_lite_dataset: xarray.Dataset, uncertainty_type: str) -> str:
    long_name = cdm_lite_dataset[uncertainty_type].long_name
    return long_name.capitalize().replace("_", " ")

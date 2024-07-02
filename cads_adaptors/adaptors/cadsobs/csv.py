import logging
from pathlib import Path

import numpy
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
        output_path_netcdf, chunks=dict(index=100000), decode_times=True
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
    area = "{}/{}/{}/{}".format(
        cdm_lite_dataset.latitude.min().compute().item(),
        cdm_lite_dataset.latitude.max().compute().item(),
        cdm_lite_dataset.longitude.min().compute().item(),
        cdm_lite_dataset.longitude.max().compute().item(),
    )
    time_start = "{:%Y%m%d}".format(
        cdm_lite_dataset.report_timestamp[0].compute().dt.date
    )
    time_end = "{:%Y%m%d}".format(
        cdm_lite_dataset.report_timestamp[-1].compute().dt.date
    )
    vars_and_units = zip(
        numpy.unique(cdm_lite_dataset.observed_variable.to_index().str.decode("utf-8")),
        numpy.unique(cdm_lite_dataset.units.to_index().str.decode("utf-8")),
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

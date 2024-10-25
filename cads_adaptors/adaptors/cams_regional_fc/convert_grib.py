from os.path import exists

from .convert_grib_to_netcdf import convert_grib_to_netcdf
from .create_file import create_file
from .formats import Formats


def convert_grib(req_groups, info, regfc_defns, context):
    """Convert files to NetCDF if required."""

    # Convert to NetCDF?
    if info["format"] in [Formats.netcdf, Formats.netcdf_zip, Formats.netcdf_cdm]:
        for req_group in req_groups:
            req_group["nc_file"] = create_file("convert", ".nc", info)
            if info["format"] in (Formats.netcdf, Formats.netcdf_zip):
                convert_grib_to_netcdf(
                    req_group["requests"],
                    req_group["grib_file"],
                    req_group["nc_file"],
                    regfc_defns,
                )
            #elif info["format"] == Formats.netcdf_cdm:
            #    convert_grib_to_netcdf_cdm(
            #        req_group["grib_file"], req_group["nc_file"], dataset_dir, context
            #    )
            else:
                raise Exception("Unrecognised format: " + info["format"])


def convert_grib_to_netcdf_cdm(infile, outfile, dataset_dir, context):
    # The ECCODES_DEFINITION_PATH variable is required for ecCodes to be able
    # to recognise the regional forecast parameters
    context.run_command(
        "cdscdm-translate",
        "-o",
        outfile,
        "--product",
        "CAMS",
        "--merge_datasets",
        "true",
        infile,
        environ={"ECCODES_DEFINITION_PATH": dataset_dir + "/eccodes_definitions"},
    )

    if not exists(outfile):
        raise Exception("netcdf_cdm conversion failed - no output file")

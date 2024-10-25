from cds_common.message_iterators import grib_file_iterator, grib_file_ordered_iterator
from eccodes import codes_get, codes_get_message, codes_release, codes_set

from .area_subset import area_subset_handle
from .create_file import create_file
from .formats import Formats
from .grib2request import grib2request


def process_grib_files(req_groups, info, context):
    """Merge grib files that need to be combined, reordering fields if required.
    Also extract any required sub-area.
    """
    # Records alterations required to the GRIB fields/files, such as area
    # sub-selection
    alterations = {"area": info["area"]}

    # Ensure the output fields are sorted if outputting in GRIB. Nice for the
    # user.
    alterations["ordered"] = info["format"] == Formats.grib

    # The netcdf_cdm converter will choke if given fields with differing
    # typeOfLevel. Since the surface fields use "surface" and the others use
    # "heightAboveGround" we need to patch the GRIB before giving it to the
    # converter.
    alterations["surface_fix"] = info["format"] == Formats.netcdf_cdm

    # For each group, combine any grib files into one, after alteration if
    # required
    for req_group in req_groups:
        # If there is only one retrieved file for this group and it does not
        # require any alteration then use as-is. Otherwise, copy data to the
        # new file.
        if len(req_group["retrieved_files"]) == 1 and \
           not any(alterations.values()) and \
           info["stages"][-1] != "merge_grib":
            req_group["grib_file"] = req_group["retrieved_files"][0]
        else:
            # Copy data to the grib file
            req_group["grib_file"] = create_file("merge_grib", ".grib", info)
            with open(req_group["grib_file"], "wb") as fout:
                for data in data_processor(req_group, alterations, context):
                    fout.write(data)


def data_processor(req_group, alterations, context):
    """Yield chunks of data from the grib files in req_group.
    Sub-areas will be extracted and fields will be ordered if required.
    """
    if not any(alterations.values()):
        # The binary data can be directly copied without grib decoding - fast
        for file in req_group["retrieved_files"]:
            with open(file, "rb") as fin:
                while True:
                    data = fin.read(1024 * 1024)  # 1MB chunks
                    if not data:
                        break
                    yield data

    else:
        # Grib decoding required. Yield fields in order if required.
        if alterations["ordered"]:
            iterator = grib_file_ordered_iterator(
                req_group["retrieved_files"],
                req_group["requests"],
                grib2request,
                logger=context,
            )
        else:
            iterator = grib_file_iterator(req_group["retrieved_files"])
        for msg in iterator:
            # Patch surface-level fields?
            if (
                alterations["surface_fix"]
                and codes_get(msg, "typeOfLevel") == "surface"
            ):
                codes_set(msg, "typeOfLevel", "heightAboveGround")
                codes_set(msg, "level", 0)

            # Sub-area extraction required?
            if alterations["area"]:
                msg2 = area_subset_handle(msg, alterations["area"])
                data = codes_get_message(msg2)
                codes_release(msg2)
            else:
                data = codes_get_message(msg)

            yield data

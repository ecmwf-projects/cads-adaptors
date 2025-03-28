import itertools
import os
import zipfile
from typing import Any, BinaryIO, Callable, Dict, List
from cacholote.extra_encoders import InPlaceFile

import yaml

# compression parameters for the supported file types
# feel free to adjust or add entries
UNKNOWN_EXTENSION = "UNKNOWN_EXTENSION"
UNKNOWN_EXTENSION_PARAMS = {"algo": zipfile.ZIP_STORED, "level": 0}
COMPRESSION_PARAMS = {
    "nc": {"algo": zipfile.ZIP_DEFLATED, "level": 0},
    "grib": {"algo": zipfile.ZIP_DEFLATED, "level": 4},
    "csv": {"algo": zipfile.ZIP_DEFLATED, "level": 6},
}


# could be done with python-magic
def determine_file_type(path: str) -> str:
    extension = os.path.splitext(path)[1][1:]
    if len(extension) == 0:
        extension = UNKNOWN_EXTENSION
    return extension


def group_files_by_type(paths: list[str]):
    return itertools.groupby(paths, key=determine_file_type)


# TODO zipstream for archive creation
def zip_paths(
    paths: List[str],
    base_target: str = "output-data",
    receipt: Any = None,
    compression_params_lookup_table: dict[str, dict[str, int]] = COMPRESSION_PARAMS,
    **kwargs,
) -> BinaryIO:
    target = f"{base_target}.zip"

    files_grouped_by_type = group_files_by_type(paths)

    for file_type, paths_for_file_type in files_grouped_by_type:
        # determine compression parameters for the current file type
        compression_params = compression_params_lookup_table.get(
            file_type, UNKNOWN_EXTENSION_PARAMS
        )
        compression_algorithm = compression_params["algo"]
        compression_level = compression_params["level"]

        # perform the compression
        with zipfile.ZipFile(
            target,
            mode="a",
            compression=compression_algorithm,
            compresslevel=compression_level,
        ) as archive:
            for path in paths_for_file_type:
                if kwargs.get("preserve_dir", False):
                    archive_name = path
                else:
                    archive_name = os.path.basename(path)
                archive.write(path, archive_name)

            if receipt is not None:
                yaml_output: str = yaml.safe_dump(receipt, indent=2)
                archive.writestr(
                    f"receipt-{base_target}.yaml",
                    data=yaml_output,
                )

    for path in paths:
        os.remove(path)

    return open(target, "rb")


# TODO use targzstream
def targz_paths(
    paths: List[str],
    base_target: str = "output-data",
    receipt: Any = None,
    **kwargs,
) -> BinaryIO:
    import tarfile

    target = f"{base_target}.tar.gz"
    with tarfile.open(target, "w:gz") as archive:
        for path in paths:
            if kwargs.get("preserve_dir", False):
                archive_name = path
            else:
                archive_name = os.path.basename(path)
            archive.add(path, arcname=archive_name)

        if receipt is not None:
            receipt_fname = f"receipt-{base_target}.yaml"
            with open(receipt_fname, "w") as receipt_file:
                yaml.safe_dump(receipt, stream=receipt_file, indent=2)
            archive.add(receipt_fname)

    for path in paths:
        os.remove(path)

    return open(target, "rb")


def as_source(paths: List[str], **kwargs) -> BinaryIO:
    # Only return as_source if a single path, otherwise list MUST be requested
    if len(paths) == 1:
        if paths[0].endswith(".grib"):
            facts = paths[0].split("/")
            if facts[-2] == "mars":
                return InPlaceFile(paths[0], "rb")
        return open(paths[0], "rb")
    else:
        raise ValueError("as_source can only be used for a single file.")


DOWNLOAD_FORMATS: Dict[str, Callable[..., BinaryIO]] = {
    "zip": zip_paths,
    "tgz": targz_paths,
    "as_source": as_source,
}

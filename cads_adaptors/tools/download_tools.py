import os
from typing import Any, BinaryIO, Callable, Dict, List, Union

import yaml

from cads_adaptors.tools.general import ensure_list


# TODO zipstream for archive creation
def zip_paths(
    paths: List[str], base_target: str = "output-data", receipt: Any = None, **kwargs
) -> BinaryIO:
    import zipfile

    target = f"{base_target}.zip"
    with zipfile.ZipFile(target, mode="w") as archive:
        for path in paths:
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


def list_paths(
    paths: List[str],
    **kwargs,
) -> List[BinaryIO]:
    if kwargs.get("receipt") is not None:
        receipt_fname = f"receipt-{kwargs.get('base_target', 'nohash')}.yaml"
        with open(receipt_fname, "w") as receipt_file:
            yaml.safe_dump(kwargs.get("receipt"), stream=receipt_file, indent=2)
        paths.append(receipt_fname)
    return [open(path, "rb") for path in ensure_list(paths)]


def as_source(paths: List[str], **kwargs) -> Union[BinaryIO, List[BinaryIO]]:
    # Only return as_source if a single path, otherwise list MUST be requested
    if len(paths) == 1:
        return open(paths[0], "rb")
    else:
        return list_paths(paths, **kwargs)


DOWNLOAD_FORMATS: Dict[str, Callable] = {
    "zip": zip_paths,
    "tgz": targz_paths,
    "list": list_paths,
    "as_source": as_source,
}

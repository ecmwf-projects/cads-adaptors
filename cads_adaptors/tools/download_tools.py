import os
from typing import BinaryIO, Callable, Dict, List

from cads_adaptors.tools import ensure_list


# TODO zipstream for archive creation
def zip_paths(paths: List[str], base_target: str = "output-data", **kwargs) -> BinaryIO:
    import zipfile

    target = f"{base_target}.zip"
    with zipfile.ZipFile(target, mode="w") as archive:
        for p in paths:
            if kwargs.get("preserve_dir", False):
                arcname = p
            else:
                arcname = os.path.basename(p)
            archive.write(p, arcname)

    for p in paths:
        os.remove(p)

    return open(target, "rb")


# TODO use targzstream
def targz_paths(
    paths: List[str],
    base_target: str = "output-data",
    **kwargs,
) -> BinaryIO:
    import tarfile

    target = f"{base_target}.tar.gz"
    with tarfile.open(target, "w:gz") as archive:
        for p in paths:
            if kwargs.get("preserve_dir", False):
                arcname = p
            else:
                arcname = os.path.basename(p)
            archive.add(p, arcname=arcname)

    for p in paths:
        os.remove(p)

    return open(target, "rb")


def list_paths(
    paths: List[str],
    **kwargs,
) -> List:
    return [open(path, "rb") for path in ensure_list(paths)]


def as_source(paths: List[str], **kwargs) -> BinaryIO:
    # Only return as_source if a single path, otherwise list MUST be requested
    assert len(paths) == 1
    return open(paths[0], "rb")


DOWNLOAD_FORMATS: Dict[str, Callable] = {
    "zip": zip_paths,
    "tgz": targz_paths,
    "list": list_paths,
    "as_source": as_source,
}

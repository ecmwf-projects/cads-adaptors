import os
from typing import List

from cads_adaptors.tools import ensure_list

# TODO use targzstream
def zip_paths(
    paths: List[str],
    base_target: str,
) -> str:
    import zipfile

    target = f"{base_target}.zip"
    with zipfile.ZipFile(target, mode="w") as archive:
        for p in paths:
            archive.write(p)

    for p in paths:
        os.remove(p)

    return open(target, "rb")


# TODO zipstream for archive creation
def targz_paths(
    paths: List[str],
    base_target: str,
    **kwargs,
) -> str:
    import tarfile

    target = f"{base_target}.tar.gz"
    with tarfile.open(target, "w:gz") as archive:
        for p in paths:
            archive.add(p)

    for p in paths:
        os.remove(p)

    return open(target, "rb")


def list_paths(
    paths: List[str],
    **kwargs,
):
    return [open(path, "rb") for path in ensure_list(paths)]


def raw_path(
    paths: List[str],
    **kwargs
):
    assert len(paths)==1
    return open(paths[0], "rb")


DOWNLOAD_FORMATS = {
    "zip": zip_paths,
    "tgz": targz_paths,
    "list": list_paths,
    "raw": raw_path,
}

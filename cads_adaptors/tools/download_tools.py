import io
import os
import zipfile
from typing import Any, BinaryIO, Callable, Dict, List

import yaml
import zipstream

# compression parameters for the supported file types
# feel free to adjust or add entries
# Note: zipstream-new does not support per-entry compression levels;
# ZIP_DEFLATED entries use the library's default level.
UNKNOWN_EXTENSION = "UNKNOWN_EXTENSION"
UNKNOWN_EXTENSION_PARAMS = {"algo": zipfile.ZIP_STORED}
COMPRESSION_PARAMS = {
    "nc": {"algo": zipfile.ZIP_STORED},
    "grib": {"algo": zipfile.ZIP_DEFLATED},
    "csv": {"algo": zipfile.ZIP_DEFLATED},
}


# could be done with python-magic
def determine_file_type(path: str) -> str:
    extension = os.path.splitext(path)[1][1:]
    if len(extension) == 0:
        extension = UNKNOWN_EXTENSION
    return extension


class _ZipStreamIO(io.RawIOBase):
    def __init__(self, zf: zipstream.ZipFile, paths_to_delete: List[str]):
        self._chunks = iter(zf)
        self._buf = b""
        self._paths_to_delete = paths_to_delete

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:
        while not self._buf:
            try:
                self._buf = next(self._chunks)
            except StopIteration:
                return 0  # EOF
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n

    def close(self) -> None:
        super().close()
        for path in self._paths_to_delete:
            os.remove(path)


def zip_paths(
    paths: List[str],
    base_target: str = "output-data",
    receipt: Any = None,
    compression_params_lookup_table: dict[str, dict[str, int]] = COMPRESSION_PARAMS,
    **kwargs,
) -> BinaryIO:
    zf = zipstream.ZipFile(mode="w")

    for path in paths:
        compression_params = compression_params_lookup_table.get(
            determine_file_type(path), UNKNOWN_EXTENSION_PARAMS
        )
        archive_name = path if kwargs.get("preserve_dir", False) else os.path.basename(path)
        zf.write(
            path,
            arcname=archive_name,
            compress_type=compression_params["algo"],
        )

    if receipt is not None:
        yaml_output: str = yaml.safe_dump(receipt, indent=2)
        zf.writestr(
            f"receipt-{base_target}.yaml",
            data=yaml_output.encode(),
        )

    return io.BufferedReader(_ZipStreamIO(zf, paths))


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
        return open(paths[0], "rb")
    else:
        raise ValueError("as_source can only be used for a single file.")


DOWNLOAD_FORMATS: Dict[str, Callable[..., BinaryIO]] = {
    "zip": zip_paths,
    "tgz": targz_paths,
    "as_source": as_source,
}

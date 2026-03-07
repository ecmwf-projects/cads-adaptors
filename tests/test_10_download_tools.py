import io
import os
import zipfile

import pytest
import yaml

from cads_adaptors.tools.download_tools import zip_paths


def _make_file(tmp_path, name: str, content: bytes) -> str:
    path = str(tmp_path / name)
    with open(path, "wb") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------


def test_zip_paths_returns_readable_stream(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"netcdf content")
    stream = zip_paths([f])
    data = stream.read()
    assert len(data) > 0
    assert zipfile.is_zipfile(io.BytesIO(data))


def test_zip_paths_file_contents_preserved(tmp_path):
    content_nc = b"netcdf data here"
    content_grib = b"grib data here"
    f1 = _make_file(tmp_path, "test.nc", content_nc)
    f2 = _make_file(tmp_path, "test.grib", content_grib)

    data = zip_paths([f1, f2]).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert zf.read("test.nc") == content_nc
        assert zf.read("test.grib") == content_grib


def test_zip_paths_multiple_file_types(tmp_path):
    files = [
        _make_file(tmp_path, "a.nc", b"nc"),
        _make_file(tmp_path, "b.grib", b"grib"),
        _make_file(tmp_path, "c.csv", b"csv"),
    ]

    data = zip_paths(files).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert set(zf.namelist()) == {"a.nc", "b.grib", "c.csv"}


def test_zip_paths_interleaved_file_types(tmp_path):
    """Files with interleaved types should all end up in the archive once each."""
    files = [
        _make_file(tmp_path, "a.nc", b"nc1"),
        _make_file(tmp_path, "b.grib", b"grib"),
        _make_file(tmp_path, "c.nc", b"nc2"),
    ]

    data = zip_paths(files).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = zf.namelist()
        assert len(names) == 3
        assert zf.read("a.nc") == b"nc1"
        assert zf.read("b.grib") == b"grib"
        assert zf.read("c.nc") == b"nc2"


# ---------------------------------------------------------------------------
# Receipt
# ---------------------------------------------------------------------------


def test_zip_paths_receipt_included(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")
    receipt = {"dataset": "era5", "variables": ["t2m"]}

    data = zip_paths([f], base_target="my-output", receipt=receipt).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert "receipt-my-output.yaml" in zf.namelist()
        loaded = yaml.safe_load(zf.read("receipt-my-output.yaml"))
        assert loaded == receipt


def test_zip_paths_no_receipt_when_none(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")

    data = zip_paths([f], receipt=None).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert not any(name.startswith("receipt-") for name in zf.namelist())


def test_zip_paths_receipt_not_duplicated_for_mixed_types(tmp_path):
    """Receipt should appear exactly once even when multiple file types are present."""
    files = [
        _make_file(tmp_path, "a.nc", b"nc"),
        _make_file(tmp_path, "b.grib", b"grib"),
    ]
    receipt = {"key": "value"}

    data = zip_paths(files, receipt=receipt).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        receipt_entries = [n for n in zf.namelist() if n.startswith("receipt-")]
        assert len(receipt_entries) == 1


# ---------------------------------------------------------------------------
# preserve_dir option
# ---------------------------------------------------------------------------


def test_zip_paths_default_strips_directory(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")

    data = zip_paths([f]).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert "data.nc" in zf.namelist()
        assert not any("/" in name for name in zf.namelist())


def test_zip_paths_preserve_dir_keeps_path(tmp_path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    f = str(subdir / "data.nc")
    with open(f, "wb") as fh:
        fh.write(b"content")

    data = zip_paths([f], preserve_dir=True).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        # The archive name should be the full path (stripped of leading separator)
        assert any("sub" in name for name in zf.namelist())


# ---------------------------------------------------------------------------
# Unknown extension
# ---------------------------------------------------------------------------


def test_zip_paths_unknown_extension(tmp_path):
    f = _make_file(tmp_path, "data.bin", b"binary data")

    data = zip_paths([f]).read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert "data.bin" in zf.namelist()
        assert zf.read("data.bin") == b"binary data"


def test_zip_paths_no_extension(tmp_path):
    path = str(tmp_path / "noext")
    with open(path, "wb") as fh:
        fh.write(b"no extension")

    data = zip_paths([path]).read()

    assert zipfile.is_zipfile(io.BytesIO(data))


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def test_zip_paths_files_deleted_on_close(tmp_path):
    f1 = _make_file(tmp_path, "a.nc", b"nc data")
    f2 = _make_file(tmp_path, "b.grib", b"grib data")

    stream = zip_paths([f1, f2])
    stream.read()
    stream.close()

    assert not os.path.exists(f1)
    assert not os.path.exists(f2)


def test_zip_paths_files_not_deleted_before_close(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")

    stream = zip_paths([f])
    # Do not read or close — files should still exist
    assert os.path.exists(f)
    stream.close()


# ---------------------------------------------------------------------------
# Stream properties
# ---------------------------------------------------------------------------


def test_zip_paths_stream_is_readable(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")
    stream = zip_paths([f])
    assert stream.readable()
    stream.read()
    stream.close()


def test_zip_paths_stream_is_not_seekable(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")
    stream = zip_paths([f])
    assert not stream.seekable()
    stream.read()
    stream.close()


def test_zip_paths_seek_raises(tmp_path):
    f = _make_file(tmp_path, "data.nc", b"content")
    stream = zip_paths([f])
    with pytest.raises((io.UnsupportedOperation, OSError)):
        stream.seek(0)
    stream.close()

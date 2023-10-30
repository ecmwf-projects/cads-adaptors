

import subprocess
import sys
import time
from cads_adaptors.tools import url_tools
import pytest
import os

@pytest.mark.parametrize(
    "url,expected_nfiles",
    (
        ("https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc", 1),
    ),
)
def test_downloaders(tmp_path, monkeypatch, url, expected_nfiles):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    paths = url_tools.try_download([url])
    assert len(paths)==expected_nfiles


@pytest.fixture()
def ftp(tmp_path):
    ftp_path = tmp_path / "ftp"
    (ftp_path / "test").mkdir(parents=True)
    (ftp_path / "test" / "foo").write_bytes(b"foo")
    P = subprocess.Popen(
        [sys.executable, "-m", "pyftpdlib", "-d", str(ftp_path)],
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )
    try:
        time.sleep(1)
        yield "localhost", 2121
    finally:
        P.terminate()
        P.wait()


def test_try_download_ftp(tmp_path, monkeypatch, ftp):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    host, port = ftp
    url = f"ftp://{host}:{port}/test/foo"

    # Try wget
    subprocess.run(("wget", url, "-O", "foo-wget"), check=True)
    assert (tmp_path / "foo-wget").read_bytes() == b"foo"

    url_tools.try_download([url])
    # TODO: add assertions

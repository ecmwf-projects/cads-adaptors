import subprocess
import sys
import time

import fsspec
import pytest

from cads_adaptors import tools


@pytest.fixture()
def ftp(tmp_path):
    ftp_path = tmp_path / "ftp"
    ftp_path.mkdir()
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


def test_multiurl(tmp_path, monkeypatch, ftp):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir

    host, port = ftp
    fs = fsspec.filesystem("ftp", host=host, port=port)
    fs.pipe("/test-file", b"dummy")

    urls = [f"ftp://{host}:{port}/test-file"]
    tools.url_tools.try_download([urls])
    # TODO: add assertions

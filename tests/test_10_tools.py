import subprocess
import sys
import time

import fsspec
import pytest

from cads_adaptors.tools.url_tools import try_download


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


def test_multiurl(tmp_path, monkeypatch, ftp):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir

    host, port = ftp
    fs = fsspec.filesystem("ftp", host=host, port=port)
    fs.download("test/foo", "foo-fsspec")
    assert (tmp_path / "foo-fsspec").read_bytes() == b"foo"

    url = f"ftp://{host}:{port}/test/foo"
    subprocess.run(("wget", url, "-O", "foo-wget"), check=True)
    assert (tmp_path / "foo-wget").read_bytes() == b"foo"

    try_download([url])
    # TODO: add assertions

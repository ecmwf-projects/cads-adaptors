import pathlib
import subprocess
import sys
import tempfile
import time

import pytest

from cads_adaptors.tools import url_tools


@pytest.fixture(scope="module")
def ftp_base_url():
    with tempfile.TemporaryDirectory() as tmpdir:
        ftp_path = pathlib.Path(tmpdir) / "ftp"
        (ftp_path / "test").mkdir(parents=True)
        (ftp_path / "foo").write_bytes(b"foo")
        (ftp_path / "test" / "bar").write_bytes(b"bar")
        P = subprocess.Popen(
            [sys.executable, "-m", "pyftpdlib", "-d", str(ftp_path), "-p", "2121"],
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )
        time.sleep(1)
        yield "ftp://localhost:2121/"
        P.terminate()
        P.wait()


@pytest.fixture
def http_base_url(httpserver):
    httpserver.expect_request("/foo").respond_with_data(b"foo")
    httpserver.expect_request("/test/bar").respond_with_data(b"bar")
    return httpserver.url_for("/")


@pytest.fixture
def urls(request, http_base_url, ftp_base_url):
    match getattr(request, "param", None):
        case "http":
            base_url = http_base_url
        case "ftp":
            base_url = ftp_base_url
        case param:
            raise ValueError(f"{param=}")
    yield [f"{base_url}foo", f"{base_url}test/bar"]


@pytest.mark.parametrize("urls", ["http", "ftp"], indirect=True)
def test_downloaders(tmp_path, monkeypatch, urls):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    paths = url_tools.try_download(urls)
    assert paths == ["foo", "test/bar"]
    assert (tmp_path / "foo").read_bytes() == b"foo"
    assert (tmp_path / "test" / "bar").read_bytes() == b"bar"

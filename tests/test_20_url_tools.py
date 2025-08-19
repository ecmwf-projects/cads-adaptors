import contextlib
import os
import random
from pathlib import Path
from typing import Any

import pytest
import pytest_httpbin.serve
import requests

from cads_adaptors.adaptors import Context
from cads_adaptors.exceptions import UrlNoDataError
from cads_adaptors.tools import url_tools

does_not_raise = contextlib.nullcontext


@pytest.mark.parametrize(
    "urls,expected_nfiles",
    (
        (
            [
                "https://sites.ecmwf.int/repository/earthkit-data/test-data/test_single.nc"
            ],
            1,
        ),
        (
            [
                "https://sites.ecmwf.int/repository/earthkit-data/test-data/test_single.nc",
                "https://sites.ecmwf.int/repository/earthkit-data/test-data/test_single.grib",
            ],
            2,
        ),
        # Check duplicate URLs are not downloaded twice
        (
            [
                "https://sites.ecmwf.int/repository/earthkit-data/test-data/test_single.nc",
                "https://sites.ecmwf.int/repository/earthkit-data/test-data/test_single.nc",
                "https://sites.ecmwf.int/repository/earthkit-data/test-data/test_single.grib",
            ],
            2,
        ),
    ),
)
def test_downloaders(tmp_path, monkeypatch, urls, expected_nfiles):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    paths = url_tools.try_download(urls, context=Context())
    assert len(paths) == expected_nfiles


@pytest.mark.parametrize(
    "server_suggested_filename,expected",
    [
        (False, "response-headers"),
        (True, "test.txt"),
    ],
)
def test_download_with_server_suggested_filename(
    httpbin: pytest_httpbin.serve.Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    server_suggested_filename: bool,
    expected: str,
) -> None:
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    content_disposition = "attachment;filename=test.txt"
    url = f"{httpbin.url}/response-headers?Content-Disposition={content_disposition}"
    (actual,) = url_tools.try_download(
        [url],
        context=Context(),
        server_suggested_filename=server_suggested_filename,
    )
    assert actual == expected
    assert (tmp_path / actual).exists()


@pytest.mark.parametrize(
    "anon",
    (
        True,
        False,
    ),
)
def test_ftp_download(tmp_path, monkeypatch, ftpserver, anon):
    local_test_file = os.path.join(tmp_path, "testfile.txt")
    with open(local_test_file, "w") as f:
        f.write("This is a test file")

    ftp_url = ftpserver.put_files(local_test_file, style="url", anon=anon)
    work_dir = os.path.join(tmp_path, "work_dir")
    os.makedirs(work_dir)
    monkeypatch.chdir(work_dir)
    local_test_download = url_tools.try_download(ftp_url, context=Context())[0]
    with open(local_test_file) as original, open(local_test_download) as downloaded:
        assert original.read() == downloaded.read()


def test_try_download_skips_404(
    httpbin: pytest_httpbin.serve.Server,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    paths = url_tools.try_download(
        [
            f"{httpbin.url}/status/404",
            f"{httpbin.url}/range/1",
        ],
        Context(),
        maximum_retries=2,
        retry_after=0,
    )
    assert "Recovering from HTTP error" not in caplog.text
    assert paths == ["range/1"]
    assert os.path.getsize("range/1") == 1


def test_try_download_raises_500(
    httpbin: pytest_httpbin.serve.Server,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(UrlNoDataError, match="Incomplete request result."):
        url_tools.try_download(
            [
                f"{httpbin.url}/status/500",
                f"{httpbin.url}/range/1",
            ],
            Context(),
            maximum_retries=2,
            retry_after=0,
        )
    assert (
        "Recovering from HTTP error [500 INTERNAL SERVER ERROR], attempt 1 of 2"
        in caplog.text
    )


def test_try_download_raises_empty(
    httpbin: pytest_httpbin.serve.Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(UrlNoDataError, match="Request empty."):
        url_tools.try_download(
            [f"{httpbin.url}/status/404"],
            Context(),
        )


@pytest.mark.parametrize(
    "maximum_tries,fail_on_timeout_for_any_part,raises",
    [
        (500, True, does_not_raise()),
        (500, False, does_not_raise()),
        (1, True, pytest.raises(UrlNoDataError, match="Incomplete request result.")),
        (1, False, pytest.raises(UrlNoDataError, match="Request empty.")),
    ],
)
def test_try_download_robust_iter_content(
    httpbin: pytest_httpbin.serve.Server,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    maximum_tries: int,
    fail_on_timeout_for_any_part: bool,
    raises: contextlib.nullcontext[Any],
) -> None:
    from multiurl.http import FullHTTPDownloader

    def patched_iter_content(self, *args, **kwargs):  # type: ignore
        for chunk in self.iter_content(chunk_size=1):
            if random.choice([True, False]):
                exception = random.choice(
                    [requests.ReadTimeout, requests.ConnectionError]
                )
                raise exception("Random error.")
            yield chunk

    def make_stream(self):  # type: ignore
        request = self.issue_request(self.range)
        return request.patched_iter_content

    monkeypatch.setattr(
        requests.Response, "patched_iter_content", patched_iter_content, raising=False
    )
    monkeypatch.setattr(FullHTTPDownloader, "make_stream", make_stream)

    monkeypatch.chdir(tmp_path)
    with raises:
        url_tools.try_download(
            [f"{httpbin.url}/range/10"],
            context=Context(),
            maximum_retries=maximum_tries,
            retry_after=0,
            fail_on_timeout_for_any_part=fail_on_timeout_for_any_part,
        )
        assert os.path.getsize("range/10") == 10


def test_try_download_missing_ftp(
    ftpserver: pytest_httpbin.serve.Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_dir = tmp_path / "work_dir"
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)

    test_file = tmp_path / "existing.txt"
    test_file.write_text("This is a test file")
    (existing_url,) = ftpserver.put_files(str(test_file), style="url")
    missing_url = f"{ftpserver.get_login_data(style='url')}/missing.txt"

    paths = url_tools.try_download([existing_url, missing_url], context=Context())
    assert paths == ["existing.txt"]
    assert (work_dir / "existing.txt").read_text() == "This is a test file"

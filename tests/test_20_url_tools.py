import contextlib
import os
import random
from pathlib import Path
from typing import Any

import pytest
import requests

from cads_adaptors.tools import url_tools

does_not_raise = contextlib.nullcontext


@pytest.mark.parametrize(
    "urls,expected_nfiles",
    (
        (
            [
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc"
            ],
            1,
        ),
        (
            [
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc",
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.grib",
            ],
            2,
        ),
        # Check duplicate URLs are not downloaded twice
        (
            [
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc",
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc",
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.grib",
            ],
            2,
        ),
    ),
)
def test_downloaders(tmp_path, monkeypatch, urls, expected_nfiles):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    paths = url_tools.try_download(urls, context=url_tools.Context())
    assert len(paths) == expected_nfiles


def test_download_with_server_suggested_filename(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    urls = ["https://gerb.oma.be/c3s/data/ceres-ebaf/tcdr/v4.2/toa_lw_all_mon/2000/07"]
    paths_false = url_tools.try_download(
        urls, context=url_tools.Context(), server_suggested_filename=False
    )
    assert len(paths_false) == 1
    assert os.path.basename(paths_false[0]) == "07"

    paths_true = url_tools.try_download(
        urls, context=url_tools.Context(), server_suggested_filename=True
    )
    assert len(paths_true) == 1
    assert (
        os.path.basename(paths_true[0])
        == "data_312a_Lot1_ceres-ebaf_tcdr_v4.2_toa_lw_all_mon_2000_07.nc"
    )

    assert os.path.dirname(paths_false[0]) == os.path.dirname(paths_true[0])


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
    local_test_download = url_tools.try_download(ftp_url, context=url_tools.Context())[
        0
    ]
    with open(local_test_file) as original, open(local_test_download) as downloaded:
        assert original.read() == downloaded.read()


def test_try_download_skips_404(caplog, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    context = url_tools.Context()
    paths = url_tools.try_download(
        [
            "http://httpbin.org/status/404",
            "http://httpbin.org/bytes/1",
        ],
        context,
        maximum_tries=2,
        retry_after=0,
    )
    assert "Recovering from HTTP error" not in caplog.text
    assert paths == ["bytes/1"]
    assert os.path.getsize("bytes/1") == 1


def test_try_download_raises_500(caplog, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    context = url_tools.Context()
    with pytest.raises(url_tools.UrlNoDataError, match="Incomplete request result."):
        url_tools.try_download(
            [
                "http://httpbin.org/status/500",
                "http://httpbin.org/bytes/1",
            ],
            context,
            maximum_tries=2,
            retry_after=0,
        )
    assert (
        "Recovering from HTTP error [500 INTERNAL SERVER ERROR], attempt 1 of 2"
        in caplog.text
    )


def test_try_download_raises_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    context = url_tools.Context()
    with pytest.raises(url_tools.UrlNoDataError, match="Request empty."):
        url_tools.try_download(
            ["http://httpbin.org/status/404"],
            context,
        )


@pytest.mark.parametrize(
    "maximum_tries,raises",
    [
        (500, does_not_raise()),
        (
            1,
            pytest.raises(url_tools.UrlNoDataError, match="Incomplete request result."),
        ),
    ],
)
def test_try_download_robust_iter_content(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    maximum_tries: int,
    raises: contextlib.nullcontext[Any],
) -> None:
    from multiurl.http import FullHTTPDownloader

    def patched_iter_content(self, *args, **kwargs):  # type: ignore
        for chunk in self.iter_content(chunk_size=1):
            if random.choice([True, False]):
                raise requests.ConnectionError("Random error.")
            yield chunk

    def make_stream(self):  # type: ignore
        request = self.issue_request(self.range)
        return request.patched_iter_content

    monkeypatch.setattr(
        requests.Response, "patched_iter_content", patched_iter_content, raising=False
    )
    monkeypatch.setattr(FullHTTPDownloader, "make_stream", make_stream)

    monkeypatch.chdir(tmp_path)
    context = url_tools.Context()
    with raises:
        url_tools.try_download(
            ["https://httpbin.org/range/10"],
            context,
            maximum_tries=maximum_tries,
            retry_after=0,
        )
        assert os.path.getsize("range/10") == 10

import os

import pytest

from cads_adaptors.tools import url_tools


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
    ),
)
def test_downloaders(tmp_path, monkeypatch, urls, expected_nfiles):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    paths = url_tools.try_download(urls)
    assert len(paths) == expected_nfiles


def test_ftp_download(tmp_path, ftpserver):
    local_test_file = os.path.join(tmp_path, "testfile.txt")
    with open(local_test_file, "w") as f:
        f.write("This is a test file")

    ftp_url = ftpserver.put_files(local_test_file, style="url", anon=True)
    local_test_download = url_tools.try_download(ftp_url)[0]
    with open(local_test_file) as original, open(local_test_download) as downloaded:
        assert original.read() == downloaded.read()

    ftp_url = ftpserver.put_files(local_test_file, style="url", anon=False)
    local_test_download = url_tools.try_download(ftp_url)[0]
    with open(local_test_file) as original, open(local_test_download) as downloaded:
        assert original.read() == downloaded.read()

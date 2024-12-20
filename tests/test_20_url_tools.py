import os
import tempfile

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
def test_downloaders(urls, expected_nfiles):
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        paths = url_tools.try_download(urls, context=url_tools.Context())
        assert len(paths) == expected_nfiles


def test_download_with_server_suggested_filename():
    urls = ["https://gerb.oma.be/c3s/data/ceres-ebaf/tcdr/v4.2/toa_lw_all_mon/2000/07"]
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
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
def test_ftp_download(tmp_path, ftpserver, anon):
    local_test_file = os.path.join(tmp_path, "testfile.txt")
    with open(local_test_file, "w") as f:
        f.write("This is a test file")

    ftp_url = ftpserver.put_files(local_test_file, style="url", anon=anon)
    work_dir = os.path.join(tmp_path, "work_dir")
    os.makedirs(work_dir)
    os.chdir(work_dir)
    local_test_download = url_tools.try_download(ftp_url, context=url_tools.Context())[
        0
    ]
    with open(local_test_file) as original, open(local_test_download) as downloaded:
        assert original.read() == downloaded.read()

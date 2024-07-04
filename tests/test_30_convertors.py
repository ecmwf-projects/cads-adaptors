import os
import tempfile

import pytest
import requests

from cads_adaptors.tools import convertors

TEST_GRIB_FILE = (
    "https://get.ecmwf.int/repository/test-data/cfgrib/era5-levels-members.grib"
)
TEST_NC_FILE = (
    "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc"
)


def test_open_grib():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_grib_file = os.path.join(tmpdirname, "test.grib")
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        xarray_dict = convertors.open_grib_file_as_xarray_dictionary(tmp_grib_file)
        assert isinstance(xarray_dict, dict)
        assert len(xarray_dict) == 1
        assert list(xarray_dict)[0] == "test_0"


def test_open_grib_open_ds_kwargs():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_grib_file = os.path.join(tmpdirname, "test.grib")
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        xarray_dict = convertors.open_grib_file_as_xarray_dictionary(
            tmp_grib_file, open_datasets_kwargs={"tag": "tag"}
        )
        assert isinstance(xarray_dict, dict)
        assert len(xarray_dict) == 1
        assert list(xarray_dict)[0] == "test_tag"


def test_open_grib_open_ds_kwargs_list():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_grib_file = os.path.join(tmpdirname, "test.grib")
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        xarray_dict = convertors.open_grib_file_as_xarray_dictionary(
            tmp_grib_file, open_datasets_kwargs=[{"tag": "tag1"}, {"tag": "tag2"}]
        )
        assert isinstance(xarray_dict, dict)
        assert len(xarray_dict) == 2
        assert list(xarray_dict)[0] == "test_tag1"
        assert list(xarray_dict)[1] == "test_tag2"


def test_open_netcdf():
    netcdf_file = requests.get(TEST_NC_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_netcdf_file = os.path.join(tmpdirname, "test.nc")
        with open(tmp_netcdf_file, "wb") as f:
            f.write(netcdf_file.content)

        xarray_dict = convertors.open_netcdf_as_xarray_dictionary(tmp_netcdf_file)
        assert isinstance(xarray_dict, dict)
        assert len(xarray_dict) == 1
        assert list(xarray_dict)[0] == "test"


def test_open_file_as_xarray_dictionary():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_grib_file = "test.grib"
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        xarray_dict = convertors.open_file_as_xarray_dictionary(
            tmp_grib_file, open_datasets_kwargs={"tag": "tag"}
        )
        assert isinstance(xarray_dict, dict)
        assert len(xarray_dict) == 1
        assert list(xarray_dict)[0] == "test_tag"

        xarray_dict = convertors.open_file_as_xarray_dictionary(
            tmp_grib_file, open_datasets_kwargs=[{"tag": "tag1"}, {"tag": "tag2"}]
        )
        assert isinstance(xarray_dict, dict)
        assert len(xarray_dict) == 2
        assert list(xarray_dict)[0] == "test_tag1"
        assert list(xarray_dict)[1] == "test_tag2"


def test_grib_to_netcdf():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_grib_file = "test.grib"
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        netcdf_files = convertors.grib_to_netcdf_files(tmp_grib_file)
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

        netcdf_files = convertors.grib_to_netcdf_files(
            tmp_grib_file, compression_options="default"
        )
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

        netcdf_files = convertors.grib_to_netcdf_files(
            tmp_grib_file, open_datasets_kwargs={"chunks": {"time": 1}}
        )
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

        netcdf_files = convertors.grib_to_netcdf_files(
            tmp_grib_file, encoding={"time": {"dtype": "int64"}}
        )
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1


EXTENSION_MAPPING = {
    "grib": ".grib",
    "netcdf": ".nc",
}


@pytest.mark.parametrize("url", [TEST_GRIB_FILE, TEST_NC_FILE])
def test_convert_format_to_netcdf(url, target_format="netcdf"):
    remote_file = requests.get(url)
    _, ext = os.path.splitext(url)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_file = f"test.{ext}"
        with open(tmp_file, "wb") as f:
            f.write(remote_file.content)

        converted_files = convertors.convert_format(
            tmp_file, target_format=target_format
        )
        assert isinstance(converted_files, list)
        assert len(converted_files) == 1
        _, out_ext = os.path.splitext(converted_files[0])
        assert out_ext == EXTENSION_MAPPING.get(target_format, f".{target_format}")


@pytest.mark.parametrize("url", [TEST_GRIB_FILE, TEST_NC_FILE])
def test_convert_format_to_grib(url, target_format="grib"):
    remote_file = requests.get(url)
    _, ext = os.path.splitext(url)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_file = f"test.{ext}"
        with open(tmp_file, "wb") as f:
            f.write(remote_file.content)

        converted_files = convertors.convert_format(
            tmp_file, target_format=target_format
        )
        assert isinstance(converted_files, list)
        assert len(converted_files) == 1
        # Can't convert from netcdf to grib yet, so ensure in extension is the same as input
        _, out_ext = os.path.splitext(converted_files[0])
        assert out_ext == ext

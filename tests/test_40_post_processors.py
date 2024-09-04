import os
import tempfile

import pytest
import requests
import xarray as xr

from cads_adaptors.tools import convertors, post_processors

TEST_FILE_1 = (
    "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/"
    "era5_temperature_france_2015_2016_2017_3deg.grib"
)


@pytest.mark.parametrize(
    "in_mapping, out_mapping",
    [
        ({"method": "daily_mean"}, {"method": "daily_reduce", "how": "mean"}),
        ({"method": "daily_std"}, {"method": "daily_reduce", "how": "std"}),
        ({"method": "monthly_median"}, {"method": "monthly_reduce", "how": "median"}),
        ({"method": "monthly_max"}, {"method": "monthly_reduce", "how": "max"}),
    ],
)
def test_pp_config_mapping(in_mapping, out_mapping):
    assert post_processors.pp_config_mapping(in_mapping) == out_mapping


def test_daily_reduce():
    remote_file = requests.get(TEST_FILE_1)
    _, ext = os.path.splitext(TEST_FILE_1)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_file = f"test{ext}"
        with open(tmp_file, "wb") as f:
            f.write(remote_file.content)

        xarray_dict = convertors.open_grib_file_as_xarray_dictionary(tmp_file)

        out_xarray_dict = post_processors.daily_reduce(xarray_dict, how="mean")
        assert isinstance(out_xarray_dict, dict)
        assert len(out_xarray_dict) == 1
        assert list(out_xarray_dict)[0] == "test_0_daily-mean"
        assert isinstance(out_xarray_dict["test_0_daily-mean"], xr.Dataset)
        assert isinstance(out_xarray_dict["test_0_daily-mean"].attrs["history"], str)


def test_monthly_reduce():
    remote_file = requests.get(TEST_FILE_1)
    _, ext = os.path.splitext(TEST_FILE_1)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_file = f"test{ext}"
        with open(tmp_file, "wb") as f:
            f.write(remote_file.content)

        xarray_dict = convertors.open_grib_file_as_xarray_dictionary(tmp_file)

        out_xarray_dict = post_processors.monthly_reduce(xarray_dict, how="mean")
        assert isinstance(out_xarray_dict, dict)
        assert len(out_xarray_dict) == 1
        assert list(out_xarray_dict)[0] == "test_0_monthly-mean"
        assert isinstance(out_xarray_dict["test_0_monthly-mean"], xr.Dataset)
        assert isinstance(out_xarray_dict["test_0_monthly-mean"].attrs["history"], str)


def test_update_history():
    in_xarray = xr.Dataset(
        {
            "temperature": xr.DataArray([1, 2, 3]),
        }
    )
    # Check that history is added to an xarray which does not haev history
    assert "history" not in in_xarray.attrs
    out_xarray = post_processors.update_history(in_xarray, "Test history create")
    assert isinstance(out_xarray, xr.Dataset)
    assert out_xarray.attrs["history"] == "Test history create"

    # Check that history is updated in an xarray which already has history
    out_xarray = post_processors.update_history(out_xarray, "Test history update")
    assert isinstance(out_xarray, xr.Dataset)
    assert out_xarray.attrs["history"] == "Test history create\nTest history update"

    # Check that warning raised, but no fail if history is not a string
    out_xarray.attrs["history"] = 1
    out_xarray = post_processors.update_history(out_xarray, "Test history update")
    assert isinstance(out_xarray, xr.Dataset)
    assert out_xarray.attrs["history"] == 1

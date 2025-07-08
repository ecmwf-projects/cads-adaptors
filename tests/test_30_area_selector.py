import os
import tempfile

import numpy as np
import pytest
import requests
import xarray as xr

from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools.area_selector import (
    area_selector,
    area_selector_path,
    area_selector_paths,
    get_dim_slices,
    wrap_longitudes,
)


# class TestWrapLongitudes(unittest.TestCase):
def test_wrap_longitudes():
    dim_key = "test_dim"
    start = 20
    end = 40
    coord_range = [0, 360]

    result = wrap_longitudes(dim_key, start, end, coord_range)
    assert result == [slice(start, end)]

    start = -40
    end = -20
    result = wrap_longitudes(dim_key, start, end, coord_range)
    assert result == [slice(start + 360, end + 360)]

    start = 440
    end = 420
    result = wrap_longitudes(dim_key, start, end, coord_range)
    assert result == [slice(start - 360, end - 360)]

    start = -10
    end = 30
    result = wrap_longitudes(dim_key, start, end, coord_range)
    assert result == [slice(start + 360, coord_range[-1]), slice(coord_range[0], end)]


def test_wrap_longitudes_oob():
    dim_key = "test_dim"
    start = -80
    end = -40
    coord_range = [-60, -20]

    result = wrap_longitudes(dim_key, start, end, coord_range)
    assert result == [slice(-80, -40)]

    start = 280
    end = 320
    result = wrap_longitudes(dim_key, start, end, coord_range)
    assert result == [slice(-80, -40)]


TEST_DATA_1 = {
    "temperature": (
        ("time", "latitude", "longitude"),
        [[[1, 2, 3, 4, 5, 6]] * 6] * 6,
    )
}
TEST_COORDS_1 = {
    "time": [1, 2, 3, 4, 5, 6],
    "latitude": [-75, -45, -15, 15, 45, 75],
    "longitude": [30, 90, 150, 210, 270, 330],
}
TEST_DS_1 = xr.Dataset(TEST_DATA_1, coords=TEST_COORDS_1)


def test_get_dim_slices_within_limits():
    result = get_dim_slices(TEST_DS_1, "latitude", -90, -40)
    assert result == [slice(-90, -40)]

    result = get_dim_slices(TEST_DS_1, "longitude", 10, 360, longitude=True)
    assert result == [slice(10, 360)]


def test_get_dim_slices_wrap_longitudes():
    result = get_dim_slices(TEST_DS_1, "longitude", 300, 370, longitude=True)
    assert result == [slice(300, 360), slice(0, 10)]


def test_get_dim_slices_incompatible_area():
    with pytest.raises(InvalidRequest):
        get_dim_slices(TEST_DS_1, "latitude", -100, -91)


def test_get_dim_slices_descending():
    coords_desc = {
        "time": [1, 2, 3, 4, 5, 6],
        "latitude": [-75, -45, -15, 15, 45, 75][::-1],
        "longitude": [30, 90, 150, 210, 270, 330][::-1],
    }
    ds_desc = xr.Dataset(TEST_DATA_1, coords=coords_desc)

    result = get_dim_slices(ds_desc, "latitude", -90, -40)
    assert result == [slice(-40, -90)]


def test_get_dim_slices_outside_limits():
    coords_desc = {
        "time": [1, 2, 3, 4, 5, 6],
        "latitude": [-75, -45, -15, 15, 45, 75],
        "longitude": [90, 120, 150, 180, 210, 240],
    }
    ds_desc = xr.Dataset(TEST_DATA_1, coords=coords_desc)
    result = get_dim_slices(ds_desc, "longitude", 10, 360, longitude=True)
    assert result == [slice(10, 360)]


TEST_COORDS_2 = {
    "time": np.arange(5),
    "latitude": np.arange(-89.5, 90, 1),
    "longitude": np.arange(-179.5, 180, 1),
}
TEST_DATA_2 = {
    "temperature": (
        ("time", "latitude", "longitude"),
        np.random.rand(5, 180, 360),
    )
}
TEST_DS_2 = xr.Dataset(TEST_DATA_2, coords=TEST_COORDS_2)
TEMP_FILENAME = "example_data.nc"


TEST_COORDS_3 = {
    "time": np.arange(5),
    "latitude": np.arange(30.5, 70, 1),
    "longitude": np.arange(-20.5, 40, 1),
}
TEST_DATA_3 = {
    "temperature": (
        ("time", "latitude", "longitude"),
        np.random.rand(5, 40, 61),
    )
}
TEST_DS_3 = xr.Dataset(TEST_DATA_3, coords=TEST_COORDS_3)


@pytest.mark.parametrize(
    "ds, area, test_result",
    [
        # FULL DOMAIN:
        (TEST_DS_2, [90, -180, -90, 180], TEST_DS_2),
        # SUB DOMAIN:
        (
            TEST_DS_2,
            [10, -10, -10, 10],
            TEST_DS_2.sel(latitude=slice(-10, 10), longitude=slice(-10, 10)),
        ),
        # SUB DOMAIN,  mixed up lats:
        (
            TEST_DS_2,
            [-10, -10, 10, 10],
            TEST_DS_2.sel(latitude=slice(-10, 10), longitude=slice(-10, 10)),
        ),
        # partially OOB lons:
        (
            TEST_DS_3,
            [50, -40, 30, 10],
            TEST_DS_3.sel(latitude=slice(30, 50), longitude=slice(-40, 10)),
        ),
        # partially OOB lats:
        (
            TEST_DS_3,
            [50, -10, 20, 10],
            TEST_DS_3.sel(latitude=slice(20, 50), longitude=slice(-10, 10)),
        ),
    ],
)
def test_area_selector_regular(ds, area, test_result):
    result = area_selector(ds, area=area)

    assert isinstance(result, xr.Dataset)
    assert result.dims == test_result.dims
    assert np.allclose(result.latitude.values, test_result.latitude.values)
    assert np.allclose(result.longitude.values, test_result.longitude.values)


@pytest.mark.parametrize(
    "ds, area, test_result",
    [
        # FULL DOMAIN:
        (TEST_DS_2, [90, -180, -90, 180], TEST_DS_2),
        # SUB DOMAIN:
        (
            TEST_DS_2,
            [10, -10, -10, 10],
            TEST_DS_2.sel(latitude=slice(-10, 10), longitude=slice(-10, 10)),
        ),
        # SUB DOMAIN,  mixed up lats:
        (
            TEST_DS_2,
            [-10, -10, 10, 10],
            TEST_DS_2.sel(latitude=slice(-10, 10), longitude=slice(-10, 10)),
        ),
        # partially OOB lons:
        (
            TEST_DS_3,
            [50, -40, 30, 10],
            TEST_DS_3.sel(latitude=slice(30, 50), longitude=slice(-40, 10)),
        ),
        # partially OOB lats:
        (
            TEST_DS_3,
            [50, -10, 20, 10],
            TEST_DS_3.sel(latitude=slice(20, 50), longitude=slice(-10, 10)),
        ),
    ],
)
def test_area_selector_path_regular(ds, area, test_result):
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, TEMP_FILENAME)
        ds.to_netcdf(test_file)
        result = area_selector_path(test_file, area=area)
        result_xr = xr.open_dataset(result[0])
        assert isinstance(result_xr, xr.Dataset)
        assert result_xr.dims == test_result.dims
        assert np.allclose(result_xr.latitude.values, test_result.latitude.values)
        assert np.allclose(result_xr.longitude.values, test_result.longitude.values)


@pytest.mark.parametrize("precompute", [True, False])
def test_area_selector_precompute(precompute):
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, TEMP_FILENAME)
        TEST_DS_2.to_netcdf(test_file)
        area = [80, -170, -80, 170]
        test_result = TEST_DS_2.sel(latitude=slice(-80, 80), longitude=slice(-170, 170))
        result = area_selector_path(
            test_file, area=area, area_selector_kwargs={"precompute": precompute}
        )
        result_xr = xr.open_dataset(result[0])
        assert isinstance(result_xr, xr.Dataset)
        assert result_xr.dims == test_result.dims
        assert np.allclose(result_xr.latitude.values, test_result.latitude.values)
        assert np.allclose(result_xr.longitude.values, test_result.longitude.values)


def test_area_selector_fully_oob():
    ds = TEST_DS_3
    area = [20, -40, 10, -30]
    with pytest.raises(InvalidRequest):
        area_selector(ds, area=area)
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, TEMP_FILENAME)
        ds.to_netcdf(test_file)
        with pytest.raises(InvalidRequest):
            area_selector_path(test_file, area=area)


# Test that InvalidRequest exception raised when a dimension has zero length
def test_area_selector_zero_length_dim():
    with pytest.raises(InvalidRequest):
        area_selector(TEST_DS_3, area=[50.4, -10.6, 50.3, -10.5])
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, TEMP_FILENAME)
        TEST_DS_3.to_netcdf(test_file)
        with pytest.raises(InvalidRequest):
            area_selector_path(test_file, area=[50.4, -10.6, 50.3, -10.5])


TEST_DATA_BASE_URL = (
    "https://get.ecmwf.int/repository/test-data/test-data/cads-adaptors/"
)


@pytest.mark.parametrize(
    "url",
    [
        f"{TEST_DATA_BASE_URL}/CAMS-GLOB-AIR_Glb_0.5x0.5_anthro_voc25_v1.1_2012.nc",
        f"{TEST_DATA_BASE_URL}/C3S-312bL1-L3C-MONTHLY-SRB-ATSR2_ORAC_ERS2_199506_fv3.0.nc",
    ],
)
def test_area_selector_real_files(url):
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, TEMP_FILENAME)
        remote_file = requests.get(url)
        with open(test_file, "wb") as f:
            f.write(remote_file.content)

        result = area_selector_path(test_file, area=[90, -180, -90, 180])
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], str)


# Test with lists of urls
@pytest.mark.parametrize(
    "urls",
    [
        [
            f"{TEST_DATA_BASE_URL}/CAMS-GLOB-AIR_Glb_0.5x0.5_anthro_voc25_v1.1_2012.nc",
        ],
        [
            f"{TEST_DATA_BASE_URL}/CAMS-GLOB-AIR_Glb_0.5x0.5_anthro_voc25_v1.1_2012.nc",
            f"{TEST_DATA_BASE_URL}/CAMS-GLOB-AIR_Glb_0.5x0.5_anthro_voc25_v1.1_2013.nc",
            f"{TEST_DATA_BASE_URL}/CAMS-GLOB-AIR_Glb_0.5x0.5_anthro_voc25_v1.1_2014.nc",
        ],
        [
            # Mixed bag of files and formats
            f"{TEST_DATA_BASE_URL}/CAMS-GLOB-AIR_Glb_0.5x0.5_anthro_voc25_v1.1_2012.nc",
            f"{TEST_DATA_BASE_URL}/CAMS-GLOB-BIO_v1.1_carbon-monoxide_2014.nc",
            f"{TEST_DATA_BASE_URL}/C3S-312bL1-L3C-MONTHLY-SRB-ATSR2_ORAC_ERS2_199506_fv3.0.nc",
        ],
    ],
)
def test_area_selector_paths_real_files(urls):
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = []
        for i, file in enumerate(urls):
            remote_file = requests.get(file)
            test_file = os.path.join(temp_dir, f"test-{i}.nc")
            print(test_file)
            with open(test_file, "wb") as f:
                f.write(remote_file.content)
            test_files.append(test_file)

        result = area_selector_paths(
            test_files,
            area=[90, -180, -90, 180],
        )
        assert isinstance(result, list)
        assert len(result) == len(urls)
        assert all(isinstance(r, str) for r in result)

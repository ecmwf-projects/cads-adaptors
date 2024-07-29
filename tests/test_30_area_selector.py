import os
import tempfile
import unittest

import numpy as np
import xarray as xr

from cads_adaptors.exceptions import PostProcessingError
from cads_adaptors.tools.area_selector import (
    area_selector,
    get_dim_slices,
    wrap_longitudes,
)


class TestWrapLongitudes(unittest.TestCase):
    def test_wrap_longitudes(self):
        dim_key = "test_dim"
        start = 20
        end = 40
        coord_range = [0, 360]

        result = wrap_longitudes(dim_key, start, end, coord_range)
        self.assertEqual(result, [slice(start, end)])

        start = -40
        end = -20
        result = wrap_longitudes(dim_key, start, end, coord_range)
        self.assertEqual(result, [slice(start + 360, end + 360)])

        start = 440
        end = 420
        result = wrap_longitudes(dim_key, start, end, coord_range)
        self.assertEqual(result, [slice(start - 360, end - 360)])

        start = -10
        end = 30
        result = wrap_longitudes(dim_key, start, end, coord_range)
        self.assertEqual(
            result, [slice(start + 360, coord_range[-1]), slice(coord_range[0], end)]
        )

    def test_wrap_longitudes_oob(self):
        dim_key = "test_dim"
        start = -80
        end = -40
        coord_range = [-60, -20]

        result = wrap_longitudes(dim_key, start, end, coord_range)
        self.assertEqual(result, [slice(-80, -40)])

        start = 280
        end = 320
        result = wrap_longitudes(dim_key, start, end, coord_range)
        self.assertEqual(result, [slice(-80, -40)])


class TestGetDimSlices(unittest.TestCase):
    def setUp(self):
        # Create a sample xarray dataset for testing
        self.coords = {
            "time": [1, 2, 3, 4, 5, 6],
            "latitude": [-75, -45, -15, 15, 45, 75],
            "longitude": [30, 90, 150, 210, 270, 330],
        }
        self.data = {
            "temperature": (
                ("time", "latitude", "longitude"),
                [[[1, 2, 3, 4, 5, 6]] * 6] * 6,
            )
        }
        self.ds = xr.Dataset(self.data, coords=self.coords)

    def test_get_dim_slices_within_limits(self):
        result = get_dim_slices(self.ds, "latitude", -90, -40)
        self.assertEqual(result, [slice(-90, -40)])

        result = get_dim_slices(self.ds, "longitude", 10, 360, longitude=True)
        self.assertEqual(result, [slice(10, 360)])

    def test_get_dim_slices_wrap_longitudes(self):
        result = get_dim_slices(self.ds, "longitude", 300, 370, longitude=True)
        self.assertEqual(result, [slice(300, 360), slice(0, 10)])

    def test_get_dim_slices_incompatible_area(self):
        with self.assertRaises(PostProcessingError):
            get_dim_slices(self.ds, "latitude", -100, -91)

    def test_get_dim_slices_descending(self):
        coords_desc = {
            "time": [1, 2, 3, 4, 5, 6],
            "latitude": [-75, -45, -15, 15, 45, 75][::-1],
            "longitude": [30, 90, 150, 210, 270, 330][::-1],
        }
        ds_desc = xr.Dataset(self.data, coords=coords_desc)

        result = get_dim_slices(ds_desc, "latitude", -90, -40)
        self.assertEqual(result, [slice(-40, -90)])

    def test_get_dim_slices_outside_limits(self):
        coords_desc = {
            "time": [1, 2, 3, 4, 5, 6],
            "latitude": [-75, -45, -15, 15, 45, 75],
            "longitude": [90, 120, 150, 180, 210, 240],
        }
        ds_desc = xr.Dataset(self.data, coords=coords_desc)
        result = get_dim_slices(ds_desc, "longitude", 10, 360, longitude=True)
        self.assertEqual(result, [slice(10, 360)])


class TestAreaSelector(unittest.TestCase):
    def setUp(self):
        # Create a sample xarray dataset for testing
        self.coords = {
            "time": np.arange(5),
            "latitude": np.arange(-89.5, 90, 1),
            "longitude": np.arange(-179.5, 180, 1),
        }
        self.data = {
            "temperature": (
                ("time", "latitude", "longitude"),
                np.random.rand(5, 180, 360),
            )
        }
        self.ds = xr.Dataset(self.data, coords=self.coords)
        self.input_file = "example_data.nc"

    def test_area_selector_regular_full_domain(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = os.path.join(temp_dir, self.input_file)
            self.ds.to_netcdf(test_file)
            result = area_selector(test_file, area=[90, -180, -90, 180])
            self.assertIsInstance(result, xr.Dataset)
            self.assertEqual(result.dims, self.ds.dims)
            self.assertTrue(
                np.allclose(result.latitude.values, self.ds.latitude.values)
            )
            self.assertTrue(
                np.allclose(result.longitude.values, self.ds.longitude.values)
            )

    def test_area_selector_regular_sub_domain(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = os.path.join(temp_dir, self.input_file)
            self.ds.to_netcdf(test_file)
            result = area_selector(test_file, area=[10, -10, -10, 10])
            test_result = self.ds.sel(latitude=slice(-10, 10), longitude=slice(-10, 10))
            self.assertIsInstance(result, xr.Dataset)
            self.assertEqual(result.dims, test_result.dims)
            self.assertTrue(
                np.allclose(result.latitude.values, test_result.latitude.values)
            )
            self.assertTrue(
                np.allclose(result.longitude.values, test_result.longitude.values)
            )


class TestAreaSelectorOOB(unittest.TestCase):
    def setUp(self):
        # Create a sample xarray dataset for testing
        self.coords = {
            "time": np.arange(5),
            "latitude": np.arange(30.5, 70, 1),
            "longitude": np.arange(-20.5, 40, 1),
        }
        self.data = {
            "temperature": (
                ("time", "latitude", "longitude"),
                np.random.rand(5, 40, 61),
            )
        }
        self.ds = xr.Dataset(self.data, coords=self.coords)
        self.input_file = "example_data.nc"

    def test_area_selector_fully_oob(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = os.path.join(temp_dir, self.input_file)
            self.ds.to_netcdf(test_file)
            result = area_selector(test_file, area=[90, -180, -90, 180])
            self.assertIsInstance(result, xr.Dataset)
            self.assertEqual(result.dims, self.ds.dims)
            self.assertTrue(
                np.allclose(result.latitude.values, self.ds.latitude.values)
            )
            self.assertTrue(
                np.allclose(result.longitude.values, self.ds.longitude.values)
            )

    def test_area_selector_regular_partially_oob_lons(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = os.path.join(temp_dir, self.input_file)
            self.ds.to_netcdf(test_file)
            result = area_selector(test_file, area=[50, -40, 30, 10])
            test_result = self.ds.sel(latitude=slice(30, 50), longitude=slice(-40, 10))
            self.assertIsInstance(result, xr.Dataset)
            self.assertEqual(result.dims, test_result.dims)
            self.assertTrue(
                np.allclose(result.latitude.values, test_result.latitude.values)
            )
            self.assertTrue(
                np.allclose(result.longitude.values, test_result.longitude.values)
            )

    def test_area_selector_regular_partially_oob_lats(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = os.path.join(temp_dir, self.input_file)
            self.ds.to_netcdf(test_file)
            result = area_selector(test_file, area=[50, -10, 20, 10])
            test_result = self.ds.sel(latitude=slice(20, 50), longitude=slice(-10, 10))
            self.assertIsInstance(result, xr.Dataset)
            self.assertEqual(result.dims, test_result.dims)
            self.assertTrue(
                np.allclose(result.latitude.values, test_result.latitude.values)
            )
            self.assertTrue(
                np.allclose(result.longitude.values, test_result.longitude.values)
            )


if __name__ == "__main__":
    unittest.main()

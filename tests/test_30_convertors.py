import os
import tempfile

import pytest
import requests

from cads_adaptors.tools import convertors

# GRIB file with multiple level types
TEST_GRIB_FILE = (
    "https://get.ecmwf.int/repository/test-data/cfgrib/era5-levels-members.grib"
)
# GRIB file with multiple expver and stream values
TEST_GRIB_FILE_2 = "https://get.ecmwf.int/repository/test-data/cfgrib/era5-2t-tp-mwp-multi-expver-and-stream.grib"
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
    "netcdf_legacy": ".nc",
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


def test_convert_format_to_netcdf_legacy(
    url=TEST_GRIB_FILE, target_format="netcdf_legacy"
):
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


def test_safely_rename_variable():
    import xarray as xr

    ds = xr.Dataset(
        {
            "time": xr.DataArray([1, 2, 3], dims="time"),
            "temperature": xr.DataArray([1, 2, 3], dims="time"),
            "humidity": xr.DataArray([4, 5, 6], dims="time"),
        }
    )

    ds_1 = convertors.safely_rename_variable(ds, {"temperature": "temp"})
    assert "temperature" not in ds_1
    assert "temp" in ds_1

    ds_2 = convertors.safely_rename_variable(ds, {"time": "valid_time"})
    assert "time" not in ds_2
    assert "valid_time" in ds_2


def test_safely_expand_dims():
    import xarray as xr

    ds = xr.Dataset(
        {
            "temperature": xr.DataArray([1, 2, 3], dims="time"),
        },
        coords={
            "lat": xr.DataArray(1),
            "lon": xr.DataArray(2),
            "time": xr.DataArray([1, 2, 3], dims="time"),
        },
    )
    assert "lat" not in ds.dims
    assert "lon" not in ds.dims
    assert "time" in ds.dims
    assert "lat" not in ds.temperature.dims
    assert "lat" not in ds.temperature.dims
    assert "time" in ds.temperature.dims

    ds_1 = convertors.safely_expand_dims(ds, ["lat", "lon"])
    assert "lat" in ds_1.dims
    assert "lon" in ds_1.dims
    assert "time" in ds_1.dims
    assert "lat" in ds_1.temperature.dims
    assert "lat" in ds_1.temperature.dims
    assert "time" in ds_1.temperature.dims


def test_prepare_open_datasets_kwargs_grib_split_on():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_grib_file = "test.grib"
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        open_ds_kwargs = {
            "test_kwarg": 1,
            "tag": "tag",
            "split_on": ["paramId"],
        }
        new_open_ds_kwargs = convertors.prepare_open_datasets_kwargs_grib(
            tmp_grib_file, open_ds_kwargs
        )
        assert isinstance(new_open_ds_kwargs, list)
        assert len(new_open_ds_kwargs) == 2
        assert "tag_paramId-130" in [d["tag"] for d in new_open_ds_kwargs]
        assert "tag_paramId-129" in [d["tag"] for d in new_open_ds_kwargs]
        assert not any("split_on" in d for d in new_open_ds_kwargs)
        assert all("test_kwarg" in d for d in new_open_ds_kwargs)

        # Single value to split on
        open_ds_kwargs = {
            "test_kwarg": 1,
            "tag": "tag",
            "split_on": ["stream"],
        }
        new_open_ds_kwargs = convertors.prepare_open_datasets_kwargs_grib(
            tmp_grib_file, open_ds_kwargs
        )
        assert isinstance(new_open_ds_kwargs, list)
        assert len(new_open_ds_kwargs) == 1
        assert "tag_stream-enda" in [d["tag"] for d in new_open_ds_kwargs]
        assert not any("split_on" in d for d in new_open_ds_kwargs)
        assert all("test_kwarg" in d for d in new_open_ds_kwargs)

        # Key does not exist
        open_ds_kwargs = {
            "test_kwarg": 1,
            "tag": "tag",
            "split_on": ["kebab"],
        }
        new_open_ds_kwargs = convertors.prepare_open_datasets_kwargs_grib(
            tmp_grib_file, open_ds_kwargs
        )
        assert isinstance(new_open_ds_kwargs, list)
        assert len(new_open_ds_kwargs) == 1
        assert "tag_kebab-None" in [d["tag"] for d in new_open_ds_kwargs]
        assert not any("split_on" in d for d in new_open_ds_kwargs)
        assert all("test_kwarg" in d for d in new_open_ds_kwargs)


def test_prepare_open_datasets_kwargs_grib_split_on_alias():
    # Test split_on_alias, if differences detected in k, then split on v
    grib_file_2 = requests.get(TEST_GRIB_FILE_2)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
        tmp_grib_file = "test2.grib"
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file_2.content)

        open_ds_kwargs = {
            "test_kwarg": 1,
            "tag": "tag",
            "split_on_alias": {"expver": "stepType"},
        }
        new_open_ds_kwargs = convertors.prepare_open_datasets_kwargs_grib(
            tmp_grib_file, open_ds_kwargs
        )
        assert isinstance(new_open_ds_kwargs, list)
        assert len(new_open_ds_kwargs) == 2
        assert "tag_stepType-instant" in [d["tag"] for d in new_open_ds_kwargs]
        assert "tag_stepType-accum" in [d["tag"] for d in new_open_ds_kwargs]
        assert not any("split_on_alias" in d for d in new_open_ds_kwargs)
        assert all("test_kwarg" in d for d in new_open_ds_kwargs)

        # Single k1 value
        open_ds_kwargs = {
            "test_kwarg": 1,
            "tag": "tag",
            # "split_on": ["origin"],
            "split_on_alias": {"origin": "stepType"},
        }
        new_open_ds_kwargs = convertors.prepare_open_datasets_kwargs_grib(
            tmp_grib_file, open_ds_kwargs
        )
        assert isinstance(new_open_ds_kwargs, list)
        assert len(new_open_ds_kwargs) == 1
        assert "tag" in [d["tag"] for d in new_open_ds_kwargs]
        assert not any("split_on_alias" in d for d in new_open_ds_kwargs)
        assert all("test_kwarg" in d for d in new_open_ds_kwargs)

        # Combined split_on and split_on_alias
        open_ds_kwargs = {
            "test_kwarg": 1,
            "tag": "tag",
            "split_on": ["stream"],
            "split_on_alias": {"expver": "paramId"},
        }
        new_open_ds_kwargs = convertors.prepare_open_datasets_kwargs_grib(
            tmp_grib_file, open_ds_kwargs
        )
        assert isinstance(new_open_ds_kwargs, list)
        assert len(new_open_ds_kwargs) == 6
        for tag in [
            "tag_stream-oper_paramId-167",
            "tag_stream-oper_paramId-140232",
            "tag_stream-oper_paramId-228",
            "tag_stream-wave_paramId-167",
            "tag_stream-wave_paramId-140232",
            "tag_stream-wave_paramId-228",
        ]:
            assert tag in [d["tag"] for d in new_open_ds_kwargs]
        assert not any("split_on_alias" in d for d in new_open_ds_kwargs)
        assert all("test_kwarg" in d for d in new_open_ds_kwargs)

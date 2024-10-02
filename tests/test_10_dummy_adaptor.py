import os
import pathlib
import zipfile

import cads_adaptors


def test_dummy_adaptor_cache_tmp_path(tmp_path: pathlib.Path) -> None:
    dummy_adaptor = cads_adaptors.DummyAdaptor(None, cache_tmp_path=tmp_path)
    result = dummy_adaptor.retrieve({"size": 1})
    assert result.name == str(tmp_path / "dummy.grib")
    assert os.path.getsize(result.name) == 1


def test_dummy_adaptor_netcdf(tmp_path: pathlib.Path) -> None:
    dummy_adaptor = cads_adaptors.DummyAdaptor(None, cache_tmp_path=tmp_path)
    grib_file = dummy_adaptor.retrieve({"size": 1})
    assert grib_file.name == str(tmp_path / "dummy.grib")
    assert os.path.getsize(grib_file.name) == 1

    netcdf_file = dummy_adaptor.retrieve({"size": 1, "format": "netcdf"})
    assert netcdf_file.name == str(tmp_path / "dummy.nc")
    assert os.path.getsize(netcdf_file.name) == 1
    assert grib_file.read() == netcdf_file.read()


def test_dummy_adaptor_zip(tmp_path: pathlib.Path) -> None:
    dummy_adaptor = cads_adaptors.DummyAdaptor(None, cache_tmp_path=tmp_path)
    grib_file = dummy_adaptor.retrieve({"size": 1, "foo": 1})
    assert grib_file.name == str(tmp_path / "dummy.grib")
    assert os.path.getsize(grib_file.name) == 1

    zip_file = dummy_adaptor.retrieve({"size": 3, "foo": [0, 1], "format": "zip"})
    unzipped_path = tmp_path / "unzipped"
    with zipfile.ZipFile(zip_file.name) as zip_fp:
        assert zip_fp.namelist() == ["dummy_0.grib", "dummy_1.grib"]
        zip_fp.extractall(unzipped_path)
    assert os.path.getsize(unzipped_path / "dummy_0.grib") == 2
    assert os.path.getsize(unzipped_path / "dummy_1.grib") == 1
    assert grib_file.read() == (unzipped_path / "dummy_1.grib").read_bytes()

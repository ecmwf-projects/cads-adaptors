import os
import pathlib
import zipfile
from datetime import datetime, timedelta, timezone

import pytest

import cads_adaptors

NOW = datetime.now(timezone.utc)
TODAY = NOW.strftime("%Y-%m-%d")
ONE_WEEK_AGO = (NOW - timedelta(days=7)).strftime("%Y-%m-%d")
TEN_DAYS_AGO = (NOW - timedelta(days=10)).strftime("%Y-%m-%d")
ONE_MONTH_AGO = (NOW - timedelta(days=30)).strftime("%Y-%m-%d")
LAST_WEEK_STR = ONE_WEEK_AGO + "/" + TODAY
LAST_MONTH_STR = ONE_MONTH_AGO + "/" + TODAY
TEN_TO_SEVEN_DAYS_AGO_STR = TEN_DAYS_AGO + "/" + ONE_WEEK_AGO


@pytest.mark.parametrize(
    "requests, embargos, expected",
    [
        (
            [{"date": LAST_WEEK_STR, "center": "C1", "variables": ["x", "y", "z"]}],
            [
                {"date": "current-4/current-3", "center": "C1"},
                {"date": "current-2/current", "center": "C2", "variables": ["x"]},
            ],
            True,
        ),
        (
            [
                {
                    "date": TEN_TO_SEVEN_DAYS_AGO_STR,
                    "center": "C1",
                    "variables": ["x", "y", "z"],
                },
                {"date": LAST_MONTH_STR, "center": "C2", "variables": ["y", "z"]},
            ],
            [
                {"date": "current-4/current-3", "center": "C1"},
                {"date": "current-2/current", "center": "C2", "variables": ["x"]},
            ],
            False,
        ),
        (
            [
                {
                    "date": TEN_TO_SEVEN_DAYS_AGO_STR,
                    "center": "C1",
                    "variables": ["x", "y", "z"],
                },
                {"date": LAST_MONTH_STR, "center": "C2", "variables": ["y", "x"]},
            ],
            [
                {"date": "current-4/current-3", "center": "C1"},
                {"date": "current-2/current", "center": "C2", "variables": ["x"]},
            ],
            True,
        ),
        (
            [
                {
                    "date": TEN_TO_SEVEN_DAYS_AGO_STR,
                    "center": "C1",
                    "variables": ["x", "y", "z"],
                },
                {
                    "date": TEN_TO_SEVEN_DAYS_AGO_STR,
                    "center": "C2",
                    "variables": ["y", "x"],
                },
            ],
            [
                {"date": "current-4/current-3", "center": "C1"},
                {"date": "current-2/current", "center": "C2", "variables": ["x"]},
            ],
            False,
        ),
    ],
)
def test_fine_grained_embargos_overlap(requests, embargos, expected):
    assert (
        cads_adaptors.adaptors.cds.is_request_overlapping_fine_grained_embargos(
            requests, embargos
        )
        == expected
    )


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

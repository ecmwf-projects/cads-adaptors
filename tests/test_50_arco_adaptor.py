import logging
import pathlib
from datetime import datetime, timedelta
from typing import Any, Type

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from cads_adaptors import ArcoDataLakeCdsAdaptor
from cads_adaptors.adaptors import Context
from cads_adaptors.exceptions import ArcoDataLakeNoDataError, InvalidRequest


@pytest.fixture
def arco_adaptor(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> ArcoDataLakeCdsAdaptor:
    def mock_add_user_visible_error(
        self, message: str, session: Any | None = None
    ) -> None:
        self.user_visible_errors.append(message)

    monkeypatch.setattr(Context, "user_visible_errors", [], raising=False)
    monkeypatch.setattr(Context, "add_user_visible_error", mock_add_user_visible_error)

    coords = {
        "time": pd.date_range(start="2000", periods=5),
        "latitude": range(-90, 91, 20),
        "longitude": range(-180, 180, 20),
    }

    ds = xr.Dataset(coords=coords)
    for var in ("foo", "bar"):
        ds[var] = xr.DataArray(np.random.randn(*ds.sizes.values()), coords=coords)

    url = str(tmp_path / "data.zarr")
    ds.to_zarr(url)
    return ArcoDataLakeCdsAdaptor(
        form=None,
        cache_tmp_path=tmp_path,
        mapping={"remap": {"variable": {"FOO": "foo", "BAR": "bar"}}},
        url=url,
    )


@pytest.mark.parametrize(
    "original,expected",
    [
        (
            {
                "data_format": ["nc"],
                "location": {
                    "longitude": 1,
                    "latitude": "2",
                },
                "date": 1990,
                "variable": ("foo", "bar"),
            },
            {
                "data_format": "netcdf",
                "location": {
                    "latitude": 2.0,
                    "longitude": 1.0,
                },
                "date": ["1990", "1990"],
                "variable": ["bar", "foo"],
            },
        ),
        (
            {
                "data_format": ["nc"],
                "location": {
                    "longitude": 1,
                    "latitude": "2",
                },
                "date": "1990/1991",
                "variable": ("foo", "bar"),
            },
            {
                "data_format": "netcdf",
                "location": {
                    "latitude": 2.0,
                    "longitude": 1.0,
                },
                "date": ["1990", "1991"],
                "variable": ["bar", "foo"],
            },
        ),
        (
            {
                "data_format": ["nc"],
                "location": {
                    "longitude": 1,
                    "latitude": "2",
                },
                "date": ["1990", "1991"],
                "variable": ("foo", "bar"),
            },
            {
                "data_format": "netcdf",
                "location": {
                    "latitude": 2.0,
                    "longitude": 1.0,
                },
                "date": ["1990", "1991"],
                "variable": ["bar", "foo"],
            },
        ),
    ],
)
def test_arco_normalise_request(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    original: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    request = arco_adaptor.normalise_request(original)
    assert request == expected


def test_arco_normalise_request_embargo_pass(
    arco_adaptor: ArcoDataLakeCdsAdaptor, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(arco_adaptor.config, "embargo", {"days": 2})

    request = {
        "data_format": "netcdf",
        "location": {
            "latitude": 2.0,
            "longitude": 1.0,
        },
        "date": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
        "variable": ["bar", "foo"],
    }
    request = arco_adaptor.normalise_request(request)
    assert request["date"] == [
        (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
        (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
    ]


def test_arco_normalise_request_embargo_raise(
    arco_adaptor: ArcoDataLakeCdsAdaptor, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(arco_adaptor.config, "embargo", {"days": 2})
    request = {
        "data_format": "netcdf",
        "location": {
            "latitude": 2.0,
            "longitude": 1.0,
        },
        "date": datetime.now().strftime("%Y-%m-%d"),
        "variable": ["bar", "foo"],
    }
    with pytest.raises(InvalidRequest, match="You have requested data under embargo"):
        arco_adaptor.normalise_request(request)


def test_arco_normalise_request_embargo_warn(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setitem(arco_adaptor.config, "embargo", {"days": 2})
    request = {
        "data_format": "netcdf",
        "location": {
            "latitude": 2.0,
            "longitude": 1.0,
        },
        "date": [
            (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ],
        "variable": ["bar", "foo"],
    }
    with caplog.at_level(logging.ERROR):
        request = arco_adaptor.normalise_request(request)
        assert any(
            "Part of the data you have requested is under embargo" in message
            for message in arco_adaptor.context.user_visible_errors   # type: ignore[attr-defined]
        )


@pytest.mark.parametrize(
    "invalid_request, match",
    [
        (
            {"location": {"latitude": 0, "longitude": 0}},
            "specify at least one variable",
        ),
        ({"variable": "FOO"}, "specify a single valid location"),
        (
            {
                "variable": "FOO",
                "location": [
                    {"latitude": 0, "longitude": 0},
                    {"latitude": 1, "longitude": 1},
                ],
            },
            "specify a single valid location",
        ),
        ({"variable": "FOO", "location": "foo"}, "Invalid location"),
        ({"variable": "FOO", "location": {"foo": "1"}}, "Invalid location"),
        (
            {"variable": "FOO", "location": {"latitude": "foo", "longitude": "bar"}},
            "Invalid location",
        ),
        (
            {
                "variable": "FOO",
                "location": {"latitude": 0, "longitude": 0},
                "date": [1, 2, 3],
            },
            "specify a single date range",
        ),
        (
            {
                "variable": "FOO",
                "location": {"latitude": 0, "longitude": 0},
                "date": [1, 2],
                "data_format": ["foo", "bar"],
            },
            "specify a single data_format",
        ),
        (
            {
                "variable": "FOO",
                "location": {"latitude": 0, "longitude": 0},
                "date": [1, 2],
                "data_format": "foo",
            },
            "Invalid data_format",
        ),
    ],
)
def test_arco_invalid_request(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    invalid_request: dict[str, Any],
    match: str,
) -> None:
    with pytest.raises(InvalidRequest, match=match):
        arco_adaptor.retrieve(invalid_request)


@pytest.mark.parametrize(
    "variable,expected",
    [
        ("FOO", {"foo"}),
        (["FOO"], {"foo"}),
        (["FOO", "BAR"], {"foo", "bar"}),
    ],
)
def test_arco_select_variable(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    variable: str | list[str],
    expected: set[str],
):
    fp = arco_adaptor.retrieve(
        {
            "variable": variable,
            "location": {"latitude": 0, "longitude": 0},
            "date": "2000",
        }
    )
    ds = xr.open_dataset(fp.name)
    assert ds.sizes == {"valid_time": 5}
    assert set(ds.coords) == {"latitude", "longitude", "valid_time"}
    assert set(ds.data_vars) == expected


def test_arco_select_location(arco_adaptor: ArcoDataLakeCdsAdaptor):
    request = {
        "variable": "FOO",
        "location": {"latitude": 31, "longitude": "41"},
        "date": "2000",
    }
    fp = arco_adaptor.retrieve(request)
    ds = xr.open_dataset(fp.name)
    assert ds["latitude"].item() == 30
    assert ds["longitude"].item() == 40


@pytest.mark.parametrize(
    "date,expected_size",
    [
        (2000, 5),
        ("2000-01-01", 1),
        ("2000-01-01/2000-01-02", 2),
    ],
)
def test_arco_select_date(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    date: str | int,
    expected_size: int,
):
    fp = arco_adaptor.retrieve(
        {
            "variable": "FOO",
            "location": {"latitude": 0, "longitude": 0},
            "date": date,
        }
    )
    ds = xr.open_dataset(fp.name)
    assert ds.sizes == {"valid_time": expected_size}
    assert set(ds.coords) == {"latitude", "longitude", "valid_time"}


@pytest.mark.parametrize(
    "data_format,extension",
    [
        ("netcdf4", ".nc"),
        ("csv", ".csv"),
    ],
)
def test_arco_data_format(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    tmp_path: pathlib.Path,
    data_format: str,
    extension: str,
):
    request = {
        "variable": "FOO",
        "location": {"latitude": 0, "longitude": 0},
        "date": "2000",
        "data_format": data_format,
    }
    fp = arco_adaptor.retrieve(request)
    assert fp.name.startswith(str(tmp_path))
    assert fp.name.endswith(extension)

    expected_variables = {"latitude", "foo", "valid_time", "longitude"}
    match extension:
        case ".nc":
            obj = xr.open_dataset(fp.name)
            assert set(obj.variables) == expected_variables
        case ".csv":
            obj = pd.read_csv(fp)
            assert set(obj.columns) == expected_variables
        case _:
            raise NotImplementedError

    assert (obj["latitude"] == 10).all()
    assert (obj["longitude"] == 0).all()
    assert obj["foo"].size == 5


@pytest.mark.parametrize(
    "bad_request,exception,message",
    [
        (
            {
                "variable": "wrong",
                "location": {"latitude": 0, "longitude": 0},
                "date": "2000",
            },
            KeyError,
            "Invalid variable: 'wrong'.",
        ),
        (
            {
                "variable": "FOO",
                "location": {"latitude": 0, "longitude": 0},
                "date": "foo",
            },
            TypeError,
            "Invalid date=['foo', 'foo']",
        ),
        (
            {
                "variable": "FOO",
                "location": {"latitude": 0, "longitude": 0},
                "date": 1990,
            },
            ArcoDataLakeNoDataError,
            "No data found for date=['1990', '1990']",
        ),
    ],
)
def test_user_visible_errors(
    arco_adaptor: ArcoDataLakeCdsAdaptor,
    bad_request: dict[str, Any],
    exception: Type[Exception],
    message: str,
):
    with pytest.raises(exception):
        arco_adaptor.retrieve(bad_request)
    assert arco_adaptor.context.user_visible_errors == [message]  # type: ignore[attr-defined]


def test_connection_problems(
    arco_adaptor: ArcoDataLakeCdsAdaptor, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(arco_adaptor.config, "url", "foo")
    with pytest.raises(FileNotFoundError):
        arco_adaptor.retrieve(
            {
                "variable": "FOO",
                "location": {"latitude": 0, "longitude": 0},
                "date": "2000",
            }
        )
    assert (
        "Cannot access the ARCO Data Lake"
        in arco_adaptor.context.user_visible_errors[-1]  # type: ignore[attr-defined]
    )

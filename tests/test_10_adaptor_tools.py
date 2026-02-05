import pytest

from cads_adaptors.exceptions import CdsConfigError
from cads_adaptors.tools.adaptor_tools import (
    get_data_format_from_mapped_requests,
    handle_data_format,
)

# -------------------------
# Tests for handle_data_format
# -------------------------


@pytest.mark.parametrize(
    "inp, expected",
    [
        ("netcdf4", "netcdf"),
        ("netcdf", "netcdf"),
        ("nc", "netcdf"),
        ("grib", "grib"),
        ("grib2", "grib"),
        ("grb", "grib"),
        ("grb2", "grib"),
        ("other", "other"),
        (["netcdf4"], "netcdf"),
        (("grib2",), "grib"),
        ({"nc"}, "netcdf"),
    ],
)
def test_handle_data_format_normalisation(inp, expected):
    assert handle_data_format(inp) == expected


def test_handle_data_format_multiple_values_asserts():
    with pytest.raises(AssertionError):
        handle_data_format(["netcdf", "grib"])


# -------------------------
# Tests for get_data_format_from_mapped_requests
# -------------------------


def test_get_data_format_from_mapped_requests_single_value():
    mapped_requests = [
        {"data_format": "netcdf", "foo": 1},
        {"data_format": "netcdf", "bar": 2},
    ]

    result = get_data_format_from_mapped_requests(mapped_requests)

    assert result == "netcdf"


def test_get_data_format_from_mapped_requests_grib():
    mapped_requests = [
        {"data_format": "grib"},
        {"data_format": "grib"},
    ]

    result = get_data_format_from_mapped_requests(mapped_requests)

    assert result == "grib"


def test_get_data_format_from_mapped_requests_mismatch_raises():
    mapped_requests = [
        {"data_format": "netcdf"},
        {"data_format": "grib"},
    ]

    with pytest.raises(CdsConfigError):
        get_data_format_from_mapped_requests(mapped_requests)


def test_get_data_format_from_mapped_requests_missing_raises():
    mapped_requests = [
        {"foo": "bar"},
    ]

    with pytest.raises(CdsConfigError):
        get_data_format_from_mapped_requests(mapped_requests)

from typing import Any

import pytest

from cads_adaptors import exceptions, mapping

DATE_KEY = "date"
YEAR_KEY = "year"
MONTH_KEY = "month"
DAY_KEY = "day"
FORCE: dict[str, Any] = {}
OPTIONS: dict[str, Any] = {}
REQUEST: dict[str, Any] = {}


def test_expand_date() -> None:
    # This used to fail because of different treatment for single dates and date
    # ranges
    r = {DATE_KEY: ["2021-01-01", "2021-01-02/2021-01-03"]}
    mapping.expand_dates(
        r,
        REQUEST,
        DATE_KEY,
        YEAR_KEY,
        MONTH_KEY,
        DAY_KEY,
        OPTIONS.get("date_format", "%Y-%m-%d"),
    )
    assert r[DATE_KEY] == ["2021-01-01", "2021-01-02", "2021-01-03"], r["date"]

    # Test separate year/month/day
    request = {YEAR_KEY: "2021", MONTH_KEY: "2", DAY_KEY: 1}
    r = {}
    mapping.expand_dates(
        r,
        request,
        DATE_KEY,
        YEAR_KEY,
        MONTH_KEY,
        DAY_KEY,
        OPTIONS.get("date_format", "%Y-%m-%d"),
    )
    assert r[DATE_KEY] == ["2021-02-01"]
    #
    # Check invalid dates are safely ignored
    request = {
        YEAR_KEY: "2021",
        MONTH_KEY: ["2", "3"],
        DAY_KEY: ["28", "29", "30", "31"],
    }
    r = {}
    mapping.expand_dates(
        r,
        request,
        DATE_KEY,
        YEAR_KEY,
        MONTH_KEY,
        DAY_KEY,
        OPTIONS.get("date_format", "%Y-%m-%d"),
    )
    assert r[DATE_KEY] == [
        "2021-02-28",
        "2021-03-28",
        "2021-03-29",
        "2021-03-30",
        "2021-03-31",
    ]


def test_date_mapping() -> None:
    req_mapping: dict[str, Any] = {"options": {"wants_dates": True}}
    request = {
        "year": "2003",
        "month": "03",
        "day": "03",
    }
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert (
        mapped_request["date"][0]
        == f"{request['year']}-{request['month']}-{request['day']}"
    )

    req_mapping = {"options": {"wants_dates": True, "date_format": "%Y%m%d"}}
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert (
        mapped_request["date"][0]
        == f"{request['year']}{request['month']}{request['day']}"
    )


def test_date_mapping_with_forced_values() -> None:
    req_mapping: dict[str, Any] = {
        "options": {"wants_dates": True},
        "force": {"day": "01"},
    }
    request = {
        "year": "2003",
        "month": "03",
    }
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert mapped_request["date"][0] == f"{request['year']}-{request['month']}-01"


def test_date_mapping_with_mixed_data_types() -> None:
    req_mapping: dict[str, Any] = {"options": {"wants_dates": True}}
    request = {"year": 2003, "month": "03", "day": 1}
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert (
        mapped_request["date"][0]
        == f"{request['year']}-{request['month']}-{request['day']:02d}"
    )


def test_hdate_mapping() -> None:
    req_mapping: dict[str, Any] = {"options": {"wants_dates": True}}
    request = {
        "hyear": "2003",
        "hmonth": "03",
        "hday": "03",
    }
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "hdate" in mapped_request
    assert (
        mapped_request["hdate"][0]
        == f"{request['hyear']}-{request['hmonth']}-{request['hday']}"
    )
    req_mapping = {"options": {"wants_dates": True, "hdate_format": "%Y%m%d"}}
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "hdate" in mapped_request
    assert (
        mapped_request["hdate"][0]
        == f"{request['hyear']}{request['hmonth']}{request['hday']}"
    )


def test_hdate_date_mapping() -> None:
    req_mapping: dict[str, Any] = {"options": {"wants_dates": True}}
    request = {
        "year": "2003",
        "month": "03",
        "day": "03",
        "hyear": "2004",
        "hmonth": "04",
        "hday": "04",
    }
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert (
        mapped_request["date"][0]
        == f"{request['year']}-{request['month']}-{request['day']}"
    )
    assert "hdate" in mapped_request
    assert (
        mapped_request["hdate"][0]
        == f"{request['hyear']}-{request['hmonth']}-{request['hday']}"
    )

    req_mapping = {"options": {"wants_dates": True, "hdate_format": "%Y%m%d"}}
    request = {
        "year": "2003",
        "month": "03",
        "day": "03",
        "hyear": "2004",
        "hmonth": "04",
        "hday": "04",
    }
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert (
        mapped_request["date"][0]
        == f"{request['year']}-{request['month']}-{request['day']}"
    )
    assert "hdate" in mapped_request
    assert (
        mapped_request["hdate"][0]
        == f"{request['hyear']}{request['hmonth']}{request['hday']}"
    )

    req_mapping = {"options": {"wants_dates": True}}
    request = {
        "date": "2003-03-03",
        "hyear": "2004",
        "hmonth": "04",
        "hday": "04",
    }
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert mapped_request["date"][0] == f"{request['date']}"
    assert "hdate" in mapped_request
    assert (
        mapped_request["hdate"][0]
        == f"{request['hyear']}-{request['hmonth']}-{request['hday']}"
    )


def test_area_as_mapping_applied_correctly():
    request = {
        "area": [60, -10, 50, 10]  # N, W, S, E
    }
    adaptor_mapping = {
        "options": {
            "area_as_mapping": [
                {
                    "latitude": 55,
                    "longitude": 0,
                    "country": "UK",
                    "source": "satellite"
                }
            ]
        }
    }
    result = mapping.area_as_mapping(request, adaptor_mapping)

    assert result["country"] == ["UK"]
    assert result["source"] == ["satellite"]
    assert "area" not in result  # Area should not be in the result
    assert "latitude" not in result
    assert "longitude" not in result


def test_area_as_mapping_merges_multiple_matches():
    request = {"area": [60, -10, 50, 10]}
    adaptor_mapping = {
        "options": {
            "area_as_mapping": [
                {"latitude": 55, "longitude": 0, "tag": "A"},
                {"latitude": 52, "longitude": 5, "tag": "B"},
            ]
        }
    }
    result = mapping.area_as_mapping(request, adaptor_mapping)

    assert sorted(result["tag"]) == ["A", "B"]
    assert "area" not in result  # Area should not be in the result


def test_area_as_mapping_raises_if_not_list():
    request = {"area": [60, -10, 50, 10]}
    adaptor_mapping = {
        "options": {
            "area_as_mapping": {
                "latitude": 55,
                "longitude": 0,
                "country": "UK",
            }  # Invalid: should be a list
        }
    }
    with pytest.raises(exceptions.CdsConfigurationError):
        mapping.area_as_mapping(request, adaptor_mapping)


def test_area_as_mapping_ignore_incorrect_elements():
    request = {"area": [60, -10, 50, 10]}
    adaptor_mapping = {
        "options": {
            "area_as_mapping": [
                {"latitude": 55, "longitude": 0, "country": "UK"},  # Correct element
                {"latitude": 53, "longitude": -8, "country": "IE"},  # Correct element
                {"longitude": 0, "country": "FR"},  # Incorrect element, no latitude
            ]
        }
    }
    result = mapping.area_as_mapping(request, adaptor_mapping)
    assert sorted(result["country"]) == [
        "IE",
        "UK",
    ]  # Only IE and UK mapping are correct
    assert (
        "another" not in result
    )  # Should not include keys only found in incorrect elements


def test_area_as_mapping_does_nothing_if_no_match():
    request = {"area": [10, -10, 0, 10]}  # Area does not match the lat/lon in mapping
    adaptor_mapping = {
        "options": {
            "area_as_mapping": [{"latitude": 50, "longitude": 5, "country": "Nowhere"}]
        }
    }
    result = mapping.area_as_mapping(request, adaptor_mapping)
    assert "country" not in result


def test_area_as_mapping_merges_with_existing_keys():
    request = {"area": [60, -10, 50, 10], "source": "ground"}
    adaptor_mapping = {
        "options": {
            "area_as_mapping": [{"latitude": 55, "longitude": 0, "source": "satellite"}]
        }
    }
    result = mapping.area_as_mapping(request, adaptor_mapping)
    assert result["source"] == ["ground", "satellite"]

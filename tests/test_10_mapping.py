from typing import Any

from cads_adaptors import mapping

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
    r = {DATE_KEY: ["2021-01-01", "2021-01-02/2021-01-03", "2021-01-04/to/2021-01-05"]}
    mapping.expand_dates(
        r,
        REQUEST,
        DATE_KEY,
        YEAR_KEY,
        MONTH_KEY,
        DAY_KEY,
        OPTIONS.get("date_format", "%Y-%m-%d"),
    )
    assert (
        r[DATE_KEY]
        == ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"]
        == r["date"]
    )

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

    request = {
        "date": ["2004-04-04", "2004-04-05/2004-04-06", "2004-04-07/to/2004-04-08"],
    }
    req_mapping = {"options": {"wants_dates": True, "date_format": "%Y-%m-%d"}}
    mapped_request = mapping.apply_mapping(request, req_mapping)
    assert "date" in mapped_request
    assert (
        mapped_request["date"]
        == [f"2004-04-{i:02d}" for i in range(4, 9)]
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

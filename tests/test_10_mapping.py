
from cads_adaptors import mapping


def test_date_mapping() -> None:
    req_mapping = {"options": {"wants_dates": True}}
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


def test_hdate_mapping() -> None:
    req_mapping = {"options": {"wants_dates": True}}
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


def test_hdate_date_mapping() -> None:
    req_mapping = {"options": {"wants_dates": True}}
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

import json
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
                {"latitude": 55, "longitude": 0, "country": "UK", "source": "satellite"}
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


def test_make_bbox_centered_in_point():
    lat, lon = 1.0, 2.0
    size = 2.0
    bbox = mapping.make_bbox_centered_in_point(lat, lon, size)
    expected_bbox = (0.0, 1.0, 2.0, 3.0)
    assert bbox == expected_bbox


def test_get_features_at_point(monkeypatch: pytest.MonkeyPatch) -> None:
    point = (10.0, 20.0)
    layer = "test_layer"
    spatial_reference_system = "test_srs"
    max_features = 10
    mock_features = [
        {"id": 1, "properties": {"name": "Feature1"}},
        {"id": 2, "properties": {"name": "Feature2"}},
    ]
    mock_response = {"features": mock_features}

    class MockResponse:
        def read(self) -> dict[str, Any]:
            return json.dumps(mock_response).encode()
        
    class MockWMS:
        def getfeatureinfo(self, **kwargs: Any) -> MockResponse:
            assert kwargs["layers"] == [layer]
            assert kwargs["srs"] == spatial_reference_system
            assert kwargs["bbox"] == (9.5, 19.5, 10.5, 20.5)
            assert kwargs["query_layers"] == [layer]
            assert kwargs["feature_count"] == 10
            return MockResponse()

    monkeypatch.setattr("owslib.wms.WebMapService", lambda *args, **kwargs: MockWMS())
    result = mapping.get_features_at_point(
        point, layer, spatial_reference_system, max_features
    )
    assert result == mock_features


def test_get_features_at_point_wms_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    point = (10.0, 20.0)
    layer = "test_layer"

    def mock_wms_constructor(*args: Any, **kwargs: Any) -> None:
        raise Exception()

    monkeypatch.setattr("owslib.wms.WebMapService", mock_wms_constructor)

    with pytest.raises(
        exceptions.GeoServerError, match="Could not connect to WMS service"
    ):
        mapping.get_features_at_point(point, layer)


def test_get_features_at_point_getfeatureinfo_error(monkeypatch: pytest.MonkeyPatch) -> None:
    point = (10.0, 20.0)
    layer = "test_layer"
    
    class MockWMS:
        def getfeatureinfo(self, **kwargs: Any) -> None:
            raise Exception()
    
    monkeypatch.setattr("owslib.wms.WebMapService", lambda *args, **kwargs: MockWMS())
    
    with pytest.raises(exceptions.GeoServerError, match="Could not retrieve features from WMS service"):
        mapping.get_features_at_point(point, layer)


def test_get_features_in_area(monkeypatch: pytest.MonkeyPatch) -> None:
    area = (40, -10.0, 50.0, 10.0)
    layer = "test_layer"
    spatial_reference_system = "test_srs"
    max_features = 10
    mock_features = [
        {"id": 1, "properties": {"name": "Feature1"}},
        {"id": 2, "properties": {"name": "Feature2"}},
    ]
    mock_response = {"features": mock_features}

    class MockResponse:
        def getvalue(self) -> dict[str, Any]:
            return json.dumps(mock_response).encode()

    class MockWFS:
        def getfeature(self, **kwargs: Any) -> MockResponse:
            assert kwargs["typename"] == [layer]
            assert kwargs["bbox"] == area
            assert kwargs["srsname"] == spatial_reference_system
            assert kwargs["maxfeatures"] == max_features
            return MockResponse()

    monkeypatch.setattr("owslib.wfs.WebFeatureService", lambda *args, **kwargs: MockWFS())
    result = mapping.get_features_in_area(
        area, layer, spatial_reference_system, max_features
    )
    assert result == mock_features


def test_get_features_in_area_wfs_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = (40, -10.0, 50.0, 10.0)
    layer = "test_layer"

    def mock_wfs_constructor(*args: Any, **kwargs: Any) -> None:
        raise Exception()

    monkeypatch.setattr("owslib.wfs.WebFeatureService", mock_wfs_constructor)

    with pytest.raises(
        exceptions.GeoServerError, match="Could not connect to WFS service"
    ):
        mapping.get_features_in_area(area, layer)


def test_get_features_in_area_getfeature_error(monkeypatch: pytest.MonkeyPatch) -> None:
    area = (40, -10.0, 50.0, 10.0)
    layer = "test_layer"
    
    class MockWFS:
        def getfeature(self, **kwargs: Any) -> None:
            raise Exception()
    
    monkeypatch.setattr("owslib.wfs.WebFeatureService", lambda *args, **kwargs: MockWFS())
    
    with pytest.raises(exceptions.GeoServerError, match="Could not retrieve features from WFS service"):
        mapping.get_features_in_area(area, layer)


def test_get_features_in_request_location(monkeypatch: pytest.MonkeyPatch) -> None:
    request = {
        "location": {
            "latitude": 10.0,
            "longitude": 20.0,
        }
    }
    layer = "test_layer"
    mock_features = [
        {"id": 1, "properties": {"name": "Feature1"}},
        {"id": 2, "properties": {"name": "Feature2"}},
    ]
    mock_response = {"features": mock_features}

    class MockResponse:
        def read(self) -> dict[str, Any]:
            return json.dumps(mock_response).encode()
        
    class MockWMS:
        def getfeatureinfo(self, **kwargs: Any) -> MockResponse:
            return MockResponse()

    monkeypatch.setattr("owslib.wms.WebMapService", lambda *args, **kwargs: MockWMS())
    result = mapping.get_features_in_request(request, layer)
    assert result == mock_features


def test_get_features_in_request_location_invalid() -> None:
    request = {
        "location": {
            "lat": 10.0,
            "lon": 20.0,
        }
    }
    layer = "test_layer"
    result = mapping.get_features_in_request(request, layer)
    expected = []
    assert result == expected


def test_get_features_in_request_area(monkeypatch: pytest.MonkeyPatch) -> None:
    request = {
        "area": [40.0, -10.0, 50.0, 10.0],
    }
    layer = "test_layer"
    mock_features = [
        {"id": 1, "properties": {"name": "Feature1"}},
        {"id": 2, "properties": {"name": "Feature2"}},
    ]
    mock_response = {"features": mock_features}

    class MockResponse:
        def getvalue(self) -> dict[str, Any]:
            return json.dumps(mock_response).encode()
        
    class MockWFS:
        def getfeature(self, **kwargs: Any) -> MockResponse:
            return MockResponse()
        
    monkeypatch.setattr("owslib.wfs.WebFeatureService", lambda *args, **kwargs: MockWFS())
    result = mapping.get_features_in_request(request, layer)
    assert result == mock_features


def test_get_features_in_request_area_invalid() -> None:
    request = {
        "area": "wrong_area_type",
    }
    layer = "test_layer"
    result = mapping.get_features_in_request(request, layer)
    expected = []
    assert result == expected

    request = {
        "area": [40.0, -10.0, 50.0],
    }
    result = mapping.get_features_in_request(request, layer)
    expected = []
    assert result == expected


def test_get_features_in_request_no_location_or_area() -> None:
    request = {}
    layer = "test_layer"
    result = mapping.get_features_in_request(request, layer)
    expected = []
    assert result == expected



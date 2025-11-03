from cads_adaptors import UrlCdsAdaptor
from cads_adaptors.adaptors import Context


def test_url_pre_mapping_modifications():
    adaptor = UrlCdsAdaptor(
        config={},
        form=[],
        context=Context(),
    )

    request = {
        "format": "zip",
        "area": [30, 20, 10, 40],
        "other_param": "value",
    }

    modified_request = adaptor.pre_mapping_modifications(request)

    assert "format" not in modified_request
    assert "area" not in modified_request
    assert adaptor.download_format == "zip"
    assert adaptor.area == [30, 20, 10, 40]
    assert modified_request["other_param"] == "value"


def test_url_pre_mapping_modifications_area_as_mapping():
    adaptor = UrlCdsAdaptor(
        mapping={
            "options": {
                "area_as_mapping": [
                    {"id": "a", "latitude": 15, "longitude": 25},
                ]
            }
        },
        form=[],
        context=Context(),
    )

    request = {
        "format": "zip",
        "area": [30, 20, 10, 40],
        "other_param": "value",
    }

    modified_request = adaptor.pre_mapping_modifications(request)

    assert "area" not in modified_request
    assert "id" in modified_request
    assert "a" in modified_request["id"]

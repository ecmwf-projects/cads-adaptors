import pytest

from cads_adaptors import DummyCdsAdaptor


@pytest.mark.parametrize(
    "in_value",
    (
        1, "1", [1], (1,), {1}, ["1"], {"1"}, ("1",),
    ),
)
def test_normalize_request(in_value):
    this_adaptor = DummyCdsAdaptor({}, collection_id="test-collection")
    request = this_adaptor.normalise_request({"test": in_value})
    assert request == {"test": ["1"]}


def test_normalize_request_complex():
    this_adaptor = DummyCdsAdaptor({}, collection_id="test-collection")
    request = this_adaptor.normalise_request({
        "test": 1,
        "test2": "1",
        "test3": [1, "2"],
        "test4": (1, "2"),
        "test5": {1, "2"}
    })
    assert request == {
        "test": ["1"],
        "test2": ["1"],
        "test3": ["1", "2"],
        "test4": ["1", "2"],
        "test5": ["1", "2"]
    }

    
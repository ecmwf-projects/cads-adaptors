import pytest

from cads_adaptors import DummyCdsAdaptor


@pytest.mark.parametrize(
    "in_value",
    (
        1,
        "1",
        [1],
        (1,),
        {1},
        ["1"],
        {"1"},
        ("1",),
    ),
)
def test_normalize_request(in_value):
    this_adaptor = DummyCdsAdaptor({}, collection_id="test-collection")
    request = this_adaptor.normalise_request({"test": in_value})
    assert request == {"test": ["1"]}


def test_normalize_request_complex():
    this_adaptor = DummyCdsAdaptor({}, collection_id="test-collection")
    request_in = {
        "test": 1,
        "test2": "1",
        "test3": [1, "2"],
        "test4": (1, "2"),
    }
    assert this_adaptor.normalise_request(request_in) == {
        "test": ["1"],
        "test2": ["1"],
        "test3": ["1", "2"],
        "test4": ["1", "2"],
    }

    request_in.update(
        {
            "test5": {1, "2"},
        }
    )
    request_out = this_adaptor.normalise_request(request_in)
    assert request_out["test"] == ["1"]
    assert request_out["test2"] == ["1"]
    assert request_out["test3"] == ["1", "2"]
    assert request_out["test4"] == ["1", "2"]
    # Test 5 is a set, so order not guaranteed
    assert sorted(request_out["test5"]) == ["1", "2"]

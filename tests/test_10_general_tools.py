import pytest

from cads_adaptors.tools import general


@pytest.mark.parametrize(
    "input_item, expected_output",
    [
        ("test", ["test"]),
        (1, [1]),
        (1.0, [1.0]),
        (object, [object]),
        ({"test"}, ["test"]),
        (["test", "test2"], ["test", "test2"]),
        (["test"], ["test"]),
        ((1, 2), [1, 2]),
        ((1,), [1]),
        ((), []),
        ({"test": "test"}, [{"test": "test"}]),
        (None, []),
    ],
)
def test_general_ensure_list(input_item, expected_output):
    assert general.ensure_list(input_item) == expected_output


# Test for split_requests_on_keys
@pytest.mark.parametrize(
    "requests, split_on_keys, expected_output",
    [
        (
            # Nothing to split, should return input
            [
                {"key1": "value1", "key2": "value2"},
                {"key1": "value3", "key2": "value4"},
            ],
            ["key1"],
            [
                {"key1": "value1", "key2": "value2"},
                {"key1": "value3", "key2": "value4"},
            ],
        ),
        (
            # Split on key1, return 2 requests
            [
                {"key1": ["value1", "value2"], "key2": "value3"},
            ],
            ["key1"],
            [
                {"key1": "value1", "key2": "value3"},
                {"key1": "value2", "key2": "value3"},
            ],
        ),
        (
            # Split on key1 in one of multiple requests
            [
                {"key1": ["value1", "value2"], "key2": "value3"},
                {"key1": "value4", "key2": "value5"},
            ],
            ["key1"],
            [
                {"key1": "value1", "key2": "value3"},
                {"key1": "value2", "key2": "value3"},
                {"key1": "value4", "key2": "value5"},
            ],
        ),
        (
            # nothing to split in multiple requests
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
            ["key1"],
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
        ),
        (
            # split on key does not exist
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
            ["key3"],
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
        ),
        (
            # split on is empty list
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
            [],
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
        ),
        (
            # Split on multiple keys
            [
                {"key1": ["value1", "value2"], "key2": ["value3", "value4"]},
                # {"key1": "value4", "key2": ["value5", "value6"]},
            ],
            ["key1", "key2"],
            [
                {"key1": "value1", "key2": "value3"},
                {"key1": "value1", "key2": "value4"},
                {"key1": "value2", "key2": "value3"},
                {"key1": "value2", "key2": "value4"},
                # {"key1": "value4", "key2": "value5"},
                # {"key1": "value4", "key2": "value6"},
            ],
        ),
        (
            # Split on multiple keys, accross multiple requests
            [
                {"key1": ["value1", "value2"], "key2": ["value3", "value4"]},
                {"key1": "value5", "key2": ["value6", "value7"]},
            ],
            ["key1", "key2"],
            [
                {"key1": "value1", "key2": "value3"},
                {"key1": "value1", "key2": "value4"},
                {"key1": "value2", "key2": "value3"},
                {"key1": "value2", "key2": "value4"},
                {"key1": "value5", "key2": "value6"},
                {"key1": "value5", "key2": "value7"},
            ],
        ),
    ],
)
def test_general_split_requests_on_keys(requests, split_on_keys, expected_output):
    assert general.split_requests_on_keys(requests, split_on_keys) == expected_output

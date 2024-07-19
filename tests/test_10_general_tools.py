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
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
            ["key1"],
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
        ),
        (
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
            ["key3"],
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
        ),
        (
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
            [],
            [{"key1": "value1", "key2": "value2"}, {"key2": "value3"}],
        ),
    ],
)
def test_general_split_requests_on_keys(requests, split_on_keys, expected_output):
    assert general.split_requests_on_keys(requests, split_on_keys) == expected_output

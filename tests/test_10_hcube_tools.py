import numpy as np
import pytest

from cads_adaptors.tools import hcube_tools


@pytest.mark.parametrize(
    "input_data, expected",
    [
        ([], {}),
        ([{"a": 1, "b": 2}], {"a": 1, "b": 2}),
        ([{"a": 1}, {"a": "2"}], {"a": [1, "2"]}),
        ([{"a": "1", "b": "2"}], {"a": "1", "b": "2"}),
        ([{"a": "1"}, {"b": "2"}, {"c": "3"}], {"a": "1", "b": "2", "c": "3"}),
        ([{"a": ["1"]}, {"b": "2"}, {"c": "3"}], {"a": ["1"], "b": "2", "c": "3"}),
        ([{"a": "1"}, {"a": "1"}], {"a": "1"}),
        ([{"a": "1"}, {"a": "2"}], {"a": ["1", "2"]}),
        ([{"a": ["1", "2"]}, {"a": ["2", "3"]}], {"a": ["1", "2", "3"]}),
        ([{"a": "1"}, {"a": ["1", "2"]}], {"a": ["1", "2"]}),
        (
            [
                {"a": "1", "b": ["1", "2"]},
                {"a": "2", "b": ["2", "3"], "c": "x"},
                {"a": "1", "c": "x"},
            ],
            {"a": ["1", "2"], "b": ["1", "2", "3"], "c": "x"},
        ),
    ],
)
def test_merge_requests(input_data, expected):
    result = hcube_tools.merge_requests(input_data)
    assert result == expected


def test_merge_tuple_values():
    input_data = [{"a": (1, 2)}, {"a": (2, 3)}]
    result = hcube_tools.merge_requests(input_data)
    assert set(result["a"]) == {1, 2, 3}


def test_non_comparable_values():
    input_data = [{"a": 1}, {"a": np.array([1, 2])}]
    result = hcube_tools.merge_requests(input_data)
    # Non-comparable values should be kept as they are and concatenated as a list
    assert result["a"][0] == 1
    assert np.array_equal(result["a"][1], np.array([1, 2]))

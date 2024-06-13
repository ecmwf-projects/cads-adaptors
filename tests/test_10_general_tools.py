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

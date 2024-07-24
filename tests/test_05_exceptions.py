import pytest

from cads_adaptors import exceptions


@pytest.mark.parametrize(
    "test_exception",
    [
        exceptions.MarsNoDataError,
        exceptions.MarsRuntimeError,
        exceptions.MarsSystemError,
        exceptions.MultiAdaptorNoDataError,
        exceptions.UrlNoDataError,
        exceptions.RoocsRuntimeError,
        exceptions.RoocsValueError,
        exceptions.CadsObsRuntimeError,
    ],
)
def test_exceptions_raise_as_expected(test_exception) -> None:
    with pytest.raises(test_exception):
        raise test_exception

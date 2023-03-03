import cads_adaptors


def test_version() -> None:
    assert cads_adaptors.__version__ != "999"

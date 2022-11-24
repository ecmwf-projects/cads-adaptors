import cads_retrieve_tools


def test_version() -> None:
    assert cads_retrieve_tools.__version__ != "999"

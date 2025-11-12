import os

import logging
import pytest
import requests

from cads_adaptors.adaptors import mars, Context
from cads_adaptors.exceptions import InvalidRequest


TEST_GRIB_FILE = "https://sites.ecmwf.int/repository/earthkit-data/test-data/era5-levels-members.grib"
logger = logging.getLogger(__name__)


def test_get_mars_servers():
    mars_servers = mars.get_mars_server_list(
        {"mars_servers": "http://b-test-server.url"}
    )
    assert len(mars_servers) == 1
    assert mars_servers[0] == "http://b-test-server.url"


def test_get_mars_servers_list_file():
    mars_servers = mars.get_mars_server_list(
        {"mars_server_list": "tests/data/mars_servers.list"}
    )
    assert len(mars_servers) == 1
    assert mars_servers[0] == "http://a-test-server.url"


def test_get_mars_servers_envvar():
    os.environ["MARS_API_SERVER_LIST"] = "tests/data/mars_servers.list"
    mars_servers = mars.get_mars_server_list({})
    assert len(mars_servers) == 1
    assert mars_servers[0] == "http://a-test-server.url"


def test_convert_format(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mars_adaptor = mars.MarsCdsAdaptor({}, {})

    assert hasattr(mars_adaptor, "convert_format")

    url = TEST_GRIB_FILE
    remote_file = requests.get(url)
    _, ext = os.path.splitext(url)

    tmp_file = f"test{ext}"
    with open(tmp_file, "wb") as f:
        f.write(remote_file.content)

    converted_files = mars_adaptor.convert_format(
        tmp_file,
        "netcdf",
    )
    assert isinstance(converted_files, list)
    assert len(converted_files) == 1
    _, out_ext = os.path.splitext(converted_files[0])
    assert out_ext == ".nc"

    test_subdir = "./test_subdir"
    os.makedirs(test_subdir, exist_ok=True)
    converted_files = mars_adaptor.convert_format(
        tmp_file, "netcdf", target_dir=test_subdir
    )
    assert isinstance(converted_files, list)
    assert len(converted_files) == 1
    _, out_ext = os.path.splitext(converted_files[0])
    assert out_ext == ".nc"
    assert "/test_subdir/" in converted_files[0]


def test_schema_null():
    """Test that null request inputs don't pass the schema"""

    # Not a dict
    _check_schema_fail('', "request: '' is not of type 'dict'")

    # Null dict
    _check_schema_fail({}, "request: {} should be non-empty")

    # Null/whitespace keys and values
    for string in ["", " ", "\t"]:
        string_repr = repr(string).strip("'")

        # Null key
        _check_schema_fail({string: "1"},
                           f"request: '{string_repr}' is an invalid key name")

        # Null value
        _check_schema_fail({"param": string},
                           f"request['param'][0]: invalid value: '{string}'")


def test_schema_invalid_chars():
    """Test that invalid characters don't pass the schema"""

    # Test various invalid characters. It's important to not allow ",", "=" and
    # "\n" because they have special meaning to MARS. Test some other random non-word
    # characters too.
    for badchar in [",", "=", "\n", "\b", "\r", "\f"]:
        # Test them at the beginning, middle and end of the string
        for pos in [0, 1, 2]:
            string = "ab"
            string = string[:pos] + badchar + string[pos:]
            string_repr = repr(string).strip("'")

            # Check the invalid string fails both as a key and a value
            _check_schema_fail({string: "1"},
                               f"request: '{string_repr}' is an invalid key name")
            _check_schema_fail({'param': string},
                               f"request['param'][0]: invalid value: '{string}'")


def test_schema_whitespace(): 
    """Test the presence of whitespace (space/tab) in keys and values"""

    # Whitespace is only permitted at the start or end of a key/value, not in the
    # middle
    for badchar in [" ", "\t"]:
        for pos in [0, 1, 2]:
            string = "ab"
            string = string[:pos] + badchar + string[pos:]
            string_repr = repr(string).strip("'")

            # Whitespace is allowed at the start and end of the string, but not in
            # the middle
            if pos in [0, 2]:
                _check_schema_pass({string: "1"}, {string: ["1"]})
                _check_schema_pass({"param": string}, {"param": [string]})
            else:
                _check_schema_fail({string: "1"},
                                   f"request: '{string_repr}' is an invalid key name")
                _check_schema_fail({'param': string},
                                   f"request['param'][0]: invalid value: '{string}'")


def test_schema_pass():
    """Check the schema allows expected keys/values"""

    _check_schema_pass({"a": 1},
                       {"a": ["1"]})
    _check_schema_pass({"A": "a"},
                       {"A": ["a"]})
    _check_schema_pass({"0": ["a"]},
                       {"0": ["a"]})
    _check_schema_pass({"_": 1},
                       {"_": ["1"]})
    _check_schema_pass({" abc ": [3, 2, 1, "foo-bar"],
                        "\txyz\t\t": "3/2/1/foo-bar"},
                       {" abc ": ["3", "2", "1", "foo-bar"],
                        "\txyz\t\t": ["3/2/1/foo-bar"]})


def test_schema_duplicates():
    """Test behaviour with duplicate values in value lists"""

    # Duplicate values are allowed for area and grid
    _check_schema_pass({"area": [1, 1]},
                       {"area": ["1", "1"]})
    _check_schema_pass({"grid": ["1", "1"]},
                       {"grid": ["1", "1"]})
    _check_schema_pass({"GriD": [1, 1]},
                       {"GriD": ["1", "1"]})

    # They're not allowed for other keys
    _check_schema_fail({"param": [1, 1]},
                       "request['param']: has repeated values in the list, e.g. '1'")

    # ... unless the key is configured to permit them
    _check_schema_pass({"param": [1, 1]},
                       {"param": ["1", "1"]},
                       allow_duplicate_values_keys=["param"])

    # ... or they are automatically removed
    _check_schema_pass({"param": [1, 1]},
                       {"param": ["1"]},
                       remove_duplicate_values=True)


def _check_schema_fail(request, error_msg):
    """Check a request fails the schema with the expected error message"""
    adp = mars.MarsCdsAdaptor(form=None,
                              context=Context(logger=logger))
    with pytest.raises(InvalidRequest) as einfo:
        output = adp.normalise_request(request)
        print('Output = ', output)

    if einfo.value.args[0] != error_msg:
        raise Exception("Schema error message not as expected: "
                        f"{einfo.value.args[0]!r} != {error_msg!r}")


def _check_schema_pass(req_in, req_out, **schema_options):
    """Check a request passes the schema and gives the expected output"""
    adp = mars.MarsCdsAdaptor(form=None,
                              context=Context(logger=logger),
                              schema_options=schema_options)
    req_mod = adp.normalise_request(req_in)
    assert req_mod == req_out

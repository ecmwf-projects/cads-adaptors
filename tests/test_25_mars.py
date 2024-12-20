import os
import tempfile

import requests

from cads_adaptors.adaptors import mars

TEST_GRIB_FILE = (
    "https://get.ecmwf.int/repository/test-data/cfgrib/era5-levels-members.grib"
)


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


def test_convert_format():
    mars_adaptor = mars.MarsCdsAdaptor({}, {})

    assert hasattr(mars_adaptor, "convert_format")

    url = TEST_GRIB_FILE
    remote_file = requests.get(url)
    _, ext = os.path.splitext(url)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chdir(tmpdirname)
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

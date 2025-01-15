import json
import os
import pathlib
from typing import Any

import cacholote
import pytest
import requests

from cads_adaptors.adaptors import Context, mars

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


def test_cached_execute_mars(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    def mock_execute_mars(
        request: dict[str, Any] | list[dict[str, Any]],
        context: Context = Context(),
        config: dict[str, Any] = dict(),
        mapping: dict[str, Any] = dict(),
        target_fname: str = "data.grib",
        target_dir: str | pathlib.Path = "",
    ) -> str:
        target_path = pathlib.Path(target_dir) / target_fname
        target_path.write_text(json.dumps(request))
        return str(target_path)

    monkeypatch.setattr(mars, "execute_mars", mock_execute_mars)
    cached_execute_mars = mars.CachedExecuteMars(Context(), {}, {}, tmp_path)

    requests = [{"1": 1, "2": 2}, {"3": 3}, {"4": 4}]
    cached_file = cached_execute_mars.execute_mars(requests)
    assert cached_file.startswith(str(tmp_path / "cache_files"))

    assert cached_execute_mars.execute_mars([{"foo": "bar"}]) != cached_file

    reversed_requests = [{"4": 4}, {"3": 3}, {"2": 2, "1": 1}]
    assert cached_execute_mars.execute_mars(reversed_requests) == cached_file

    result = cached_execute_mars.retrieve(requests)
    assert isinstance(result, cacholote.extra_encoders.InPlaceFile)
    assert result.name == cached_execute_mars.execute_mars(requests)

import os

from cads_adaptors.adaptors import mars


def test_get_mars_servers():
    mars_servers = mars.get_mars_server_list(
        {"mars_server_list": "tests/data/mars_servers.list"}
    )
    assert len(mars_servers) == 1
    assert mars_servers[0] == "http://a-test-server.url"


def test_get_mars_servers_env():
    os.environ["MARS_API_SERVER_LIST"] = "tests/data/mars_servers.list"
    mars_servers = mars.get_mars_server_list({})
    assert len(mars_servers) == 1
    assert mars_servers[0] == "http://a-test-server.url"

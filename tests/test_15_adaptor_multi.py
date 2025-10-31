import os

import pytest
import requests

from cads_adaptors import AbstractAdaptor
from cads_adaptors.adaptors import multi

TEST_GRIB_FILE = "https://sites.ecmwf.int/repository/earthkit-data/test-data/era5-levels-members.grib"

FORM = {
    "level": ["500", "850"],
    "time": ["12:00", "00:00"],
    "param": ["Z", "T"],
    "stat": ["mean", "max"],
}
REQUEST = FORM.copy()

ADAPTOR_CONFIG = {
    "entry_point": "MultiAdaptor",
    "adaptors": {
        "mean": {
            "entry_point": "cads_adaptors:UrlCdsAdaptor",
            "values": {
                "level": ["500", "850"],
                "time": ["12:00", "00:00"],
                "param": ["Z", "T"],
                "stat": ["mean"],
            },
            "required_keys": ["level"],
            "dont_split_keys": ["dont_split"],
        },
        "max": {
            "entry_point": "cads_adaptors:DummyCdsAdaptor",
            "values": {
                "level": ["500", "850"],
                "time": ["12:00", "00:00"],
                "param": ["Z", "T"],
                "stat": ["max"],
            },
        },
    },
}


def test_multi_adaptor_extract_subrequests():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    split_mean = multi_adaptor.extract_subrequest(
        REQUEST, multi_adaptor.config["adaptors"]["mean"]["values"]
    )
    assert split_mean == ADAPTOR_CONFIG["adaptors"]["mean"]["values"]

    split_max = multi_adaptor.extract_subrequest(
        REQUEST, multi_adaptor.config["adaptors"]["max"]["values"]
    )
    assert split_max == ADAPTOR_CONFIG["adaptors"]["max"]["values"]


def test_multi_adaptor_extract_subrequests_required_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = REQUEST.copy()
    del request["level"]
    split_mean_required_missing = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        required_keys=["level"],
    )
    assert split_mean_required_missing == dict()

    split_max_required_present = multi_adaptor.extract_subrequest(
        REQUEST,
        multi_adaptor.config["adaptors"]["max"]["values"],
        required_keys=["level"],
    )
    assert split_max_required_present == ADAPTOR_CONFIG["adaptors"]["max"]["values"]


def test_multi_adaptor_extract_subrequests_dont_split_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = REQUEST.copy()
    # dont_split_keys as list dtype
    request["dont_split"] = [1, 2, 3, 4]
    split_mean_dont_split_area = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split_area

    # dont_split_keys as integer dtype
    request["dont_split"] = "1"
    split_mean_dont_split = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # dont_split_keys as integer dtype
    request["dont_split"] = 1
    split_mean_dont_split = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # dont_split_keys as float dtype
    request["dont_split"] = 1.0
    split_mean_dont_split = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # dont_split_keys as dict dtype
    request["dont_split"] = {"a": 1}
    split_mean_dont_split = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # Area is dont_split as default
    request["area"] = [1, 2, 3, 4]
    split_max_split_area = multi_adaptor.extract_subrequest(
        request,
        multi_adaptor.config["adaptors"]["max"]["values"],
    )
    assert "dont_split" not in split_max_split_area
    assert "area" in split_max_split_area


def test_multi_adaptor_extract_subrequests_filter_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = {
        "a": ["a1", "a2"],
        "b": ["b1", "b2"],
    }
    values = {
        "a": ["a1"],
        "b": ["b2"],
    }

    # Check that we only filter a certain key
    filter_a = multi_adaptor.extract_subrequest(
        request,
        values,
        filter_keys=["a"],
    )
    assert filter_a == {"a": ["a1"], "b": ["b1", "b2"]}

    # Check that we default to filter all keys (duplicate of previous test)
    filter_all = multi_adaptor.extract_subrequest(
        request,
        values,
    )
    assert filter_all == {"a": ["a1"], "b": ["b2"]}


EXTRACT_SR_KWARGS_FORM = {
    "a": ["a1", "a2", "a3", "a4"],
    "b": ["b1", "b2", "b3", "b4"],
    "c": ["c1", "c2", "c3", "c4"],
    "d": ["d1", "d2", "d3", "d4"],
}
EXTRACT_SR_KWARGS_ADAPTOR_CONFIG = {
    "entry_point": "MultiAdaptor",
    "adaptors": {
        "adaptor1": {
            "entry_point": "cads_adaptors:DummyCdsAdaptor",
            "values": {
                "a": ["a1", "a2", "a3", "a4"],
                "b": [
                    "b1",
                    "b2",
                ],
                "c": ["c1", "c2", "c3"],
                "d": ["d1", "d2", "d3", "d4"],
            },
            # We can also decide set extract_subrequest_kwargs for each adaptor, but hoepfully this is
            # no longer required. For backwards compatibility we do not next inside extract_subrequest_kwargs
            "filter_keys": ["a"],
            "dont_split_keys": ["e"],
            "required_keys": [
                "a",
                "b",
                "c",
            ],  # Includes check that double definition does not break
        },
        "adaptor2": {
            "entry_point": "cads_adaptors:DummyCdsAdaptor",
            "values": {
                "a": ["a1", "a2", "a3", "a4"],
                "b": ["b3", "b4"],
                "c": ["c2", "c3", "c4"],
            },
        },
    },
    # These filter keys are used by all sub-adaptors. In practice gecko will
    # detect requiements and populate this list automatically
    "extract_subrequest_kwargs": {
        "filter_keys": ["b", "c", "d"],
        "dont_split_keys": ["f", "g", "h"],
        "required_keys": ["a"],
    },
}


@pytest.mark.parametrize("entry_point", ["MultiAdaptor", "MultiMarsCdsAdaptor"])
def test_multi_adaptor_get_extract_subrequests_kwargs(entry_point):
    multi_adaptor = multi.MultiAdaptor(
        EXTRACT_SR_KWARGS_FORM,
        **{**EXTRACT_SR_KWARGS_ADAPTOR_CONFIG, "entry_point": entry_point},
    )
    # Check that we filter expected keys
    adaptor1_kwargs = multi_adaptor.get_extract_subrequest_kwargs(
        EXTRACT_SR_KWARGS_ADAPTOR_CONFIG["adaptors"]["adaptor1"]
    )
    assert "filter_keys" in adaptor1_kwargs
    assert sorted(adaptor1_kwargs["filter_keys"]) == ["a", "b", "c", "d"]
    assert "dont_split_keys" in adaptor1_kwargs
    assert sorted(adaptor1_kwargs["dont_split_keys"]) == ["e", "f", "g", "h"]
    assert "required_keys" in adaptor1_kwargs
    assert sorted(adaptor1_kwargs["required_keys"]) == ["a", "b", "c"]

    adaptor2_kwargs = multi_adaptor.get_extract_subrequest_kwargs(
        EXTRACT_SR_KWARGS_ADAPTOR_CONFIG["adaptors"]["adaptor2"]
    )
    assert "filter_keys" in adaptor2_kwargs
    assert sorted(adaptor2_kwargs["filter_keys"]) == ["b", "c", "d"]
    assert "dont_split_keys" in adaptor2_kwargs
    assert sorted(adaptor2_kwargs["dont_split_keys"]) == ["f", "g", "h"]
    assert "required_keys" in adaptor2_kwargs
    assert sorted(adaptor2_kwargs["required_keys"]) == ["a"]


def test_multi_adaptor_split_adaptors():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    sub_adaptors = multi_adaptor.split_adaptors(
        REQUEST,
    )

    # Check that the sub-adaptors have the correct values
    for adaptor in ["mean", "max"]:
        sub_adaptor_request = sub_adaptors[adaptor][1]
        assert sub_adaptor_request == ADAPTOR_CONFIG["adaptors"][adaptor]["values"]

    for adaptor_tag, [adaptor, req] in sub_adaptors.items():
        assert isinstance(adaptor_tag, str)
        assert isinstance(adaptor, AbstractAdaptor)
        assert isinstance(req, dict)

        # Check context is inherited from parent
        assert adaptor.context is multi_adaptor.context


def test_multi_adaptor_split_adaptors_required_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = REQUEST.copy()
    del request["level"]
    sub_adaptors = multi_adaptor.split_adaptors(
        request,
    )

    assert "mean" not in sub_adaptors.keys()
    assert "max" in sub_adaptors.keys()


def test_multi_adaptor_split_adaptors_dont_split_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = REQUEST.copy()
    request["dont_split"] = [1, 2, 3, 4]
    # Area is dont_split as default
    request["area"] = [1, 2, 3, 4]
    sub_adaptors = multi_adaptor.split_adaptors(
        request,
    )

    assert "dont_split" in sub_adaptors["mean"][1].keys()
    assert "dont_split" not in sub_adaptors["max"][1].keys()
    assert "area" in sub_adaptors["max"][1].keys()


def test_convert_format(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    multi_adaptor = multi.MultiMarsCdsAdaptor({}, {})

    assert hasattr(multi_adaptor, "convert_format")

    url = TEST_GRIB_FILE
    remote_file = requests.get(url)
    _, ext = os.path.splitext(url)

    tmp_file = f"test{ext}"
    with open(tmp_file, "wb") as f:
        f.write(remote_file.content)

    converted_files = multi_adaptor.convert_format(
        tmp_file,
        "netcdf",
    )
    assert isinstance(converted_files, list)
    assert len(converted_files) == 1
    _, out_ext = os.path.splitext(converted_files[0])
    assert out_ext == ".nc"

    test_subdir = "./test_subdir"
    os.makedirs(test_subdir, exist_ok=True)
    converted_files = multi_adaptor.convert_format(
        tmp_file, "netcdf", target_dir=test_subdir
    )
    assert isinstance(converted_files, list)
    assert len(converted_files) == 1
    _, out_ext = os.path.splitext(converted_files[0])
    assert out_ext == ".nc"
    assert "/test_subdir/" in converted_files[0]


def test_intersect_constraints_handling():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)
    sub_adaptors = multi_adaptor.split_adaptors(
        REQUEST,
    )
    for _adaptor_tag, [adaptor, _req] in sub_adaptors.items():
        assert adaptor.intersect_constraints_bool is False

    multi_adaptor = multi.MultiAdaptor(
        FORM, **ADAPTOR_CONFIG, intersect_constraints=True
    )
    sub_adaptors = multi_adaptor.split_adaptors(
        REQUEST,
    )
    for _, [adaptor, _] in sub_adaptors.items():
        assert adaptor.intersect_constraints_bool is True

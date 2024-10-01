from cads_adaptors import AbstractAdaptor
from cads_adaptors.adaptors import multi

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
            "entry_point": "cads_adaptors:DummyAdaptor",
            "values": {
                "level": ["500", "850"],
                "time": ["12:00", "00:00"],
                "param": ["Z", "T"],
                "stat": ["max"],
            },
        },
    },
}


def test_multi_adaptor_split_requests():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    split_mean = multi_adaptor.split_request(
        REQUEST, multi_adaptor.config["adaptors"]["mean"]["values"]
    )
    assert split_mean == ADAPTOR_CONFIG["adaptors"]["mean"]["values"]

    split_max = multi_adaptor.split_request(
        REQUEST, multi_adaptor.config["adaptors"]["max"]["values"]
    )
    assert split_max == ADAPTOR_CONFIG["adaptors"]["max"]["values"]


def test_multi_adaptor_split_requests_required_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = REQUEST.copy()
    del request["level"]
    split_mean_required_missing = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        required_keys=["level"],
    )
    assert split_mean_required_missing == dict()

    split_max_required_present = multi_adaptor.split_request(
        REQUEST,
        multi_adaptor.config["adaptors"]["max"]["values"],
        required_keys=["level"],
    )
    assert split_max_required_present == ADAPTOR_CONFIG["adaptors"]["max"]["values"]


def test_multi_adaptor_split_requests_dont_split_keys():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    request = REQUEST.copy()
    # dont_split_keys as list dtype
    request["dont_split"] = [1, 2, 3, 4]
    split_mean_dont_split_area = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split_area

    # dont_split_keys as integer dtype
    request["dont_split"] = "1"
    split_mean_dont_split = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # dont_split_keys as integer dtype
    request["dont_split"] = 1
    split_mean_dont_split = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # dont_split_keys as float dtype
    request["dont_split"] = 1.0
    split_mean_dont_split = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # dont_split_keys as dict dtype
    request["dont_split"] = {"a": 1}
    split_mean_dont_split = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["mean"]["values"],
        dont_split_keys=["dont_split"],
    )
    assert "dont_split" in split_mean_dont_split

    # Area is dont_split as default
    request["area"] = [1, 2, 3, 4]
    split_max_split_area = multi_adaptor.split_request(
        request,
        multi_adaptor.config["adaptors"]["max"]["values"],
    )
    assert "dont_split" not in split_max_split_area
    assert "area" in split_max_split_area


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

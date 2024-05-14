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


def test_multi_adaptor_split_adaptors():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    sub_adaptors = multi_adaptor.split_adaptors(
        REQUEST,
    )

    # Check that the sub-adaptors have the correct values
    for adaptor in ["mean", "max"]:
        sub_adaptor_request = sub_adaptors[adaptor][1]
        sub_adaptor_request.pop("download_format")
        sub_adaptor_request.pop("receipt")
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

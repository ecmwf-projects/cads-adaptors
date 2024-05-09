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


def test_multi_adaptor_split_adaptors():
    multi_adaptor = multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    sub_adaptors = multi_adaptor.split_adaptors(
        REQUEST,
    )

    for s_a in list(sub_adaptors):
        assert s_a.context is multi_adaptor.context

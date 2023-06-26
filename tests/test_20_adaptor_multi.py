from cads_adaptors import adaptor_multi

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


def test_multi_adaptor():
    multi_adaptor = adaptor_multi.MultiAdaptor(FORM, **ADAPTOR_CONFIG)

    assert multi_adaptor.adaptors["mean"].__class__.__name__ == "UrlCdsAdaptor"
    assert multi_adaptor.adaptors["max"].__class__.__name__ == "DummyAdaptor"

    split_mean = multi_adaptor.split_request(REQUEST, multi_adaptor.values["mean"])
    assert split_mean == ADAPTOR_CONFIG["adaptors"]["mean"]["values"]

    split_max = multi_adaptor.split_request(REQUEST, multi_adaptor.values["max"])
    assert split_max == ADAPTOR_CONFIG["adaptors"]["max"]["values"]

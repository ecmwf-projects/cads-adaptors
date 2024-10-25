from cads_adaptors.tools import hcube_tools


def test_remove_subsets():
    requests = [
        {
            "product_type": ["reanalysis", "ensemble_mean"],
            "variable": ["2m_temperature"],
            "data_format": "grib",
        },
        {
            "product_type": ["reanalysis", "ensemble_mean"],
            "variable": ["2m_temperature"],
        },
    ]
    hcube_tools.remove_subsets(requests)
    assert len(requests) == 1
    assert len(requests[0]) == 3
    assert "data_format" in requests[0]

    requests = [
        {
            "product_type": ["reanalysis", "ensemble_mean"],
            "variable": ["2m_temperature"],
            "data_format": "grib",
        },
        {"product_type": ["reanalysis"], "variable": ["2m_temperature"]},
        {"product_type": ["ensemble_mean"], "variable": ["2m_temperature"]},
        {"product_type": ["ensemble_spread"], "variable": ["2m_temperature"]},
    ]
    hcube_tools.remove_subsets(requests)
    assert len(requests) == 2
    assert len(requests[0]) == 3
    assert requests[0]["product_type"] == ["reanalysis", "ensemble_mean"]
    assert len(requests[1]) == 2
    assert requests[1]["product_type"] == ["ensemble_spread"]

    requests = [
        {
            "product_type": ["ensemble_mean"],
            "variable": ["2m_temperature", "total_precipitation"],
            "data_format": "grib",
        },
        {"product_type": ["ensemble_mean", "ensemble_spread"], "data_format": "grib"},
        {
            "product_type": ["ensemble_mean", "ensemble_spread"],
            "variable": ["2m_temperature"],
            "data_format": "grib",
        },
        {
            "product_type": ["ensemble_mean", "ensemble_spread"],
            "variable": ["2m_temperature", "total_precipitation"],
            "data_format": "grib",
        },
        {"variable": ["2m_temperature"], "data_format": "grib"},
        {"variable": ["2m_temperature", "total_precipitation"], "data_format": "grib"},
        {"data_format": "grib"},
        {
            "product_type": ["reanalysis"],
            "variable": ["2m_temperature"],
            "data_format": "grib",
        },
        {
            "product_type": ["reanalysis"],
            "variable": ["2m_temperature", "total_precipitation"],
            "data_format": "grib",
        },
        {"product_type": ["reanalysis"], "data_format": "grib"},
        {
            "product_type": ["ensemble_spread"],
            "variable": ["2m_temperature", "total_precipitation"],
            "data_format": "grib",
        },
        {"product_type": ["ensemble_spread"], "data_format": "grib"},
    ]
    hcube_tools.remove_subsets(requests)
    assert len(requests) == 2
    assert len(requests[0]) == 3
    assert requests[0]["product_type"] == ["ensemble_mean", "ensemble_spread"]
    assert len(requests[1]) == 2
    assert requests[1]["product_type"] == ["reanalysis"]

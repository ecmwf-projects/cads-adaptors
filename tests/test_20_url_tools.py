import os

import pytest

from cads_adaptors.tools import url_tools
from cads_adaptors.mapping import apply_mapping


@pytest.mark.parametrize(
    "urls,expected_nfiles",
    (
        (
            [
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc"
            ],
            1,
        ),
        (
            [
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc",
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.grib",
            ],
            2,
        ),
        # Check duplicate URLs are not downloaded twice
        (
            [
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc",
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.nc",
                "https://get.ecmwf.int/repository/test-data/earthkit-data/test-data/test_single.grib",
            ],
            2,
        ),
    ),
)
def test_downloaders(tmp_path, monkeypatch, urls, expected_nfiles):
    monkeypatch.chdir(tmp_path)  # try_download generates files in the working dir
    paths = url_tools.try_download(urls, context=url_tools.Context())
    assert len(paths) == expected_nfiles


@pytest.mark.parametrize(
    "anon",
    (
        True,
        False,
    ),
)
def test_ftp_download(tmp_path, ftpserver, anon):
    local_test_file = os.path.join(tmp_path, "testfile.txt")
    with open(local_test_file, "w") as f:
        f.write("This is a test file")

    ftp_url = ftpserver.put_files(local_test_file, style="url", anon=anon)
    work_dir = os.path.join(tmp_path, "work_dir")
    os.makedirs(work_dir)
    os.chdir(work_dir)
    local_test_download = url_tools.try_download(ftp_url, context=url_tools.Context())[
        0
    ]
    with open(local_test_file) as original, open(local_test_download) as downloaded:
        assert original.read() == downloaded.read()




ADAPTOR_CONFIG = {
    "collection_id": "test-adaptor-url-debugging",
    "entry_point": "cads_adaptors:UrlCdsAdaptor",
    "mapping": {
        "force": {
            "spatial_aggregation": [
                "NUT0"
            ]
        },
        "remap": {
            "energy_product_type": {
                "capacity_factor_ratio": "CFR",
                "energy": "NRG",
                "power": "PWR"
            },
            "experiment": {
                "rcp_4_5": "45",
                "rcp_8_5": "85"
            },
            "gcm": {
                "ec_earth": "IC",
                "hadgem2_es": "MO",
                "mpi_esm_lr": "MP"
            },
            "rcm": {
                "cclm4_8_17": "CC",
                "racmo22e": "RA"
            },
            "temporal_aggregation": {
                "monthly": "01m",
                "seasonal": "03m"
            },
            "variable": {
                "2m_air_temperature": "TA",
                "electricity_demand": "EDM",
                "surface_downwelling_shortwave_radiation": "GHI",
                "wind_power_generation_onshore": "WON",
                "wind_speed_at_10m": "10WS"
            }
        }
    },
    "patterns": [
        "{% if (energy_product_type is not defined or energy_product_type is none) %}{% if experiment == '85' and variable == 'GHI' and gcm == 'MO' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAMO/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_GHI_0000m_Euro_{{ spatial_aggregation }}_S195101010130_E209812312230_INS_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_NA---.csv{% endif %}{% endif %}",
        "{% if (energy_product_type is not defined or energy_product_type is none) %}{% if variable == '10WS' and gcm == 'IC' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAIC/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_WS-_0010m_Euro_{{ spatial_aggregation }}_S195101010000_E210012312100_INS_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_NA---.csv{% endif %}{% endif %}",
        "{% if (energy_product_type is not defined or energy_product_type is none) %}{% if variable == '10WS' and gcm == 'MO' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAMO/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_WS-_0010m_Euro_{{ spatial_aggregation }}_S195101010000_E209812312100_INS_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_NA---.csv{% endif %}{% endif %}",
        "{% if (energy_product_type is not defined or energy_product_type is none) %}{% if variable == 'GHI' and gcm == 'IC' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAIC/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_GHI_0000m_Euro_{{ spatial_aggregation }}_S195101010130_E210012312230_INS_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_NA---.csv{% endif %}{% endif %}",
        "{% if (energy_product_type is not defined or energy_product_type is none) %}{% if variable == 'TA' and gcm == 'IC' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAIC/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_TA-_0002m_Euro_{{ spatial_aggregation }}_S195101010000_E210012312100_INS_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_NA---.csv{% endif %}{% endif %}",
        "{% if (energy_product_type is not defined or energy_product_type is none) %}{% if variable == 'TA' and gcm == 'MO' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAMO/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_TA-_0002m_Euro_{{ spatial_aggregation }}_S195101010000_E209812312100_INS_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_NA---.csv{% endif %}{% endif %}",
        "{% if experiment == '45' and variable == 'EDM' and rcm == 'CC' and gcm == 'MP' and energy_product_type in ['NRG','PWR'] %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/CCMP/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_EDM_0000m_Euro_{{ spatial_aggregation }}_S197001010000_E210012310000_{{ energy_product_type }}_TIM_{{ temporal_aggregation }}_NA-_noc_org_01_RCP{{ experiment }}_NA---_GamNT.csv{% endif %}",
        "{% if variable == 'EDM' and rcm == 'RA' and gcm == 'IC' and energy_product_type in ['NRG','PWR'] %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/RAIC/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_EDM_0000m_Euro_{{ spatial_aggregation }}_S197001010000_E210012310000_{{ energy_product_type }}_TIM_{{ temporal_aggregation }}_NA-_noc_org_01_RCP{{ experiment }}_NA---_GamNT.csv{% endif %}",
        "{% if variable == 'EDM' and rcm == 'RA' and gcm == 'MO' and energy_product_type in ['NRG','PWR'] %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/RAMO/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_EDM_0000m_Euro_{{ spatial_aggregation }}_S197001010000_E209812310000_{{ energy_product_type }}_TIM_{{ temporal_aggregation }}_NA-_noc_org_01_RCP{{ experiment }}_NA---_GamNT.csv{% endif %}",
        "{% if variable == 'WON' and rcm == 'RA' and gcm == 'IC' and energy_product_type == 'CFR' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/RAIC/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_WON_0100m_Euro_{{ spatial_aggregation }}_S195101010000_E210012312100_{{ energy_product_type }}_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_PhM01.csv{% endif %}",
        "{% if variable == 'WON' and rcm == 'RA' and gcm == 'MO' and energy_product_type == 'CFR' %}http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/RAMO/RCP{{ experiment }}/{{ variable }}/{{ spatial_aggregation }}/P_CMI5_{{ rcm }}{{ gcm }}_CM20_WON_0100m_Euro_{{ spatial_aggregation }}_S195101010000_E209812312100_{{ energy_product_type }}_TIM_{{ temporal_aggregation }}_NA-_cdf_org_01_RCP{{ experiment }}_NA---_PhM01.csv{% endif %}"
    ]
}

CONSTRAINTS = [
    {"experiment": ["rcp_4_5"], "gcm": ["ec_earth"], "rcm": ["racmo22e"], "temporal_aggregation": ["monthly", "seasonal"], "variable": ["wind_speed_at_10m", "surface_downwelling_shortwave_radiation", "2m_air_temperature"]},
    {"experiment": ["rcp_4_5"], "gcm": ["hadgem2_es"], "rcm": ["racmo22e"], "temporal_aggregation": ["monthly", "seasonal"], "variable": ["wind_speed_at_10m", "2m_air_temperature"]},
    {"experiment": ["rcp_8_5"], "gcm": ["ec_earth", "hadgem2_es"], "rcm": ["racmo22e"], "temporal_aggregation": ["monthly", "seasonal"], "variable": ["wind_speed_at_10m", "surface_downwelling_shortwave_radiation", "2m_air_temperature"]},
    {"energy_product_type": ["capacity_factor_ratio"], "experiment": ["rcp_4_5", "rcp_8_5"], "gcm": ["ec_earth", "hadgem2_es"], "rcm": ["racmo22e"], "temporal_aggregation": ["monthly", "seasonal"], "variable": ["wind_power_generation_onshore"]},
    {"energy_product_type": ["energy", "power"], "experiment": ["rcp_4_5"], "gcm": ["mpi_esm_lr"], "rcm": ["cclm4_8_17"], "temporal_aggregation": ["monthly", "seasonal"], "variable": ["electricity_demand"]},
    {"energy_product_type": ["energy", "power"], "experiment": ["rcp_4_5", "rcp_8_5"], "gcm": ["ec_earth", "hadgem2_es"], "rcm": ["racmo22e"], "temporal_aggregation": ["monthly", "seasonal"], "variable": ["electricity_demand"]}
]

def test_find_all_urls():
    request = {
        "variable": ["surface_downwelling_shortwave_radiation", "electricity_demand",],
        "spatial_aggregation": ["country_level"],
        "temporal_aggregation": ["monthly"],
        "energy_product_type": ["energy"],
        "experiment": ["rcp_4_5", "rcp_8_5"],
        "rcm": ["racmo22e", "cclm4_8_17"],
        "gcm": ["hadgem2_es", "mpi_esm_lr"],
    }

    expected_urls = [
        "http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/CLIM/RAMO/RCP85/GHI/NUT0/P_CMI5_RAMO_CM20_GHI_0000m_Euro_NUT0_S195101010130_E209812312230_INS_TIM_01m_NA-_cdf_org_01_RCP85_NA---_NA---.csv"
        "http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/RAMO/RCP45/EDM/NUT0/P_CMI5_RAMO_CM20_EDM_0000m_Euro_NUT0_S197001010000_E209812310000_NRG_TIM_01m_NA-_noc_org_01_RCP45_NA---_GamNT.csv",
        "http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/CCMP/RCP45/EDM/NUT0/P_CMI5_CCMP_CM20_EDM_0000m_Euro_NUT0_S197001010000_E210012310000_NRG_TIM_01m_NA-_noc_org_01_RCP45_NA---_GamNT.csv",
        "http://energy-tools.copernicus-climate.eu/C3S_ENERGY/PROJ/ENER/RAMO/RCP85/EDM/NUT0/P_CMI5_RAMO_CM20_EDM_0000m_Euro_NUT0_S197001010000_E209812310000_NRG_TIM_01m_NA-_noc_org_01_RCP85_NA---_GamNT.csv",
    ]
    mapped_request = apply_mapping(request, ADAPTOR_CONFIG["mapping"])

    requests_urls = url_tools.requests_to_urls(mapped_request, ADAPTOR_CONFIG["patterns"])
    requests_urls = [req_url["url"] for req_url in requests_urls]

    assert len(requests_urls) == 4
    # At first, we just want the expected_urls to be in the requests urls
    assert all(url in requests_urls for url in expected_urls)

    #in the future, we may expect that the requests urls are exactly the expected urls
    # assert requests_urls == expected_urls


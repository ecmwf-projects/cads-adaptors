from cads_adaptors.tools import constraint_tools


def test_apply_constraints():
    constraints = [
        {
            "experiment": ["rcp_4_5"],
            "gcm": ["ec_earth"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": [
                "wind_speed_at_10m",
                "surface_downwelling_shortwave_radiation",
                "2m_air_temperature",
            ],
        },
        {
            "experiment": ["rcp_4_5"],
            "gcm": ["hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["wind_speed_at_10m", "2m_air_temperature"],
        },
        {
            "experiment": ["rcp_8_5"],
            "gcm": ["ec_earth", "hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": [
                "wind_speed_at_10m",
                "surface_downwelling_shortwave_radiation",
                "2m_air_temperature",
            ],
        },
        {
            "energy_product_type": ["capacity_factor_ratio"],
            "experiment": ["rcp_4_5", "rcp_8_5"],
            "gcm": ["ec_earth", "hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["wind_power_generation_onshore"],
        },
        {
            "energy_product_type": ["energy", "power"],
            "experiment": ["rcp_4_5"],
            "gcm": ["mpi_esm_lr"],
            "rcm": ["cclm4_8_17"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["electricity_demand"],
        },
        {
            "energy_product_type": ["energy", "power"],
            "experiment": ["rcp_4_5", "rcp_8_5"],
            "gcm": ["ec_earth", "hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["electricity_demand"],
        },
    ]
    request = {
        "variable": ["surface_downwelling_shortwave_radiation", "electricity_demand"],
        "spatial_aggregation": ["country_level"],
        "temporal_aggregation": ["monthly"],
        "energy_product_type": ["energy"],
        "experiment": ["rcp_8_5"],
        "rcm": ["racmo22e"],
        "gcm": ["hadgem2_es"],
    }
    expected = [
        {
            "variable": ["surface_downwelling_shortwave_radiation"],
            "spatial_aggregation": ["country_level"],
            "temporal_aggregation": ["monthly"],
            "experiment": ["rcp_8_5"],
            "rcm": ["racmo22e"],
            "gcm": ["hadgem2_es"],
        },
        {
            "variable": ["electricity_demand"],
            "spatial_aggregation": ["country_level"],
            "temporal_aggregation": ["monthly"],
            "energy_product_type": ["energy"],
            "experiment": ["rcp_8_5"],
            "rcm": ["racmo22e"],
            "gcm": ["hadgem2_es"],
        },
    ]
    actual = constraint_tools.intersect_constraints(request, constraints)
    assert actual == expected

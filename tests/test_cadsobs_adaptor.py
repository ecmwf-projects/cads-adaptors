import time
from pathlib import Path

import h5netcdf

from cads_adaptors import ObservationsAdaptor

CDM_LITE_VARIABLES = {
    "mandatory": [
        "observation_id",
        "observed_variable",
        "units",
        "actual_time",
        "agency",
        "observation_value",
        "observation_value_total_uncertainty",
        "city",
        "country",
        "height_of_station_above_sea_level",
        "latitude",
        "longitude",
        "z_coordinate",
        "data_policy_licence",
        "platform_type",
        "primary_station_id",
        "qc_method",
        "quality_flag",
        "report_id",
        "report_timestamp",
        "report_meaning_of_time_stamp",
        "report_duration",
        "report_type",
        "sensor_id",
        "source_id",
        "station_name",
        "z_coordinate_type",
    ],
    "optional": [
        "source_id",
        "product_citation",
        "data_policy_licence",
        "homogenization_adjustment",
        "homogenization_method",
        "number_of_observations",
        "secondary_id",
        "sensor_model",
        "station_type",
        "platform_type",
        "report_type",
        "station_automation",
        "profile_id",
        "z_coordinate",
        "spatial_representativeness",
        "exposure_of_sensor",
        "fg_depar@body",
        "an_depar@body",
    ],
    "auxiliary": [
        "total_uncertainty",
        "positive_total_uncertainty",
        "negative_total_uncertainty",
        "random_uncertainty",
        "positive_random_uncertainty",
        "negative_random_uncertainty",
        "systematic_uncertainty",
        "positive_systematic_uncertainty",
        "negative_systematic_uncertainty",
        "quasisystematic_uncertainty",
        "positive_quasisystematic_uncertainty",
        "negative_quasisystematic_uncertainty",
        "flag",
    ],
}


class MockerCadsobsApiClient:
    def __init__(self, baseurl: str):
        pass

    def get_service_definition(self, dataset: str) -> dict:
        return {
            "global_attributes": {
                "contactemail": "https://support.ecmwf.int",
                "licence_list": "20180314_Copernicus_License_V1.1",
                "responsible_organisation": "ECMWF",
            }
        }

    def get_cdm_lite_variables(self):
        return CDM_LITE_VARIABLES

    def get_objects_to_retrieve(
        self, dataset_name: str, mapped_request: dict, size_limit: int
    ) -> list[str]:
        return [
            "https://object-store.os-api.cci2.ecmwf.int/"
            "cds2-obs-alpha-insitu-observations-near-surface-temperature-us/"
            "insitu-observations-near-surface-temperature-us-climate-reference-network_USCRN_DAILY_200808_30.0_-150.0.nc"
        ]

    def get_aux_var_mapping(
        self, dataset: str, source: str
    ) -> dict[str, list[dict[str, str]]]:
        return {
            "accumulated_precipitation": [],
            "air_temperature": [
                {
                    "auxvar": "air_temperature_mean_positive_total_uncertainty",
                    "metadata_name": "positive_total_uncertainty",
                },
                {
                    "auxvar": "air_temperature_mean_negative_total_uncertainty",
                    "metadata_name": "negative_total_uncertainty",
                },
            ],
            "daily_maximum_air_temperature": [
                {
                    "auxvar": "air_temperature_max_positive_total_uncertainty",
                    "metadata_name": "positive_total_uncertainty",
                },
                {
                    "auxvar": "air_temperature_max_negative_total_uncertainty",
                    "metadata_name": "negative_total_uncertainty",
                },
            ],
            "daily_maximum_relative_humidity": [],
            "daily_minimum_air_temperature": [
                {
                    "auxvar": "air_temperature_min_positive_total_uncertainty",
                    "metadata_name": "positive_total_uncertainty",
                },
                {
                    "auxvar": "air_temperature_min_negative_total_uncertainty",
                    "metadata_name": "negative_total_uncertainty",
                },
            ],
            "daily_minimum_relative_humidity": [],
            "relative_humidity": [],
        }


def test_adaptor(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_request = {
        "time_aggregation": "daily",
        "format": "netCDF",
        "variable": [
            "maximum_air_temperature",
            "maximum_air_temperature_negative_total_uncertainty",
            "maximum_air_temperature_positive_total_uncertainty",
        ],
        "year": ["2007"],
        "month": ["11"],
        "day": [
            "01",
            "02",
            "03",
        ],
        "_timestamp": str(time.time()),
    }
    test_form = {}
    # + "/v1/AUTH_{public_user}" will be needed to work with S3 ceph public urls, but it
    # is not needed for this test
    test_adaptor_config = {
        "entry_point": "cads_adaptors:ObservationsAdaptor",
        "collection_id": "insitu-observations-near-surface-temperature-us-climate-reference-network",
        "obs_api_url": "http://localhost:8000",
        "mapping": {
            "remap": {
                "time_aggregation": {
                    "daily": "USCRN_DAILY",
                    "hourly": "USCRN_HOURLY",
                    "monthly": "USCRN_MONTHLY",
                    "sub_hourly": "USCRN_SUBHOURLY",
                },
                "variable": {
                    "maximum_air_temperature": "daily_maximum_air_temperature",
                    "maximum_air_temperature_negative_total_uncertainty": "air_temperature_max_negative_total_uncertainty",  # noqa E501
                    "maximum_air_temperature_positive_total_uncertainty": "air_temperature_max_positive_total_uncertainty",  # noqa E501
                    "maximum_relative_humidity": "daily_maximum_relative_humidity",
                    "maximum_soil_temperature": "hourly_maximum_soil_temperature",
                    "maximum_soil_temperature_flag": "hourly_maximum_soil_temperature_flag",  # noqa E501
                    "maximum_solar_irradiance": "hourly_maximum_downward_shortwave_irradiance_at_earth_surface",  # noqa E501
                    "maximum_solar_irradiance_quality_flag": "hourly_maximum_downward_shortwave_irradiance_at_earth_surface_quality_flag",  # noqa E501
                    "mean_air_temperature_negative_total_uncertainty": "air_temperature_mean_negative_total_uncertainty",  # noqa E501
                    "mean_air_temperature_positive_total_uncertainty": "air_temperature_mean_positive_total_uncertainty",  # noqa E501
                    "minimum_air_temperature": "daily_minimum_air_temperature",
                    "minimum_air_temperature_negative_total_uncertainty": "air_temperature_min_negative_total_uncertainty",  # noqa E501
                    "minimum_air_temperature_positive_total_uncertainty": "air_temperature_min_positive_total_uncertainty",  # noqa E501
                    "minimum_relative_humidity": "daily_minimum_relative_humidity",
                    "minimum_soil_temperature": "hourly_minimum_soil_temperature",
                    "minimum_soil_temperature_quality_flag": "hourly_minimum_soil_temperature_quality_flag",  # noqa E501
                    "minimum_solar_irradiance": "hourly_minimum_downward_shortwave_irradiance_at_earth_surface",  # noqa E501
                    "minimum_solar_irradiance_quality_flag": "hourly_minimum_downward_shortwave_irradiance_at_earth_surface_quality_flag",  # noqa E501
                    "solar_irradiance": "downward_shortwave_irradiance_at_earth_surface",
                    "solar_irradiance_quality_flag": "downward_shortwave_irradiance_at_earth_surface_quality_flag",  # noqa E501
                },
            },
            "format": {"netcdf": "netCDF"},
            "rename": {"time_aggregation": "dataset_source", "variable": "variables"},
            "force": {},
        },
    }
    adaptor = ObservationsAdaptor(test_form, **test_adaptor_config)
    result = adaptor.retrieve(test_request)
    tempfile = Path(tmp_path, "test_adaptor.nc")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    actual = h5netcdf.File(tempfile)
    assert actual.dimensions["index"].size > 0

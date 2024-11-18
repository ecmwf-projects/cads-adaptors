import time
from pathlib import Path
from unittest.mock import Mock

import h5netcdf
import pytest

from cads_adaptors import Context, ObservationsAdaptor
from cads_adaptors.adaptors.cadsobs.api_client import CadsobsApiClient
from cads_adaptors.exceptions import CadsObsConnectionError, InvalidRequest

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
    def __init__(self, baseurl: str, context: Context):
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
            "cds2-obs-dev-insitu-observations-near-surface-temperature-us-cl/"
            "insitu-observations-near-surface-temperature-us-climate-reference-network_uscrn_daily_200808_30.0_-150.0.nc"
        ]


class ClientErrorMockerCadsobsApiClient(MockerCadsobsApiClient):
    def get_objects_to_retrieve(
        self, dataset_name: str, mapped_request: dict, size_limit: int
    ):
        raise RuntimeError("This is a test error")


class BackendErrorCadsobsApiClient(CadsobsApiClient):
    def _send_request(self, endpoint, method, payload):
        response = self.requests.Response()
        response.code = "expired"
        response.error_type = "expired"
        response.status_code = 400
        response._content = (
            b'{"detail": {"message" : "Error: something failed somehow", '
            b'"traceback": "this is a traceback" }}'
        )
        return response


TEST_REQUEST = {
    "time_aggregation": "daily",
    "format": "netCDF",
    "variable": ["maximum_air_temperature", "maximum_relative_humidity"],
    "year": ["2007"],
    "month": ["11"],
    "day": [
        "01",
        "02",
        "03",
    ],
    "_timestamp": str(time.time()),
}

TEST_ADAPTOR_CONFIG = {
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
                "maximum_relative_humidity": "daily_maximum_relative_humidity",
                "maximum_soil_temperature": "hourly_maximum_soil_temperature",
                "maximum_solar_irradiance": "hourly_maximum_downward_shortwave_irradiance_at_earth_surface",  # noqa E501
                "minimum_air_temperature": "daily_minimum_air_temperature",
                "minimum_relative_humidity": "daily_minimum_relative_humidity",
                "minimum_soil_temperature": "hourly_minimum_soil_temperature",
                "minimum_solar_irradiance": "hourly_minimum_downward_shortwave_irradiance_at_earth_surface",  # noqa E501
                "solar_irradiance": "downward_shortwave_irradiance_at_earth_surface",
            },
        },
        "format": {"netcdf": "netCDF"},
        "rename": {"time_aggregation": "dataset_source", "variable": "variables"},
        "force": {},
    },
}


def test_adaptor(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    result = adaptor.retrieve(TEST_REQUEST)
    tempfile = Path(tmp_path, "test_adaptor.nc")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    actual = h5netcdf.File(tempfile)
    assert actual.dimensions["index"].size > 0


def test_adaptor_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    test_request_csv = TEST_REQUEST.copy()
    test_request_csv["format"] = "csv"
    result = adaptor.retrieve(test_request_csv)
    tempfile = Path(tmp_path, "test_adaptor.csv")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    file_lines = tempfile.read_text().split("\n")
    assert "# daily_maximum_air_temperature [K]" in file_lines
    assert "# daily_maximum_relative_humidity [%]" in file_lines


def test_adaptor_error(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        ClientErrorMockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    adaptor.context.add_user_visible_error = Mock()
    with pytest.raises(RuntimeError) as e:
        adaptor.retrieve(TEST_REQUEST)
    expected_error = "RuntimeError('This is a test error')"
    assert repr(e.value) == expected_error
    adaptor.context.add_user_visible_error.assert_called_with(expected_error)


def test_adaptor_wrong_key(monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}
    test_request = TEST_REQUEST.copy()
    test_request.pop("time_aggregation")
    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    with pytest.raises(InvalidRequest):
        adaptor.retrieve(test_request)

    test_request["time_aggregation_dasdas"] = "daily"
    with pytest.raises(InvalidRequest):
        adaptor.retrieve(test_request)


def test_adaptor_wrong_value(monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}
    test_request = TEST_REQUEST.copy()
    test_request["variable"] = "FAKE_VARIABLE"
    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    with pytest.raises(InvalidRequest):
        adaptor.retrieve(test_request)

    # And dataset_source variables
    test_request["time_aggregation"] = "FAKE_VARIABLE"
    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    with pytest.raises(InvalidRequest):
        adaptor.retrieve(test_request)


def test_connection_error(tmp_path):
    test_form = {}
    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    adaptor.context.add_user_visible_error = Mock()
    with pytest.raises(CadsObsConnectionError) as e:
        adaptor.retrieve(TEST_REQUEST)
    expected_error = 'CadsObsConnectionError("Can\'t connect to the observations API.")'
    assert repr(e.value) == expected_error
    adaptor.context.add_user_visible_error.assert_called_with(expected_error)


def test_api_error(tmp_path, monkeypatch):
    test_form = {}
    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        BackendErrorCadsobsApiClient,
    )
    adaptor.context.add_user_visible_error = Mock()
    adaptor.context.add_stderr = Mock()
    with pytest.raises(CadsObsConnectionError):
        adaptor.retrieve(TEST_REQUEST)

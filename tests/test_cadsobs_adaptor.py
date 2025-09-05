import zipfile
from pathlib import Path
from unittest.mock import Mock

import h5netcdf
import pytest

from cads_adaptors import Context, ObservationsAdaptor
from cads_adaptors.adaptors.cadsobs.api_client import CadsobsApiClient
from cads_adaptors.exceptions import CadsObsConnectionError, InvalidRequest

# get numbered vars programatically, as they are to many to add by hand to
# the list
number_of_uncertainty_types = 17
uncertainty_numbered_vars = [
    f"{unc_var}{n}"
    for n in range(number_of_uncertainty_types + 1)
    for unc_var in ["uncertainty_value", "uncertainty_type", "uncertainty_units"]
]

CDM_LITE_VARIABLES: dict[str, list[str] | dict] = {
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
    ]
    + uncertainty_numbered_vars,
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
    "attributes": {
        "uncertainty_value1": {"long_name": "random_uncertainty"},
        "uncertainty_value10": {"long_name": "negative_systematic_uncertainty"},
        "uncertainty_value11": {"long_name": "positive_systematic_uncertainty"},
        "uncertainty_value12": {"long_name": "negative_quasisystematic_uncertainty"},
        "uncertainty_value13": {"long_name": "positive_quasisystematic_uncertainty"},
        "uncertainty_value14": {"long_name": "negative_structured_random_uncertainty"},
        "uncertainty_value15": {"long_name": "positive_structured_random_uncertainty"},
        "uncertainty_value16": {"long_name": "negative_total_uncertainty"},
        "uncertainty_value17": {"long_name": "positive_total_uncertainty"},
        "uncertainty_value2": {"long_name": "systematic_uncertainty"},
        "uncertainty_value3": {"long_name": "quasisystematic_uncertainty"},
        "uncertainty_value4": {"long_name": "structured_random_uncertainty"},
        "uncertainty_value5": {"long_name": "total_uncertainty"},
        "uncertainty_value6": {
            "long_name": "ozone_partial_pressure_total_uncertainty_uncertainty"
        },
        "uncertainty_value7": {
            "long_name": "ozone_partial_pressure_percentage_uncertainty_uncertainty"
        },
        "uncertainty_value8": {"long_name": "negative_random_uncertainty"},
        "uncertainty_value9": {"long_name": "positive_random_uncertainty"},
    },
}


TEST_REQUEST = {
    "time_aggregation": "daily",
    "format": "netCDF",
    "variable": ["maximum_air_temperature", "maximum_relative_humidity"],
    "year": "2007",
    "month": ["11"],
    "day": [
        "01",
        "02",
        "03",
    ],
    "area": ["50", "-150", "30", "-100"],
}

TEST_REQUEST_CUON = {
    "version": "1_1_0",
    "variable": ["air_dewpoint", "air_temperature"],
    "year": ["1965"],
    "month": ["07"],
    "day": ["01", "02"],
    "data_format": "netcdf",
}


TEST_ADAPTOR_CONFIG = {
    "entry_point": "cads_adaptors:ObservationsAdaptor",
    "collection_id": "insitu-observations-near-surface-temperature-us-climate-reference-network",
    "obs_api_url": "http://localhost:8000",
    "mapping": {
        "remap": {
            "time_aggregation": {
                "daily": "uscrn_daily",
                "hourly": "uscrn_hourly",
                "monthly": "uscrn_monthly",
                "sub_hourly": "uscrn_subhourly",
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
    "licences": ["licence-to-use-copernicus-products", "uscrn-data-policy"],
}

TEST_ADAPTOR_CONFIG_CUON = {
    "costing": {"max_costs": {"size": 1600}},
    "entry_point": "cads_adaptors:ObservationsAdaptor",
    "intersect_constraints": True,
    "collection_id": "insitu-comprehensive-upper-air-observation-network",
    "obs_api_url": "http://localhost:8000",
    "mapping": {
        "force": {"dataset_source": ["CUON"]},
        "remap": {
            "data_format": {"netcdf": "netCDF"},
            "version": {"1_1_0": "1.1.0"},
        },
        "rename": {"data_format": "format", "variable": "variables"},
    },
}

S3_URL = "https://sites.ecmwf.int/repository/data-store-service/"
CUON_DISABLED_FIELDS = [
    "report_type",
    "report_duration",
    "station_type",
    "secondary_id",
]


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
        self, dataset_name: str, mapped_request: dict
    ) -> list[str]:
        if (
            dataset_name
            == "insitu-observations-near-surface-temperature-us-climate-reference-network"
        ):
            return [
                S3_URL
                + "insitu-observations-near-surface-temperature-us-climate-reference-network"
                + "_1.0.0_uscrn_daily_200808_30.0_-150.0.nc"
            ]
        elif dataset_name == "insitu-comprehensive-upper-air-observation-network":
            return [
                S3_URL
                + "insitu-comprehensive-upper-air-observation-network_1.1.0_CUON_196507_-90.0_-180.0.nc"
            ]
        else:
            raise RuntimeError(f"Unknown dataset {dataset_name}")

    def get_disabled_fields(self, dataset_name: str, dataset_source: str) -> list[str]:
        """Get the list of fields that are disabled for the given dataset."""
        if (
            dataset_name
            == "insitu-observations-near-surface-temperature-us-climate-reference-network"
        ):
            return []
        elif dataset_name == "insitu-comprehensive-upper-air-observation-network":
            return CUON_DISABLED_FIELDS
        else:
            raise RuntimeError(f"Unknown dataset {dataset_name}")


class ClientErrorMockerCadsobsApiClient(MockerCadsobsApiClient):
    def get_objects_to_retrieve(self, dataset_name: str, mapped_request: dict):
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


def test_adaptor(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
    result = adaptor.retrieve(TEST_REQUEST)
    tempfile = Path(tmp_path, "test_adaptor.nc")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    actual = h5netcdf.File(tempfile)
    assert actual.dimensions["index"].size > 0
    # Check if the parameters have been properly mapped.
    assert adaptor.mapped_request == {
        "dataset_source": "uscrn_daily",
        "format": "netCDF",
        "variables": [
            "daily_maximum_air_temperature",
            "daily_maximum_relative_humidity",
        ],
        "year": [2007],
        "month": [11],
        "day": [1, 2, 3],
        "latitude_coverage": ["30", "50"],
        "longitude_coverage": ["-150", "-100"],
    }


def test_adaptor_cuon(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG_CUON)
    result = adaptor.retrieve(TEST_REQUEST_CUON)
    tempfile = Path(tmp_path, "test_adaptor.nc")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    actual = h5netcdf.File(tempfile)
    assert actual.dimensions["index"].size > 0
    assert not any([f in actual for f in CUON_DISABLED_FIELDS])


def test_adaptor_estimate_costs(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}
    adaptor = ObservationsAdaptor(test_form, **TEST_ADAPTOR_CONFIG)
    test_request_noarea = TEST_REQUEST.copy()
    test_request_noarea.pop("area")
    costs_noarea = adaptor.estimate_costs(test_request_noarea)
    costs = adaptor.estimate_costs(TEST_REQUEST)
    assert costs_noarea["number_of_fields"] > costs["number_of_fields"]
    assert costs_noarea["size"] > costs["size"]


def test_adaptor_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
    test_request_csv = TEST_REQUEST.copy()
    test_request_csv["format"] = "csv"
    result = adaptor.retrieve(test_request_csv)
    with zipfile.ZipFile(result, "r") as zipf:
        file_lines = zipf.read(name=zipf.namelist()[0]).decode("UTF-8").split("\n")
    assert len(file_lines) > 0
    assert file_lines[0] != ""
    assert "# daily_maximum_air_temperature [K]" in file_lines
    assert "# daily_maximum_relative_humidity [%]" in file_lines


def test_adaptor_error(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        ClientErrorMockerCadsobsApiClient,
    )
    test_form = {}

    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
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
    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
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
    test_request["variable"] = ["FAKE_VARIABLE"]
    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
    with pytest.raises(InvalidRequest):
        adaptor.retrieve(test_request)

    # And dataset_source variables
    test_request["time_aggregation"] = "FAKE_VARIABLE"
    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
    with pytest.raises(InvalidRequest):
        adaptor.retrieve(test_request)


def test_connection_error(tmp_path):
    test_form = {}
    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
    adaptor.context.add_user_visible_error = Mock()
    with pytest.raises(CadsObsConnectionError) as e:
        adaptor.retrieve(TEST_REQUEST)
    expected_error = 'CadsObsConnectionError("Can\'t connect to the observations API.")'
    assert repr(e.value) == expected_error
    adaptor.context.add_user_visible_error.assert_called_with(expected_error)


def test_api_error(tmp_path, monkeypatch):
    test_form = {}
    adaptor = ObservationsAdaptor(form=test_form, **TEST_ADAPTOR_CONFIG)
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        BackendErrorCadsobsApiClient,
    )
    adaptor.context.add_user_visible_error = Mock()
    adaptor.context.add_stderr = Mock()
    with pytest.raises(CadsObsConnectionError):
        adaptor.retrieve(TEST_REQUEST)

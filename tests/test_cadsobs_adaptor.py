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
        "max_positive_total_uncertainty",
        "max_negative_total_uncertainty",
        "min_positive_total_uncertainty",
        "min_negative_total_uncertainty",
        "random_uncertainty",
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
        self, dataset_name: str, mapped_request: dict
    ) -> list[str]:
        return [
            "https://object-store.os-api.cci2.ecmwf.int/"
            "cds2-obs-alpha-insitu-observations-woudc-ozone-total-column-and/"
            "insitu-observations-woudc-ozone-total-column-and-profiles_TotalOzone_201102_0.0_0.0.nc"
        ]


def test_adaptor(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_request = {
        "observation_type": ["total_column"],
        "format": "netCDF",
        "variable": ["total_ozone_column", "total_ozone_column_total_uncertainty"],
        "year": ["2011"],
        "month": ["02"],
        "day": [
            "01",
            "02",
            "03",
        ],
    }
    test_form = {}
    # + "/v1/AUTH_{public_user}" will be needed to work with S3 ceph public urls, but it
    # is not needed for this test
    test_adaptor_config = {
        "entry_point": "cads_adaptors:ObservationsAdaptor",
        "collection_id": "insitu-observations-woudc-ozone-total-column-and-profiles",
        "obs_api_url": "http://localhost:8000",
        "mapping": {
            "remap": {"network_type": {"epn_repro2": "EPN", "igs_daily": "IGS"}},
            "rename": {"observation_type": "dataset_source", "variable": "variables"},
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
    # TODO: Shouldn't be observation_id??
    assert actual.dimensions["index"].size > 0

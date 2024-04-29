import datetime
from pathlib import Path

import xarray
from numpy import nan

0


def mocked_retrieve_observations(
    session, storage_url, retrieve_args, output_dir: Path, size_limit: int
) -> Path:
    dataset = xarray.Dataset.from_dict(
        {
            "coords": {},
            "attrs": {
                "featureType": "point",
                "contactemail": "https://support.ecmwf.int",
                "licence_list": "20180314_Copernicus_License_V1.1",
                "responsible_organisation": "ECMWF",
            },
            "dims": {"index": 5},
            "data_vars": {
                "observation_id": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [101, 102, 103, 104, 105],
                },
                "report_id": {"dims": ("index",), "attrs": {}, "data": [2, 2, 2, 2, 2]},
                "platform_type": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [b"STN", b"STN", b"STN", b"STN", b"STN"],
                },
                "primary_station_id": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [b"7", b"7", b"7", b"7", b"7"],
                },
                "height_of_station_above_sea_level": {
                    "dims": ("index",),
                    "attrs": {"units": "m"},
                    "data": [283.0, 283.0, 283.0, 283.0, 283.0],
                },
                "report_timestamp": {
                    "dims": ("index",),
                    "attrs": {"standard_name": "time"},
                    "data": [
                        datetime.datetime(1969, 2, 13, 5, 45),
                        datetime.datetime(1969, 2, 13, 5, 45),
                        datetime.datetime(1969, 2, 13, 5, 45),
                        datetime.datetime(1969, 2, 13, 5, 45),
                        datetime.datetime(1969, 2, 13, 5, 45),
                    ],
                },
                "z_coordinate": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [98510.0, 94160.0, 92500.0, 90000.0, 86070.0],
                },
                "z_coordinate_type": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [
                        b"pressure (Pa)",
                        b"pressure (Pa)",
                        b"pressure (Pa)",
                        b"pressure (Pa)",
                        b"pressure (Pa)",
                    ],
                },
                "observed_variable": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [
                        b"air_temperature",
                        b"air_temperature",
                        b"air_temperature",
                        b"air_temperature",
                        b"air_temperature",
                    ],
                },
                "observation_value": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [
                        290.45001220703125,
                        287.8500061035156,
                        286.8500061035156,
                        285.25,
                        283.45001220703125,
                    ],
                },
                "units": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [b"Kelvin", b"Kelvin", b"Kelvin", b"Kelvin", b"Kelvin"],
                },
                "sensor_id": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [b"KC-68", b"KC-68", b"KC-68", b"KC-68", b"KC-68"],
                },
                "longitude|observations_table": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [nan, nan, nan, nan, nan],
                },
                "latitude|observations_table": {
                    "dims": ("index",),
                    "attrs": {},
                    "data": [nan, nan, nan, nan, nan],
                },
                "longitude": {
                    "dims": ("index",),
                    "attrs": {
                        "cdm_table": "station_configuration",
                        "standard_name": "longitude",
                        "units": "degrees_east",
                    },
                    "data": [
                        130.60000610351562,
                        130.60000610351562,
                        130.60000610351562,
                        130.60000610351562,
                        130.60000610351562,
                    ],
                },
                "latitude": {
                    "dims": ("index",),
                    "attrs": {
                        "cdm_table": "station_configuration",
                        "standard_name": "latitude",
                        "units": "degrees_north",
                    },
                    "data": [
                        31.600000381469727,
                        31.600000381469727,
                        31.600000381469727,
                        31.600000381469727,
                        31.600000381469727,
                    ],
                },
            },
        }
    )
    output_file = Path(output_dir, "test_cadsobs_adaptor.nc")
    dataset.to_netcdf(output_file)
    return output_file


CDM_LITE_VARIABLES = [
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
    "source_id",
    "product_citation",
    "data_policy_licence",
    "homogenization_adjustment",
    "homogenization_method",
    "uncertainty_type",
    "uncertainty_value",
    "uncertainty_units",
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


# @pytest.mark.skip("Depends on cdsobs")
def test_adaptor(tmp_path, monkeypatch):
    from cads_adaptors import ObservationsAdaptor

    monkeypatch.setattr(
        "cads_adaptors.adaptors.cadsobs.adaptor.CadsobsApiClient",
        MockerCadsobsApiClient,
    )
    test_request = {
        "observation_type": ["total_column"],
        "format": "netCDF",
        "variable": ["total_ozone_column"],
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
            "remap": {
                "observation_type": {
                    "total_column": "TotalOzone",
                    "vertical_profile": "OzoneSonde",
                }
            },
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
    actual = xarray.open_dataset(tempfile)
    assert actual.observation_id.size > 0

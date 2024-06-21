import os
import tempfile

import requests

from cads_adaptors.tools import convertors

TEST_GRIB_FILE = (
    "https://get.ecmwf.int/repository/test-data/cfgrib/era5-levels-members.grib"
)



def test_open_grib():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_grib_file = os.path.join(tmpdirname, "test.grib")
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        netcdf_files = convertors.grib_to_netcdf_files(tmp_grib_file)
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

        netcdf_files = convertors.grib_to_netcdf_files(
            tmp_grib_file, compression_options="default"
        )
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

def test_grib_to_netcdf():
    grib_file = requests.get(TEST_GRIB_FILE)
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_grib_file = os.path.join(tmpdirname, "test.grib")
        with open(tmp_grib_file, "wb") as f:
            f.write(grib_file.content)

        netcdf_files = convertors.grib_to_netcdf_files(tmp_grib_file)
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

        netcdf_files = convertors.grib_to_netcdf_files(
            tmp_grib_file, compression_options="default"
        )
        assert isinstance(netcdf_files, list)
        assert len(netcdf_files) == 1

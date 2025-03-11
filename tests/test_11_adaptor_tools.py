import pytest

from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools.adaptor_tools import handle_data_format, handle_data_and_download_format

def test_handle_data_format():
    # Test single string inputs
    assert handle_data_format("netcdf4") == "netcdf"
    assert handle_data_format("netcdf") == "netcdf"
    assert handle_data_format("nc") == "netcdf"
    assert handle_data_format("grib") == "grib"
    assert handle_data_format("grib2") == "grib"
    assert handle_data_format("grb") == "grib"
    assert handle_data_format("grb2") == "grib"

    # Test list/tuple/set inputs with a single value
    assert handle_data_format(["netcdf4"]) == "netcdf"
    assert handle_data_format(("grib2",)) == "grib"
    assert handle_data_format({"nc"}) == "netcdf"

    # Test assertion error for multiple values
    with pytest.raises(InvalidRequest, match="Only one value of data_format is allowed"):
        handle_data_format(["netcdf", "grib"])
    with pytest.raises(InvalidRequest, match="Only one value of data_format is allowed"):
        handle_data_format(("nc", "grb2"))
    with pytest.raises(InvalidRequest, match="Only one value of data_format is allowed"):
        handle_data_format({"grib", "netcdf"})
    
    # Test unexpected input types
    # Srings are parsed unmodified:
    assert handle_data_format("csv") == "csv"

    # Other types raise an InvalidRequest error:
    with pytest.raises(InvalidRequest, match="data_format must be a string"):
        handle_data_format(42)


def test_handle_data_and_download_format():
    # Test default values
    assert handle_data_and_download_format({}) == {"data_format": "grib", "download_format": "as_source"}
    
    # Test overriding data format
    assert handle_data_and_download_format({"data_format": "netcdf4"}) == {"data_format": "netcdf", "download_format": "as_source"}
    assert handle_data_and_download_format({"format": "nc"}) == {"data_format": "netcdf", "download_format": "as_source"}
    
    # Test netcdf zip handling
    assert handle_data_and_download_format({"data_format": "netcdf.zip"}) == {"data_format": "netcdf", "download_format": "zip"}
    assert handle_data_and_download_format({"data_format": "netcdf_zip"}) == {"data_format": "netcdf", "download_format": "zip"}
    
    # Test overriding download format
    assert handle_data_and_download_format({"download_format": "tar"}) == {"data_format": "grib", "download_format": "tar"}
    assert handle_data_and_download_format({"data_format": "netcdf", "download_format": "gzip"}) == {"data_format": "netcdf", "download_format": "gzip"}
    
    # Test list/tuple/set inputs with a single value
    assert handle_data_and_download_format({"data_format": ["grib2"]}) == {"data_format": "grib", "download_format": "as_source"}
    assert handle_data_and_download_format({"data_format": ("nc",)}) == {"data_format": "netcdf", "download_format": "as_source"}
    
    # Test assertion error for multiple values
    with pytest.raises(InvalidRequest, match="Only one value of data_format is allowed"):
        handle_data_and_download_format({"data_format": ["netcdf", "grib"]})
    with pytest.raises(InvalidRequest, match="Only one value of data_format is allowed"):
        handle_data_and_download_format({"data_format": ("nc", "grb2")})
    
    # Test unexpected input types
    # Srings are parsed unmodified:
    assert handle_data_and_download_format({"data_format": "csv"}) == {"data_format": "csv", "download_format": "as_source"}
    assert handle_data_and_download_format({"data_format": "csv", "download_format": "xxx"}) == {"data_format": "csv", "download_format": "xxx"}
    # Other types raise an InvalidRequest error:
    with pytest.raises(InvalidRequest, match="data_format must be a string"):
        handle_data_and_download_format({"data_format": 42})

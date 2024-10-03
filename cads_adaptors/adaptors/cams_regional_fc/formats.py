class Formats:
    grib = "grib"
    netcdf = "netcdf"
    netcdf_zip = "netcdf_zip"
    netcdf_cdm = "netcdf_cdm"
    # netcdf_cdm isn't in Formats.all to avoid it being presented as a
    # suggestion when a user gives an incorrect string
    all = (grib, netcdf, netcdf_zip)

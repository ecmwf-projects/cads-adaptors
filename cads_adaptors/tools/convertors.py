import os

def grib_to_netcdf_files(grib_file):
    fname, extension = os.path.splitext(grib_file)
    import cfgrib

    datasets = cfgrib.open_datasets(grib_file)
    
    out_nc_files = []
    for i, dataset  in enumerate(datasets):
        out_fname = f"{fname}_{i}.nc"
        dataset.to_netcdf(out_fname)
        out_nc_files.append(out_fname)
    
    return out_nc_files


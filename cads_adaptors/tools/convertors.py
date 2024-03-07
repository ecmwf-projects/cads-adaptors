import os

DEFAULT_COMPRESSION_OPTIONS = {
    "compression": "gzip",
    "compression_opts": 9,
    "shuffle": True,
    "engine": "h5netcdf",
}


def grib_to_netcdf_files(
    grib_file, compression_options=None, open_datasets_kwargs=None, **to_netcdf_kwargs
):
    fname, extension = os.path.splitext(os.path.basename(grib_file))
    grib_file = os.path.realpath(grib_file)

    import cfgrib

    if open_datasets_kwargs is None:
        open_datasets_kwargs = {
            "chunks": {"time": 1}  # Auto chunking
        }
    print(open_datasets_kwargs)
    datasets = cfgrib.open_datasets(grib_file, **open_datasets_kwargs)

    if compression_options == "default":
        compression_options = DEFAULT_COMPRESSION_OPTIONS

    if compression_options is not None:
        to_netcdf_kwargs.setdefault(
            "engine", compression_options.pop("engine", "h5netcdf")
        )

    out_nc_files = []
    for i, dataset in enumerate(datasets):
        if compression_options is not None:
            to_netcdf_kwargs.update(
                {
                    "encoding": {var: compression_options for var in dataset},
                }
            )
        out_fname = f"{fname}_{i}.nc"
        dataset.to_netcdf(out_fname, **to_netcdf_kwargs)
        out_nc_files.append(out_fname)

    del dataset
    del datasets

    return out_nc_files

    # from earthkit import data as ek_data

    # dataset = ek_data.from_source("file", grib_file)

    # # Assume full data cube:
    # out_file = os.path.join(os.path.dirname(grib_file), f"{fname}.nc")

    # dataset.to_netcdf(out_file)

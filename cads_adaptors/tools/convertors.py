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
    fname, _ = os.path.splitext(os.path.basename(grib_file))
    grib_file = os.path.realpath(grib_file)

    import cfgrib
    import dask

    with dask.config.set(scheduler="threads"):
        if open_datasets_kwargs is None:
            open_datasets_kwargs = {
                "chunks": {"time": 1, "step": 1, "plev": 1}  # Auto chunk by field
            }
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

    return out_nc_files

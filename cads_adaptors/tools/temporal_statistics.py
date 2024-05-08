from earthkit.aggregate import temporal
from earthkit.data import from_source

def temporal_reduce(in_path, **kwargs, **open_datasets_kwargs):

    dataset = from_source("file", in_path).to_xarray(**open_datasets_kwargs)


    return out_paths
from typing import Any, Callable

from earthkit.aggregate import temporal
from earthkit.data import from_source


def daily_statistics(
    in_path: str,
    how: str | Callable = "mean",
    open_datasets_kwargs: dict[str, Any] = dict(),
    to_netcdf_kwargs: dict[str, Any] = dict(),
):
    dataset = from_source("file", in_path).to_xarray(**open_datasets_kwargs)

    daily_ds = temporal.daily_reduce(dataset, how=how)

    out_paths = [daily_ds.to_netcdf(**to_netcdf_kwargs)]

    return out_paths

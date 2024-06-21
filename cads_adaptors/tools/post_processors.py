from typing import Any, Callable


CONFIG_MAPPING = {
    "daily_statistics": {
        "method": "temporal_reduction",
        "frequency": "day",
    },
    "daily_mean": {
        "method": "daily_statistics",
        "how": "mean",
    },
    "daily_median": {
        "method": "daily_statistics",
        "how": "median",
    },
    "daily_min": {
        "method": "daily_statistics",
        "how": "min",
    },
    "daily_max": {
        "method": "daily_statistics",
        "how": "max",
    },
    "monthly_statistics": {
        "method": "temporal_reduction",
        "frequency": "month",
    },
    "monthly_median": {
        "method": "monthly_statistics",
        "how": "median",
    },
    "monthly_min": {
        "method": "monthly_statistics",
        "how": "min",
    },
    "monthly_max": {
        "method": "monthly_statistics",
        "how": "max",
    },
}

def pp_config_mapping(pp_config: dict[str, Any]) -> dict[str, Any]:
    cnt = 0 # Escape infinite loop
    while pp_config.get("method") in CONFIG_MAPPING and cnt<100:
        pp_config = {**pp_config, **CONFIG_MAPPING[pp_config["method"]]}
    return pp_config

def temporal_reduction(
    in_path: str,
    how: str | Callable = "mean",
    frequency: str = "day",
    open_datasets_kwargs: dict[str, Any] = dict(),
    to_netcdf_kwargs: dict[str, Any] = dict(),
):
    from earthkit.aggregate import temporal
    from earthkit.data import from_source

    dataset = from_source("file", in_path).to_xarray(**open_datasets_kwargs)

    reduced_ds = temporal.reduce(dataset, how=how, frequency=frequency)

    out_paths = [reduced_ds.to_netcdf(**to_netcdf_kwargs)]

    return out_paths

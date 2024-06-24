from typing import Any, Callable
from xarray import Dataset

from cads_adaptors import Context

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
    in_xarray_dict: dict[str, Dataset],
    context: Context = Context(),
    how: str | Callable = "mean",
    frequency: str = "day",
    **kwargs,
) -> dict[str, Dataset]:
    from earthkit.aggregate import temporal

    out_xarray_dict = {}
    for in_tag, in_dataset in in_xarray_dict.items():
        out_tag = f"{in_tag}_{how}_{frequency}"
        context.add_stdout(f"Temporal reduction: {out_tag}")
        out_xarray_dict[out_tag] = temporal(
            in_dataset,
            how=how,
            frequency=frequency,
            **kwargs,
        )

    return out_xarray_dict

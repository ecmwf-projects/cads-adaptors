from typing import Any, Callable

from xarray import Dataset

from cads_adaptors import Context

CONFIG_MAPPING = {
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
    cnt = 0  # Escape infinite loop
    while pp_config.get("method") in CONFIG_MAPPING and cnt < 100:
        pp_config = {**pp_config, **CONFIG_MAPPING[pp_config["method"]]}
    return pp_config


def daily_statistics(
    in_xarray_dict: dict[str, Dataset],
    context: Context = Context(),
    how: str | Callable = "mean",
    **kwargs,
) -> dict[str, Dataset]:
    from earthkit.aggregate import temporal

    out_xarray_dict = {}
    for in_tag, in_dataset in in_xarray_dict.items():
        out_tag = f"{in_tag}_daily-{how}"
        context.add_stdout(f"Daily reduction: {how} {kwargs}")
        context.add_user_visible_log(f"Temporal reduction: {how} {kwargs}")
        out_xarray_dict[out_tag] = temporal.daily_reduce(
            in_dataset,
            how=how,
            **kwargs,
        )

    return out_xarray_dict


def monthly_statistics(
    in_xarray_dict: dict[str, Dataset],
    context: Context = Context(),
    how: str | Callable = "mean",
    **kwargs,
) -> dict[str, Dataset]:
    from earthkit.aggregate import temporal

    out_xarray_dict = {}
    for in_tag, in_dataset in in_xarray_dict.items():
        out_tag = f"{in_tag}_monthly-{how}"
        context.add_stdout(f"Temporal reduction: {how} {kwargs}")
        context.add_user_visible_log(f"Temporal reduction: {how} {kwargs}")
        out_xarray_dict[out_tag] = temporal.monthly_reduce(
            in_dataset,
            how=how,
            **kwargs,
        )

    return out_xarray_dict

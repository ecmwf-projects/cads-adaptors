from typing import Any, Callable

from xarray import Dataset

from cads_adaptors import Context

CONFIG_MAPPING = {
    "daily_mean": {
        "method": "daily_reduce",
        "how": "mean",
    },
    "daily_median": {
        "method": "daily_reduce",
        "how": "median",
    },
    "daily_min": {
        "method": "daily_reduce",
        "how": "min",
    },
    "daily_max": {
        "method": "daily_reduce",
        "how": "max",
    },
    "daily_std": {
        "method": "daily_reduce",
        "how": "std",
    },
    "monthly_median": {
        "method": "monthly_reduce",
        "how": "median",
    },
    "monthly_min": {
        "method": "monthly_reduce",
        "how": "min",
    },
    "monthly_max": {
        "method": "monthly_reduce",
        "how": "max",
    },
}


def pp_config_mapping(
    pp_config: dict[str, Any], context: Context = Context()
) -> dict[str, Any]:
    cnt = 0  # Escape infinite loop
    while pp_config.get("method") in CONFIG_MAPPING and cnt < 100:
        pp_config = {**pp_config, **CONFIG_MAPPING[pp_config["method"]]}
    if "method" not in pp_config:
        context.add_user_visible_error(
            f"Ignoring invalid post-processor config: {pp_config}"
        )
        return {}
    return pp_config


def daily_reduce(
    in_xarray_dict: dict[str, Dataset],
    context: Context = Context(),
    how: str | Callable = "mean",
    **kwargs,
) -> dict[str, Dataset]:
    from earthkit.transforms.aggregate import temporal

    out_xarray_dict = {}
    for in_tag, in_dataset in in_xarray_dict.items():
        out_tag = f"{in_tag}_daily-{how}"
        context.add_stdout(f"Daily reduction: {how} {kwargs}")
        context.add_user_visible_log(f"Temporal reduction: {how} {kwargs}")
        reduced_data = temporal.daily_reduce(
            in_dataset,
            how=how,
            **kwargs,
        )
        out_xarray_dict[out_tag]

    return out_xarray_dict


def monthly_reduce(
    in_xarray_dict: dict[str, Dataset],
    context: Context = Context(),
    how: str | Callable = "mean",
    **kwargs,
) -> dict[str, Dataset]:
    from earthkit.transforms.aggregate import temporal

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


def update_history(dataset: Dataset, update_text: str) -> Dataset:
    
    history = dataset.attrs.get("history", None)
    if history is None:
        history = update_text
    else:
        history += f"\n{update_text}"
    return dataset.assign_attrs({"history": history})



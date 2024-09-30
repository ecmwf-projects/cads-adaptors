import itertools
import math
import warnings
from typing import Any, Generator

EXCLUDED_WIDGETS = [
    "GeographicExtentWidget",
    "UnknownWidget",
]

# TODO: Handle DateRangeWidget
# , "DateRangeWidget"]


def ensure_set(input_item):
    if not isinstance(input_item, set):
        if isinstance(input_item, (list, tuple)):
            return set(input_item)
        else:
            return {input_item}
    return input_item


def combination_tuples_iterater(
    found: list[dict[str, set[str]]],
) -> Generator[tuple[tuple[str, str]]]:
    if not found:
        yield tuple()
    for d in found:
        keys, values = zip(*d.items())
        for v in itertools.product(*values):
            yield tuple(zip(keys, v))


def count_weighted_size(
    found: list[dict[str, set[str]]],
    weighted_keys: dict[str, int] = dict(),
    weighted_values: dict[str, dict[str, int]] = dict(),
) -> int:  # TODO: integer is not strictly required

    n_granules = 0
    for granule in combination_tuples_iterater(found):
        w_granule = 1
        for key, w_values in weighted_values.items():
            w_granule *= w_values.get(granule.get(key, {}), 1)
        for key, weight in weighted_keys.items():
            w_granule *= weight if key in granule else 1
        n_granules += w_granule
    return n_granules

    # if len(weighted_values) > 0:
    #     w_granules = []  # Weight of each granule
    #     for granule in combination_tuples_iterater(found):
    #         w_granule = 1
    #         for key, w_values in weighted_values.items():
    #             if key in granule:
    #                 for value, weight in w_values.items():
    #                     if value == granule[key]:
    #                         w_granule *= weight
    #         w_granules.append(w_granule)
    # else:
    #     w_granules = [1 for _ in granules]

    # for key, weight in weighted_keys.items():
    #     for i, (w_granule, granule) in enumerate(zip(w_granules, granules)):
    #         if key in granule:
    #             w_granules[i] = w_granule * weight

    # n_granules = sum(w_granules)
    # return n_granules


def estimate_precise_size(
    form: list[dict[str, Any]] | dict[str, Any] | None,
    mapped_intersected_selection: set[dict[str, set[str]]],
    ignore_keys: list[str] = [],
    weight: int = 1,
    weighted_keys: dict[str, int] = {},
    weighted_values: dict[str, dict[str, int]] = {},
    # extra: dict = {},
    **kwargs,
) -> int:
    ignore_keys += get_excluded_keys(form)

    # form_keys = [widget["name"] for widget in form if "name" in widget]
    # form_key_values = constraints.parse_form(form)

    # # Ensure any missing fields are filled with a DUMMY value
    # mapped_intersected_selection = [{
    #     widget: ensure_set(selection.get(widget, list(values)[0]))
    #     for widget, values in form_key_values.items()
    #     if widget not in ignore_keys
    # } for selection in mapped_intersected_selection]
    mapped_intersected_selection = [
        {
            widget: ensure_set(values)
            for widget, values in selection.items()
            if widget not in ignore_keys
        }
        for selection in mapped_intersected_selection
    ]

    if len(weighted_keys) == 0 and len(weighted_values) == 0:
        quick_size = 0
        for selection in mapped_intersected_selection:
            quick_size += estimate_number_of_fields(form, selection)
        return quick_size * weight
    
    return (
        count_weighted_size(
            mapped_intersected_selection,
            weighted_keys=weighted_keys,
            weighted_values=weighted_values,
        )
        * weight
    )


def get_excluded_keys(
    form: list[dict[str, Any]] | dict[str, Any] | None,
) -> list[str]:
    if form is None:
        form = []
    if not isinstance(form, list):
        form = [form]
    excluded_keys = []
    for widget in form:
        if widget.get("type", "UnknownWidget") in EXCLUDED_WIDGETS:
            excluded_keys.append(widget["name"])
    return excluded_keys


def estimate_number_of_fields(
    form: list[dict[str, Any]] | dict[str, Any] | None,
    request: dict[str, Any],
) -> int:
    excluded_variables = get_excluded_keys(form)
    number_of_values = []
    for variable_id, variable_value in request.items():
        if isinstance(variable_value, set):
            variable_value = list(variable_value)
        if not isinstance(variable_value, (list, tuple)):
            variable_value = [
                variable_value,
            ]
        if variable_id not in excluded_variables:
            number_of_values.append(len(variable_value))
    number_of_fields = math.prod(number_of_values)
    return number_of_fields

import itertools
import math
from typing import Any

from . import constraints

EXCLUDED_WIDGETS = ["GeographicExtentWidget", "UnknownWidget"]

# TODO: Handle DateRangeWidget
# , "DateRangeWidget"]


def compute_combinations(d: dict[str, set[str]]) -> list[dict[str, str]]:
    if not d:
        return []
    keys, values = zip(*d.items())
    return [dict(zip(keys, v)) for v in itertools.product(*values)]


def remove_duplicates(found: list[dict[str, set[str]]]) -> list[dict[str, str]]:
    combinations: list[dict[str, str]] = []
    for d in found:
        combinations += compute_combinations(d)
    granules = {tuple(combination.items()) for combination in combinations}
    return [dict(granule) for granule in granules]


def count_combinations(
    found: list[dict[str, set[str]]],
    selected_but_always_valid: list[str] = [],
    weighted_keys: dict[str, int] = dict(),
    weighted_values: dict[str, dict[str, int]] = dict(),
) -> int:
    # granules: list[dict[str, str]] = []
    # for d in found:
    #     granules = granules + compute_combinations(d)
    granules = remove_duplicates(found)
    print(granules)
    if len(weighted_keys) > 0:
        n_granules = 0
        for granule in granules:
            w_granule = 1
            # Weight combination by key
            for key, weight in weighted_keys.items():
                if key in granule or key in selected_but_always_valid:
                    w_granule *= weight
            # Weight combination by value
            for key, w_values in weighted_values.items():
                if key in granule or key in selected_but_always_valid:
                    for value, weight in w_values.items():
                        if value == granule[key]:
                            w_granule *= weight
            n_granules += w_granule
    else:
        n_granules = len(granules)

    return n_granules


def estimate_granules(
    form_key_values: dict[str, set[Any]],
    selection: dict[str, set[str]],
    _constraints: list[dict[str, set[str]]],
    weighted_keys: dict[str, int] = dict(),  # Mapping of widget key to weight
    weighted_values: dict[
        str, dict[str, int]
    ] = dict(),  # Mapping of widget key to values-weights
    safe: bool = True,
) -> int:
    constraint_keys = constraints.get_keys(_constraints)
    always_valid = constraints.get_always_valid_params(form_key_values, constraint_keys)
    selected_but_always_valid = {
        k: v for k, v in selection.items() if k in always_valid
    }
    always_valid_multiplier = math.prod(map(len, selected_but_always_valid.values()))
    selected_constrained = {
        k: v for k, v in selection.items() if k not in always_valid.keys()
    }
    found = []
    # Apply constraints prior to ensure real cost is calculated
    for constraint in _constraints:
        intersection = {}
        ok = True
        for key, values in constraint.items():
            if key in selected_constrained.keys():
                common = values.intersection(selected_constrained[key])
                if common:
                    intersection.update({key: common})
                else:
                    ok = False
                    break
            else:
                ok = False
                break
        if ok:
            if intersection not in found:
                found.append(intersection)
    if safe:
        n_granules = count_combinations(
            found, list(selected_but_always_valid), weighted_keys, weighted_values
        )
        return (n_granules) * max(1, always_valid_multiplier)
    else:
        return sum([math.prod([len(e) for e in d.values()]) for d in found]) * max(
            1, always_valid_multiplier
        )


def estimate_size(
    form: list[dict[str, Any]] | dict[str, Any] | None,
    selection: dict[str, set[str]],
    _constraints: list[dict[str, set[str]]],
    ignore_keys: list[str] = [],
    weight: int = 1,
    weighted_keys: dict = {},
    weighted_values: dict = {},
    safe: bool = True,
    **kwargs,
) -> int:
    ignore_keys += get_excluded_keys(form)

    this_selection: dict[str, set[str]] = {
        k: v for k, v in selection.items() if k not in ignore_keys
    }

    form_key_values = constraints.parse_form(form)

    return (
        estimate_granules(
            form_key_values,
            this_selection,
            _constraints,
            weighted_keys=weighted_keys,
            weighted_values=weighted_values,
            safe=safe,
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

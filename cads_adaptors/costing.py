import itertools
import math
import warnings
from typing import Any

from cads_adaptors.tools.general import ensure_list

from . import constraints

EXCLUDED_WIDGETS = [
    "GeographicExtentWidget",
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


def compute_combinations(d: dict[str, set[str]]) -> list[dict[str, str]]:
    warnings.warn("compute_combinations is deprecated", DeprecationWarning)
    if not d:
        return []
    keys, values = zip(*d.items())
    return [dict(zip(keys, v)) for v in itertools.product(*values)]


def compute_combination_tuples(
    d: dict[str, set[str]],
) -> list[tuple[tuple[Any, Any], ...]]:
    if not d:
        return []
    keys, values = zip(*d.items())
    return [tuple(zip(keys, v)) for v in itertools.product(*values)]


def remove_duplicates(found: list[dict[str, set[str]]]) -> list[dict[str, str]]:
    combinations: list[tuple[tuple[Any, Any], ...]] = []
    for d in found:
        combinations += compute_combination_tuples(d)
    return [dict(granule) for granule in set(combinations)]


def n_unique_granules(found: list[dict[str, set[str]]]) -> int:
    combinations: list[tuple[tuple[Any, Any], ...]] = []
    for d in found:
        combinations += compute_combination_tuples(d)
    return len(set(combinations))


def count_combinations(
    found: list[dict[str, set[str]]],
    weighted_keys: dict[str, int] = dict(),
    weighted_values: dict[str, dict[str, int]] = dict(),
) -> int:  # TODO: integer is not strictly required
    if len(weighted_keys) == 0 and len(weighted_values) == 0:
        return n_unique_granules(found)

    granules = remove_duplicates(found)

    if len(weighted_values) > 0:
        w_granules = []  # Weight of each granule
        for granule in granules:
            w_granule = 1
            for key, w_values in weighted_values.items():
                if key in granule:
                    for value, weight in w_values.items():
                        if value == granule[key]:
                            w_granule *= weight
            w_granules.append(w_granule)
    else:
        w_granules = [1 for _ in granules]

    for key, weight in weighted_keys.items():
        w_granules = [
            w_granule * weight
            for w_granule, granule in zip(w_granules, granules)
            if key in granule
        ]
    n_granules = sum(w_granules)
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
    if len(_constraints) > 0:
        # Ensure contraints are sets
        _constraints = [
            {k: set(v) for k, v in constraint.items()} for constraint in _constraints
        ]
        constraint_keys = constraints.get_keys(_constraints)
        always_valid = constraints.get_always_valid_params(
            form_key_values, constraint_keys
        )
        selected_but_always_valid = {
            k: v for k, v in selection.items() if k in always_valid
        }
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
                intersection.update(selected_but_always_valid)
                if intersection not in found:
                    found.append(intersection)
    else:
        selected_but_always_valid = {}
        found = [selection]
    if safe:
        return count_combinations(found, weighted_keys, weighted_values)
    else:
        return sum([math.prod([len(e) for e in d.values()]) for d in found])


def estimate_precise_size(
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

    form_key_values = constraints.parse_form(form)

    # Build selection for calculating costs, any missing fields are filled with a DUMMY value,
    #  This may be problematic for DateRangeWidget
    this_selection: dict[str, set[str]] = {
        widget: ensure_set(selection.get(widget, list(values)[0]))
        for widget, values in form_key_values.items()
        if widget not in ignore_keys
    }
    return (
        estimate_granules(
            form_key_values,
            this_selection,
            _constraints,
            weighted_keys=weighted_keys,
            weighted_values=weighted_values,
            safe=safe,
            **kwargs,
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
    ignore_keys: list[str] = [],
    **kwargs,
) -> int:
    weighted_values = kwargs.get("weighted_values", {})
    weighted_keys = kwargs.get("weighted_keys", {})
    excluded_variables = get_excluded_keys(form) + ensure_list(ignore_keys)
    number_of_values = []
    for variable_id, variable_value in request.items():
        weights_v = weighted_values.get(variable_id, {})
        weight_k = weighted_keys.get(variable_id, 1)
        if isinstance(variable_value, set):
            variable_value = list(variable_value)
        if not isinstance(variable_value, (list, tuple)):
            variable_value = [
                variable_value,
            ]
        if variable_id not in excluded_variables:
            n_values = len(variable_value)
            # If any values in weighted_values, add weight - 1 to n_values (the first is already counted)
            for val, weight in weights_v.items():
                if val in variable_value:
                    n_values += weight - 1
            # Append number of values, multiplied by weight for the key
            number_of_values.append(n_values * weight_k)
    number_of_fields = math.prod(number_of_values)
    return number_of_fields

import itertools
import math
from typing import Any

from . import constraints

EXCLUDED_WIDGETS = ["GeographicExtentWidget", "DateRangeWidget"]


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


def estimate_granules(
    form: dict[str, set[str]],
    selection: dict[str, set[str]],
    _constraints: list[dict[str, set[str]]],
    safe: bool = True,
) -> int:
    constraint_keys = constraints.get_keys(_constraints)
    always_valid = constraints.get_always_valid_params(form, constraint_keys)
    selected_but_always_valid = {
        k: v for k, v in selection.items() if k in always_valid.keys()
    }
    always_valid_multiplier = math.prod(map(len, selected_but_always_valid.values()))
    selected_constrained = {
        k: v for k, v in selection.items() if k not in always_valid.keys()
    }
    found = []
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
        unique_granules = remove_duplicates(found)
        return (len(unique_granules)) * max(1, always_valid_multiplier)
    else:
        return sum([math.prod([len(e) for e in d.values()]) for d in found]) * max(
            1, always_valid_multiplier
        )


def estimate_size(
    form: dict[str, set[str]],
    selection: dict[str, set[str]],
    _constraints: list[dict[str, set[str]]],
    safe: bool = True,
    granule_size: int = 1,
) -> int:
    return estimate_granules(form, selection, _constraints, safe=safe) * granule_size


def get_excluded_fields(ogc_form: dict[str, set[str]]) -> list[str]:
    if ogc_form is None:
        ogc_form = []
    if not isinstance(ogc_form, list):
        ogc_form = [ogc_form]
    excluded_fields = []
    for schema in ogc_form:
        if schema["type"] in EXCLUDED_WIDGETS:
            excluded_fields.append(schema["name"])
    return excluded_fields


def estimate_number_of_values(
    form: dict[str, set[str]], request: dict[str, dict[str, Any]]
) -> int:
    excluded_fields = get_excluded_fields(form)
    selection = request["inputs"]
    number_of_values = []
    for field_id, field_value in selection.items():
        if not isinstance(field_value, list):
            field_value = [
                field_value,
            ]
        if field_id not in excluded_fields:
            number_of_values.append(len(field_value))
    number_of_fields = math.prod(number_of_values)
    return number_of_fields

import itertools
import math
from typing import Any, Generator

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


def combination_tuples_iterater(
    found: list[dict[str, set[str]]],
) -> Generator[tuple[tuple[Any, Any], ...], None, None]:
    if not found:
        yield tuple()
    seen_granules = set()
    for d in found:
        keys, values = zip(*d.items())
        for v in itertools.product(*values):

            _hash = hash(v)
            if _hash in seen_granules:
                continue
            seen_granules.add(_hash)

            yield tuple(zip(keys, v))


def combination_tuples(
    found: list[dict[str, set[str]]],
) -> tuple[tuple[Any, Any]]:
    if not found:
        return tuple()
    seen_key_vals = dict()
    granules = set()
    # Order by size, largest first, a smaller set may be a subset of a larger set
    found.sort(key=lambda x: -len(x))
    print("found", found)
    for d in found:
        keys, values = zip(*d.items())
        these_seen = []
        for s_k, s_v in seen_key_vals.items():
            if set(keys) <= set(s_k):
                these_seen.append({s_k: s_v})
        print("these_seen", these_seen)
        for vs in itertools.product(*values):
            if any([
                all([v in i_seen[k] for k, v in zip(keys, vs)])
                for i_seen in these_seen
            ]):
                continue
            if keys in seen_key_vals:
                seen_key_vals[keys].append(vs)
            else:
                seen_key_vals[keys] = [vs]
            granules.add(tuple(zip(keys, vs)))
    print("granules", granules)
    return granules


def count_weighted_size(
    found: list[dict[str, set[str]]],
    weighted_keys: dict[str, int] = dict(),
    weighted_values: dict[str, dict[str, int]] = dict(),
) -> int:
    n_granules: int = 0
    for _granule in combination_tuples(found):
        granule: dict[str, str] = dict(_granule)
        w_granule = 1
        for key, w_values in weighted_values.items():
            w_granule *= int(w_values.get(granule.get(key, "__NULL__"), 1))
        for key, weight in weighted_keys.items():
            w_granule *= weight if key in granule else 1
        n_granules += w_granule
    return n_granules


def estimate_precise_size(
    form: list[dict[str, Any]] | dict[str, Any] | None,
    mapped_intersected_selection: list[dict[str, set[str]]],
    ignore_keys: list[str] = [],
    weight: int = 1,
    weighted_keys: dict[str, int] = {},
    weighted_values: dict[str, dict[str, int]] = {},
    **kwargs,
) -> int:
    ignore_keys += get_excluded_keys(form)

    mapped_intersected_selection = [
        {
            widget: ensure_set(values)
            for widget, values in selection.items()
            if widget not in ignore_keys
        }
        for selection in mapped_intersected_selection
    ]

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

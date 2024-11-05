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
    all_keys = set()
    for d in found:
        all_keys.update(d.keys())
    all_keys = list(all_keys)

    seen_granules = set()
    seen_granules_hashes = set()
    for d in found:
        keys, values = zip(*d.items())
        for v in itertools.product(*values):
            # Create a tuple for hashing, we populate with None for all keys not in the current granule
            v_for_seen = (
                v[keys.index(k)] if k in keys else None for k in all_keys
            )
            _hash = hash(v_for_seen)
            # Check if the hash is a duplicate of a previously seen granule
            if _hash in seen_granules:
                continue
            # Check if the granule is a subset of a previously seen granule
            for _v_seen in seen_granules:
                v_seen = [v for v in _v_seen if v is not None]
                v_len_diff = len(v) - len(v_seen)
                if v_len_diff == 0:
                    # Identical length, so cannot be a subset
                    continue
                elif v_len_diff < 0:
                    # potential subset of existing granule
                    if all(
                        v2 is None or v2 == v for v, v2 in zip(v_seen, v)
                    ):
                        continue
                else:
                    # potential superset of existing granule
                    # if it is, we must remove the existing granule from seen sets
                    if all(
                        v is None or v2 == v for v, v2 in zip(v, v_seen)
                    ):
                        seen_granules.remove(_v_seen)
                        seen_granules_hashes.remove(_hash)

            seen_granules_hashes.add(_hash)
            seen_granules.add(tuple(v_for_seen))

            yield tuple(zip(keys, v))


def combination_tuples(
    found: list[dict[str, set[str]]],
) -> tuple[tuple[Any, Any]]:
    if not found:
        return tuple()
    
    all_keys = set()
    for d in found:
        all_keys.update(d.keys())
    all_keys = list(all_keys)

    seen_granules = set()
    seen_granules_hashes = set()
    for d in found:
        keys, values = zip(*d.items())
        for v in itertools.product(*values):
            # Create a tuple for hashing, we populate with None for all keys not in the current granule
            v_for_seen = (
                v[keys.index(k)] if k in keys else None for k in all_keys
            )
            _hash = hash(v_for_seen)
            # Check if the hash is a duplicate of a previously seen granule
            if _hash in seen_granules:
                continue
            # Check if the granule is a subset of a previously seen granule
            for _v_seen in seen_granules.copy():
                v_seen = [v for v in _v_seen if v is not None]
                v_len_diff = len(v) - len(v_seen)
                if v_len_diff == 0:
                    # Identical length, so cannot be a subset
                    continue
                elif v_len_diff < 0:
                    # potential subset of existing granule
                    if all(
                        v2 is None or v2 == v for v, v2 in zip(_v_seen, v_for_seen)
                    ):
                        continue
                else:
                    # potential superset of existing granule
                    # if it is, we must remove the existing granule from seen sets
                    if all(
                        v is None or v2 == v for v, v2 in zip(_v_seen, v_for_seen)
                    ):
                        seen_granules.remove(_v_seen)
                        seen_granules_hashes.remove(_hash)

            seen_granules_hashes.add(_hash)
            seen_granules.add(tuple(v_for_seen))
    
    return (
        ((all_keys[i], v) for i, v in enumerate(values) if v is not None) for values in seen_granules
    )


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

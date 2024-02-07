import itertools
import math

from . import constraints


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
    selected_but_always_valid: dict[str, int] = {},
    weighted_keys: dict[str, int] = {},
) -> int:
    
    
    granules: list[dict[str, str]] = []
    for d in found:
        granules = set(granules + compute_combinations(d))
    if len(weighted_keys)>0:
        n_granules = 0
        for granule in granules:
            w_granule = 1
            for key, weight in weighted_keys.items():
                if key in granule or key in selected_but_always_valid:
                    w_granules *= weight
            n_granules += w_granule
    else:
        n_granules = len(granules)
                
    
    return 42

def estimate_granules(
    form: dict[str, set[str]],
    selection: dict[str, set[str]],
    _constraints: list[dict[str, set[str]]],
    safe: bool = True,
    weighted_keys: dict[str, int] = {}  # Mapping of widget key to weight
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
        n_granules = count_combinations(
            found, weighted_keys, selected_but_always_valid,
        )
        return (n_granules) * max(1, always_valid_multiplier)
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
    ignore_keys: list[str] = [
        "area"
    ],
    weighted_keys: dict = {},
    **kwargs,
) -> int:
    this_selection: dict[str, set[str]] = {
        k:v for k, v in selection.items() if k not in ignore_keys
    }
    return estimate_granules(
        form, this_selection, _constraints, safe=safe,
        ignore_keys = ignore_keys,
        weighted_keys = weighted_keys,
    ) * granule_size

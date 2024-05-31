from . import hcube_tools


def apply_constraints(requests, constraints, rejects=False, logger=None):
    """Apply the constraints to the requests and output the accepted and,
    optionally, rejected request fragments.
    """
    if not isinstance(requests, (list, tuple)):
        requests = [requests]

    if logger is not None:
        logger.info("requests: " + repr(requests))

    # Get the type of each constraint key
    types = {}
    for c in constraints:
        for k, v in c.items():
            if k not in types:
                types[k] = set()
            types[k].update([type(x) for x in v])
    for k in list(types.keys()):
        if len(types[k]) > 1:
            raise Exception('Multiple types for "' + k + '" in constraints')
        types[k] = types[k].pop()

    # Set of all constrained keys
    allk = set(sum([list(c.keys()) for c in constraints], []))

    # Apply each constraint
    accepted = []
    for con in constraints:
        # We will remove request keys that are constrained but are not in this
        # constraint. This happens when there are distinct hypercubes that do
        # not share the same dimensionality in the dataset, e.g. when both
        # pressure_level and model_level exist.
        conkeys = set(con.keys())
        remk = allk - conkeys

        # Get a copy of the requests with values converted to the correct type
        # and unwanted keys removed. We will not include requests that are
        # missing keys that appear in this constraint.
        reqs = [
            {k: _cast_list(k, v, types.get(k)) for k, v in req.items() if k not in remk}
            for req in requests
            if conkeys <= set(req.keys())
        ]

        # Get the intersection and difference between the requests and this
        # constraint. The intersection are those that pass the constraint.
        intn, d12, d21 = hcube_tools.hcubes_intdiff2(reqs, [con])
        accepted.extend(intn)

    if rejects:
        rejected = hcube_tools.hcubes_subtract(requests, accepted)
        return (accepted, rejected)
    else:
        return accepted


def _cast(key, value, constraint_type):
    """Convert value to constraint_type if possible."""
    if constraint_type is None or type(value) is constraint_type:
        return value
    if constraint_type is str:
        return str(value)
    raise Exception(
        f'constraint "{key}" is type '
        + str(constraint_type)
        + " but request value is type "
        + str(value)
    )


def _cast_list(key, values, constraint_type):
    """Convert all values to constraint_type if possible."""
    return [
        _cast(key, x, constraint_type)
        for x in (values if isinstance(values, list) else [values])
    ]

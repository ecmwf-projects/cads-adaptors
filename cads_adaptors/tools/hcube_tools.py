# copied from cdscommon

"""Functions for processing hypercubes represented as dicts."""

# dicts became ordered by default from Python 3.6
import sys
from copy import deepcopy
from itertools import chain, product

if sys.version_info.major >= 3 and sys.version_info.minor >= 6:
    odict = dict
else:
    from collections import OrderedDict as odict

from .date_tools import compress_dates_list, expand_dates_list


def same_fields(reqs1, reqs2, date_field="date"):
    """Return True of reqs1 and reqs2 represent the same fields and
    False if not
    .
    """
    intdiff = hcubes_intdiff(reqs1, reqs2, date_field=date_field)
    return intdiff[1] == [] and intdiff[2] == []


def hcubes_intdiff(reqs1, reqs2, date_field="date"):
    """Calculate the intersection and differences of the two sets of requests.
    The output is a three-element list containing 1). a list of requests
    representing the intersection, 2). a list of requests representing the
    reqs1 minus reqs2 difference and 3). a list of requests representing the
    reqs2 minus reqs1 difference.
    The requests do not all have to have the same keys, but requests will
    only be considered as having a possible intersection if they do.
    """
    intn = []
    d12 = []
    d21 = []

    # Only compare requests with the same sets of keys. The sorting is done for
    # reproducibility.
    keysets = sorted(
        list(set([frozenset(r.keys()) for r in reqs1 + reqs2])),
        key=lambda ks: repr(sorted(list(ks))),
    )
    for keyset in keysets:
        r1 = [r for r in reqs1 if frozenset(r.keys()) == keyset]
        r2 = [r for r in reqs2 if frozenset(r.keys()) == keyset]
        intdiff = hcubes_intdiff2(r1, r2, date_field=date_field)
        intn.extend(intdiff[0])
        d12.extend(intdiff[1])
        d21.extend(intdiff[2])

    return [intn, d12, d21]


def hcubes_intdiff2(reqs1, reqs2, date_field="date"):
    """Same as hcubes_intdiff() but requests are compared even if
    they do not have the same keys. Elements of reqs1 may have keys that
    elements of reqs2 do not, but not the other way around or an exception
    will be raised. Keys that are not shared are completely ignored.
    """
    reqs1 = _ensure_list(reqs1)
    reqs2 = _ensure_list(reqs2)
    assert_lists(reqs1, "reqs1")
    assert_lists(reqs2, "reqs2")

    # Get all intersections between requests
    intns = []
    intns2 = []
    for req1 in reqs1:
        for req2 in reqs2:
            intn1, _, _ = hcube_intdiff(req1, req2, date_field=date_field)
            if intn1 is not None:
                # req1 is allowed to have keys that req2 doesn't and intn1
                # will have those extra keys. Get rid of them.
                intn2 = {k: intn1[k] for k in req2.keys()}

                intns.append(intn1)
                intns2.append(intn2)

    # Remove duplicate fields from lists of intersections
    remove_duplicates(intns)
    remove_duplicates(intns2)

    # Subtract the intersections from the originals to get the remainders
    rem1 = hcubes_subtract(reqs1, intns)
    rem2 = hcubes_subtract(reqs2, intns2)

    # Perform merging on the result if possible
    hcubes_merge(intns)
    hcubes_merge(rem1)
    hcubes_merge(rem2)

    # Attempt to put lists back in original orders, purely for tidiness
    for reqs, orig in zip([intns, rem1, rem2], [reqs1, reqs1, reqs2]):
        orig_key_order = sum([list(x.keys()) for x in orig], [])
        for rr in reqs:
            dict_sort_keys(rr, orig_key_order.index)
            for k, v in rr.items():
                original = sum([x[k] for x in orig if k in x], [])
                try:
                    rr[k] = sorted(v, key=original.index)
                except ValueError:
                    # This can happen when compressed date ranges have been
                    # expanded - new date strings or date ranges may be in the
                    # result which are not in the original
                    pass

    return [intns, rem1, rem2]


def hcube_intdiff(req1, req2, date_field="date"):
    """Calculate the intersection and differences of two requests.
    The output is a three-element list containing 1). the intersection of
    the two requests or None if there is no intersection 2). a list of
    requests representing the req1 minus req2 difference and 3). a list of
    requests representing the req2 minus req1 difference.
    req1 may have keys that req2 does not but not the other way around or an
    exception will be raised. Keys that are not shared are ignored.
    """
    assert set(req2.keys()) <= set(req1.keys()), (
        "req2 has keys that req1 does not: "
        + repr(list(req1.keys()))
        + " vs "
        + repr(list(req2.keys()))
    )
    assert_lists(req1, "req1")
    assert_lists(req2, "req2")

    # Expand dates if compressed
    req1b = _expand_dates(req1, date_field)
    req2b = _expand_dates(req2, date_field)
    expanded_dates1 = date_field in req1 and req1b[date_field] != req1[date_field]
    expanded_dates2 = date_field in req2 and req2b[date_field] != req2[date_field]

    # Find intersection and differences
    intdiff = _hcube_intdiff(req1b, req2b)

    # Recompress dates if appropriate
    if (expanded_dates1 or expanded_dates2) and intdiff[0] is not None:
        intdiff[0][date_field] = compress_dates_list(intdiff[0][date_field])
    if expanded_dates1:
        for x in intdiff[1]:
            x[date_field] = compress_dates_list(x[date_field])
    if expanded_dates2:
        for x in intdiff[2]:
            x[date_field] = compress_dates_list(x[date_field])

    return intdiff


def _expand_dates(req, date_field):
    req_out = req.copy()
    if req.get(date_field) and isinstance(req[date_field][0], str):
        req_out[date_field] = expand_dates_list(req[date_field])
    return req_out


def _hcube_intdiff(req1, req2):
    """Calculate the intersection and differences of two requests.
    The output is a three-element list containing 1). the intersection of
    the two requests or None if there is no intersection 2). a list of
    requests representing the req1 minus req2 difference and 3). a list of
    requests representing the req2 minus req1 difference.
    req1 may have keys that req2 does not but not the other way around or an
    exception will be raised. Keys that are not shared are ignored.
    """
    assert set(req2.keys()) <= set(req1.keys()), (
        "req2 has keys that req1 does not: "
        + repr(list(req1.keys()))
        + " vs "
        + repr(list(req2.keys()))
    )
    assert_lists(req1, "req1")
    assert_lists(req2, "req2")

    # We ignore keys that are in req1 but not req2 so if req2 is empty the
    # intersection is total.
    if len(req2) == 0:
        return [deepcopy(req1), [], []]

    # Get the values for key[0]
    key = next(iter(req2.keys()))
    v1 = req1[key]
    v2 = req2[key]
    s1 = set(v1)
    s2 = set(v2)

    # Type-comparability sanity check
    t1 = set([type(v) for v in s1])
    t2 = set([type(v) for v in s2])
    if t1.difference(t2) and t2.difference(t1):
        # We're trying to subtract values of different types. Probably a
        # mistake. Maybe we should be even stricter and not allow multiple types
        # in a key?
        raise Exception("Values have different types for key: " + key)

    # Get the lists of values that are common and different between requests in
    # key[0]
    common = sorted(list(s1.intersection(s2)), key=v1.index)
    dif12 = sorted(list(s1 - s2), key=v1.index)
    dif21 = sorted(list(s2 - s1), key=v2.index)

    # Get the intersection and difference for the requests without key[0]
    r1 = req1.copy()
    r2 = req2.copy()
    del r1[key]
    del r2[key]
    if common:
        intdiff = _hcube_intdiff(r1, r2)
    else:
        # Short circuit recursive calls for the rest of the keys
        intdiff = [None, [], []]

    # Add the common values to the intersection and difference
    for x in [intdiff[0]] + intdiff[1] + intdiff[2]:
        if x is not None:
            x[key] = common.copy()

    # The difference values represent additional hypercubes of difference
    if dif12:
        r1[key] = dif12
        intdiff[1].append(deepcopy(r1))
    if dif21:
        r2[key] = dif21
        intdiff[2].append(deepcopy(r2))

    # check_no_shared_lists(([intdiff[0]] if intdiff[0] else [])
    #                      + intdiff[1] + intdiff[2])

    return intdiff


def remove_duplicates(reqs, date_field="date"):
    """Remove all duplicate fields from reqs."""
    # Loop over pairs of elements
    ii = 0
    while ii < len(reqs) - 1:
        jj = ii + 1
        ii_incr = 1
        while jj < len(reqs):
            # Get the intersection and difference of these two requests
            if set(reqs[ii].keys()) == set(reqs[jj].keys()):
                intn, d12, d21 = hcube_intdiff(
                    reqs[ii], reqs[jj], date_field=date_field
                )
            else:
                intn = None

            # An intersection represents duplicated fields. Replace ii with the
            # intersection and the ii remainder; replace jj with just the jj
            # remainder.
            if intn is not None:
                reqs[:] = (
                    reqs[0:ii] + [intn] + d12 + reqs[ii + 1 : jj] + d21 + reqs[jj + 1 :]
                )
                jj += len(d12) + len(d21) - 1
                # ii_incr += len(d12) # This was a bug

            jj += 1
        ii += ii_incr


def parent_child_keys(req1, req2):
    """Deduce if req1.keys or req2.keys is a subset of the other."""
    if set(req1.keys()) > set(req2.keys()):
        parent = req1
        child = req2
    elif set(req1.keys()) < set(req2.keys()):
        child = req1
        parent = req2
    else:
        return False, req1, req2
    return True, parent, child


def remove_subsets(requests: list[dict[str, list]]):
    """Remove any requests which are subsets of other requests in the list.
    A subset is a request where all key: [values] are found in another dictionary in the list.
    e.g.:
       {"a": [1, 2], "b": [3]} is a subset of {"a": [1, 2], "b": [3], "c": [4]}
       {"a": [1], "b": [3]} is a subset of {"a": [1, 2], "b": [3]}.
    """
    removal_complete = False
    cnt = 0
    while not removal_complete and len(requests) > 1 and cnt < 100:
        cnt += 1
        reduced_requests_index = set()
        subset_requests = set()
        for i, req1 in enumerate(requests):
            for _j, req2 in enumerate(requests[i + 1 :]):
                j = i + _j + 1
                if set(req1.keys()) == set(req2.keys()):
                    # siblings, so the values of req1 and req2 could be subsets of each other
                    #  we only consider them as subsets if all values in one is a complete
                    #  subset of the other
                    req2_is_subset = all(
                        [
                            set(_ensure_list(req1[k])) >= set(_ensure_list(req2[k]))
                            for k in req1.keys()
                        ]
                    )
                    if req2_is_subset:
                        subset_requests.add(j)
                    req1_is_subset = all(
                        [
                            set(_ensure_list(req2[k])) >= set(_ensure_list(req1[k]))
                            for k in req1.keys()
                        ]
                    )
                    if req1_is_subset:
                        subset_requests.add(i)

                subset_keys, parent, child = parent_child_keys(req1, req2)
                # If the keys of the requests are different, they are not subsets
                if not subset_keys:
                    continue

                # Get the index of the child
                ci = i if child == req1 else j

                subset = all(
                    [
                        set(_ensure_list(parent[k])) >= set(_ensure_list(child[k]))
                        for k in child.keys()
                    ]
                )
                # If the child we add to list:
                if subset:
                    subset_requests.add(ci)
            # After all comparisons, if i is not in subset_requests, then it is not a subset
            if i not in subset_requests:
                reduced_requests_index.add(i)

        # We always do one extra iteration to check that we have removed all subsets
        if len(reduced_requests_index) == len(requests):
            removal_complete = True
        requests[:] = [requests[i] for i in sorted(list(reduced_requests_index))]


def hcubes_subtract(reqs1, reqs2, date_field="date"):
    """Return a copy of reqs1 with all fields in reqs2 removed."""
    output = []

    # Expand dates if compressed
    reqs2b = [_expand_dates(r, date_field) for r in reqs2]

    # Loop over all pairs
    for req1 in reqs1:
        # Expand dates if compressed
        req1b = _expand_dates(req1, date_field)
        expanded_dates1 = date_field in req1 and req1b[date_field] != req1[date_field]

        for i2, req2 in enumerate(reqs2b):
            # req2 cannot be subtracted if it contains keys that req1 doesn't
            if set(req2.keys()) <= set(req1b.keys()):
                # If there is an intersection between the two then replace
                # req1 with the difference.
                intn, d12, _ = hcube_intdiff(req1b, req2, date_field=date_field)
                if intn is not None:
                    diff = hcubes_subtract(d12, reqs2b[i2 + 1 :], date_field=date_field)
                    break

        else:
            diff = [req1b]

        # Compress dates if expanded
        if expanded_dates1:
            for d in diff:
                if date_field in d:
                    d[date_field] = compress_dates_list(d[date_field])

        output.extend(diff)

    # Deep copy on output to ensure no output lists are references of input
    # ones
    return deepcopy(output)


def hcubes_merge(requests):
    """Merge mergeable hypercubes into each other."""
    assert_lists(requests, "request")

    merge_occurred = True
    while merge_occurred:
        merge_occurred = False

        # Loop over pairs of requests
        for i1, req in enumerate(requests):
            if not req:
                continue
            for i2 in range(i1 + 1, len(requests)):
                req2 = requests[i2]

                # Attempt to merge req2 into req
                success = hcube_merge(req, req2)
                if success:
                    merge_occurred = True
                    while req2:
                        req2.popitem()

        # Remove destroyed requests
        if merge_occurred:
            while True:
                try:
                    requests.remove({})
                except ValueError:
                    break


def hcube_merge(req, req2, nomerge_keys=[]):
    """Attempt to merge req2 into req. Return True if successful and False
    if not.
    """
    if sorted(req2.keys()) != sorted(req.keys()):
        return False

    # At most one key may have different values to be mergeable
    diffkeys = []
    for k in req.keys():
        if sorted(req[k]) != sorted(req2[k]):
            if k in nomerge_keys:
                return False
            diffkeys.append(k)
        if len(diffkeys) > 1:
            break
    else:
        if diffkeys:
            k = diffkeys[0]
            req[k].extend([v for v in req2[k] if v not in req[k]])
        return True
    return False


# def hcubes_merge2(hcubes, date_field='date'):
#     """Another version of hcubes_merge that is intended to result in a more
#        merged result in the case where the best merge cannot be obtained
#        merely by successive merges of pairs. Likely much slower."""
#
#     keys = hcubes[0].keys()
#
#     if date_field in keys:
#         for hcube in hcubes:
#             hcube[date_field] = expand_dates_list(hcube[date_field])
#
#     # Get the envelope of all the hypercubes
#     envelope = {k: set() for k in hcubes[0].keys()}
#     for hcube in hcubes:
#         assert hcube.keys() == keys
#         for k in keys:
#             assert isinstance(hcube[k], list)
#             envelope[k].update(hcube[k])
#     for k in keys:
#         envelope[k] = sorted(list(envelope[k]))
#
#     # Subtract the hypercubes from the envelope to get the fields which are
#     # inside the envelope but not included in the hcubes
#     _, not_included, _ = hcubes_intdiff([envelope], hcubes)
#
#     # Subtract the fields not included in hcubes from the envelope to arrive
#     # at another representation of hcubes
#     _, merged, _ = hcubes_intdiff([envelope], not_included)
#
#     hcubes[:] = merged


def hcubes_reduce_dims(reqs1, reqs2):
    """Return a copy of reqs1 after removing all keys not present in any of
    reqs2 and performing a merge on the result.
    """
    reqs1 = _ensure_list(reqs1)
    reqs2 = _ensure_list(reqs2)
    keys2 = set(chain(*(r.keys() for r in reqs2)))
    reqs1b = [{k: deepcopy(v) for k, v in r.items() if k in keys2} for r in reqs1]
    hcubes_merge(reqs1b)
    return reqs1b


def hcubes_sort(hcubes, hcube_key=None, key_key=None, value_key=None):
    """Sort every aspect of the hypercubes: their order, the order of the
    keys within a given cube and the order of the values within a given
    key.
    """
    # By default values will be sorted numerically if possible
    if value_key is None:

        def sort_values(values):
            try:
                return sorted(values, key=lambda x: float(x))
            except ValueError:
                return sorted(values)

    else:

        def sort_values(values):
            return sorted(values, key=value_key)

    # Sort keys and values
    for ic, hcube in enumerate(hcubes):
        hcubes[ic] = odict(
            [(k, sort_values(hcube[k])) for k in sorted(hcube.keys(), key=key_key)]
        )

    # Sort the hypercubes themselves
    if hcube_key is None:

        def hcube_key(hcube):
            return repr(hcube)

    hcubes[:] = sorted(hcubes, key=hcube_key)


def hcubes_split(requests, nfields):
    """Return the list of requests with any representing more than nfields
    fields split up into smaller requests
    .
    """
    remainder = _ensure_list(requests)
    output = []
    while remainder:
        subreq, remainder0 = hcube_extract(remainder[0], nfields)
        if remainder0:
            remainder = remainder0 + remainder[1:]
        else:
            remainder = remainder[1:]
        output.append(subreq)
    return output


def hcubes_chunk(requests, chunk_size, date_field="date", max_groups=None):
    """Chop and group the hypercubes into groups of chunk_size fields. This
    differs from hcubes_split in its grouping aspect - the output is a list
    of lists, with each sub-list containing requests that do not exceed
    chunk_size fields.
    """
    requests = _ensure_list(requests)
    assert_lists(requests, "requests")

    # Loop until all fields assigned to an output group
    output = []
    nfields = chunk_size
    while requests:
        if nfields == chunk_size:
            # Need to start a new group
            if max_groups and len(output) == max_groups:
                break
            output.append([])
            nfields = 0

        # Extract at most (chunk_size - nfields) fields from request[0]
        subreq, remainder = hcube_extract(requests[0], chunk_size - nfields)
        output[-1].append(subreq)
        nfields += count_fields(subreq)

        if remainder:
            requests = remainder + requests[1:]
        else:
            requests = requests[1:]

    return output


def hcube_extract(request, nfields, date_field="date"):
    """Extract at most nfields fields from request, returning that sub request
    and a list of hypercubes comprising the remaining fields
    .
    """
    assert_lists(request, "request")

    # Expand compressed date ranges
    if "date" in request:
        request = request.copy()
        request["date"] = expand_dates_list(request["date"])

    if count_fields(request) > nfields:
        # Strip it down to its first field
        subreq = {k: [v[0]] for k, v in request.items()}
        nsubreq = 1

        # Add in more fields until we hit the max size
        for k, v in request.items():
            # Calculate the max number of values we can add from this key
            # without nsubreq exceeding nfields
            maxnv = nfields // nsubreq
            assert maxnv > 0
            nv = min(maxnv, len(v))
            subreq[k] = v[0:nv]
            nsubreq *= nv
            if nv < len(v):
                break

        # Subtract the subrequest from the input to get the remaining fields
        _, remainder, _ = hcube_intdiff(request, subreq, date_field=date_field)

        # Restore remainder key order for tidiness
        remainder = [
            {
                k: r[k]
                for k in sorted(r.keys(), key=lambda k: list(request.keys()).index(k))
            }
            for r in remainder
        ]

    else:
        subreq = deepcopy(request)
        remainder = []

    return [subreq, remainder]


def dict_sort_keys(adict, key=None):
    """Sort a dictionary's keys in place."""
    keys = sorted(adict.keys(), key=key)
    bdict = adict.copy()
    for k in keys:
        del adict[k]
        adict[k] = bdict[k]


def dates_to_ints(hcubes, date_field="date"):
    """Convert dates to integers."""
    for hcube in hcubes:
        dates = hcube.get(date_field, [])
        if dates:
            dates = _ensure_list(dates)
            hcube[date_field] = [
                int(d.strftime("%Y%m%d"))
                for d in expand_dates_list(dates, as_datetime=True)
            ]


def assert_lists(reqs, name="requests"):
    """Check that all request values are lists."""
    reqs = _ensure_list(reqs)
    for r in reqs:
        for k, v in r.items():
            if not isinstance(v, list):
                raise Exception(
                    "{n}[{k}]".format(n=name, k=repr(k))
                    + " is not a list: {}".format(repr(v))
                )


def count_fields(hcubes, date_field="date", ignore=[]):
    """Count the fields represented by the hypercube or list of hypercubes."""
    nfields = 0
    hcubes = _ensure_list(hcubes)
    if isinstance(ignore, str):  # We allow ignore to be list, tuple, set, etc.
        ignore = [ignore]
    for hcube in hcubes:
        hcube = {k: v for k, v in hcube.items() if k not in ignore}
        if not hcube:
            continue
        nf = 1
        for k, v in hcube.items():
            v = _ensure_list(v)
            if k == date_field:
                v = expand_dates_list(v)
            nf *= len(v)
        nfields += nf
    return nfields


def check_no_shared_lists(reqs):
    """Debugging tool to check that no pair of requests share a reference to
    the same list
    .
    """
    ids = {}
    for i, r in enumerate(reqs):
        for k, v in r.items():
            if id(v) not in ids:
                ids[id(v)] = (i, k)
            else:
                i2, k2 = ids[id(v)]
                raise Exception(
                    'Key "{}" of request {}'.format(k2, i2)
                    + " shares the same list as key "
                    + '"{}" of request {}'.format(k, i)
                )


def unfactorise(hcubes, date_field="date"):
    """Generator function that, for a list of hypercubes, yields each individual
    field as a dict in order.
    """
    expanders = {date_field: expand_dates_list}

    for hcube in _ensure_list(hcubes):
        value_lists = [expanders.get(k, _ensure_list)(v) for k, v in hcube.items()]
        for values in product(*value_lists):
            yield {k: v for k, v in zip(hcube.keys(), values)}


def _ensure_list(x):
    return x if isinstance(x, (list, tuple)) else [x]

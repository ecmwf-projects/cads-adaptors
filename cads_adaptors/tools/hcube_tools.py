# copied from cdscommon

"""Functions for processing hypercubes represented as dicts."""

# dicts became ordered by default from Python 3.6
import sys
from itertools import product

from .date_tools import expand_dates_list

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

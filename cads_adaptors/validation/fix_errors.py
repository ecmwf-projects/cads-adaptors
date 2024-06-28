import logging
from copy import deepcopy

from .fixers import BaseFixer


def fix_errors(request, errors, logger=None):
    """
    Attempt to fix the validation errors. Return modified request (perhaps
    modified in-place, perhaps not, depending on error) and boolean
    indicating whether at least one fix was made.
    """
    lg = logger if (logger is not None) else logging.getLogger(__name__)

    lg.debug("===================================================")

    # Attempt to fix the errors
    paths_altered = []
    for error in errors:
        log_error(error, lg)

        # Only attempt a fix if not already fixed an error in this JSON
        # path. Fixing >1 error in a path without revalidating is probably a
        # bad idea.
        error_path = list(error.absolute_path)
        if not [p for p in paths_altered if same_root_path(p, error_path)]:
            # Loop over all fixers until one fixes the error
            request_orig = deepcopy(request)
            for fixer in BaseFixer.all_fixers:
                # The request will be altered in-place if possible, but it won't
                # be if the problem is at the top level, e.g. it needs changing
                # from a dict to a list containing a dict or vice-versa. If the
                # request couldn't be fixed this returns None.
                fixed_request = fixer(request, error).fix()
                if fixed_request is not None:
                    request = fixed_request
                    paths_altered.append(error_path)
                    lg.debug(
                        f"modified request={request!r} " + str(request == request_orig)
                    )
                    break

    return request, (len(paths_altered) > 0)


def same_root_path(path1, path2):
    """
    Return True if both paths follow the same route, although one may have
    gone further than the other.
    """
    n = min(len(path1), len(path2))
    return path1[0:n] == path2[0:n]


def log_error(e, lg):
    """For debugging."""
    show = [
        "message",
        "validator",
        "validator_value",
        "schema",
        "relative_schema_path",
        "absolute_schema_path",
        "schema_path",
        "relative_path",
        "absolute_path",
        "json_path",
        "instance",
        "context",
        "cause",
    ]
    for k in show:
        lg.debug(f"e.{k}=" + repr(getattr(e, k, "NOT DEFINED")))
    # for k in dir(e):
    #    if k not in show and not callable(getattr(e, k)):
    #        lg.debug(f'xxx e.{k}=' + repr(getattr(e, k, 'NOT DEFINED')))

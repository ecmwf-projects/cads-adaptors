import json
import logging
import re
from copy import deepcopy

import jsonschema.exceptions

from .error_message import error_message
from .fix_errors import fix_errors
from .get_validator import get_validator


def enforce(request, schema, logger=None):
    """
    Check whether the request conforms to the schema and if it doesn't,
    attempt to make it. If it cannot be made to conform, raise a
    BadRequest exception with an informative error message about which aspect
    of it is wrong.
    """
    lg = logger if (logger is not None) else logging.getLogger(__name__)

    request = deepcopy(request)
    schema = deepcopy(schema)

    # JSON schema validation chokes on non-string keys so check these first.
    # Note that non-string keys are not valid JSON.
    recursive_call(request, check_key_is_string, text_path="request")

    # For any string items that have had '_splitOn' defined, set a regex
    # pattern that makes the splitOn character an illegal character in the
    # string
    recursive_call(schema, set_split_pattern)

    # Object which will do the schema validation
    validator = get_validator(schema)

    lg.debug("Schema: " + json.dumps(schema, indent=None))

    # Loop while errors exist and we appear to be making progress in fixing
    # them
    count = 0
    while True:
        count += 1
        if count == 999:
            lg.error("enforce_schema: likely infinite loop detected")
            break

        # Get a list of all validation errrors
        try:
            errors = list(validator.iter_errors(request))
        except jsonschema.exceptions.SchemaError as e:
            lg.error(repr(e))
            raise Exception("Supplied JSON schema is not valid")
        except Exception as e:
            # This can happen if you supply a request that could not have come
            # from JSON (e.g. non-string dict keys) or an invalid schema (e.g.
            # invalid type strings).
            lg.error(repr(e))
            raise Exception(["Request fails validation: " + repr(request)])

        # Attempt to fix errors
        if not schema.get("_noFixes"):
            request, progress = fix_errors(request, errors, logger=logger)
        else:
            progress = False
        if not progress:
            break

    # If unfixed errors remain then the request is bad
    if errors:
        # Get the list of unique error messages. There can be duplicates if the
        # constraints have been applied because then you can have multiple dicts
        # each of which can generate the same error. They would be different if
        # the dict-list subscripts were included but they're not as they're
        # not considered meaningful to the user. We preserve message order just
        # in case there's some particular sense to the order, which there may
        # well not be.
        msgs = []
        for msg in [error_message(e) for e in errors]:
            if msg not in msgs:
                msgs.append(msg)
        raise BadRequest(msgs)

    return request


def recursive_call(item, func, path=[], text_path="root"):
    """
    Recursively traverse the input item, calling the function on it and all
    nested list and dictionary values.
    """
    func(item, path=path, text_path=text_path)

    if isinstance(item, (list, tuple)):
        for iv, v in enumerate(item):
            recursive_call(
                v, func, path=path + [(item, iv)], text_path=f"{text_path}[{iv}]"
            )

    elif isinstance(item, dict):
        for k, v in item.items():
            recursive_call(
                v, func, path=path + [(item, k)], text_path=f"{text_path}[{k!r}]"
            )


def check_key_is_string(item, path=None, text_path=None):
    """
    If this item is a dict value then check its associated key is of type
    string.
    """
    if path:
        parent, index = path[-1]
        if isinstance(parent, dict) and not isinstance(index, str):
            raise BadRequest([f"{text_path}: dict key is not of type " "string"])


def set_split_pattern(item, **kwargs):
    """
    For schema string items that have "_splitOn" defined, set a default
    pattern in the that excludes split characters. Not necessary for
    numbers as any string will already be an illegal value that will
    trigger a fix.
    """
    if (
        isinstance(item, dict)
        and item.get("type") == "string"
        and item.get("_splitOn")
        and "pattern" not in item
    ):
        item["pattern"] = "^[^" + re.escape(item["_splitOn"]) + "]*$"


class BadRequest(Exception):

    def __init__(self, msgs):

        if isinstance(msgs, str):
            msgs = [msgs]
        super().__init__("\n".join(msgs))

        self.messages = deepcopy(msgs)

"""
Define classes specific to certain types of validation error that can be
used to construct a better error message.
"""

import logging
import re
import sys

logger = logging.getLogger(__name__)


def error_message(error):
    """
    Return an associated validation error message. This may be an improvement
    on, or replacement for, the existing message attribute.
    """
    # In addition to the ones defined below, error message classes, can be
    # specified in the schema as class names. Convert names to classes.
    error_classes = []
    for cls in error.schema.get("_error_classes", []):
        module = ".".join(cls.split(".")[:-1])
        classname = cls.split(".")[-1]
        error_classes.append(getattr(sys.modules[module], classname))

    # Find a class relevant to this error type
    for cls in error_classes + ErrorMessageBase.subclasses:
        if cls.relevant(error):
            break
    else:
        cls = ErrorMessageBase

    # Construct the error message
    msg = cls().message(error)

    # Prepend the message with the path to the erroneous dict key
    path = "request"
    for item in error.absolute_path:
        if isinstance(item, int):
            item = str(item)
        else:
            item = "'" + item + "'"
        path += "[" + item + "]"

    # Remove any request list indices from the start of the path
    # (i.e. "request[0]" -> "request") as the list is only the effect of
    # applying the constraints, which is not visible to the user and may just
    # confuse them
    path = re.sub(r"^request\[[0-9]+\]", "request", path)

    return path + ": " + msg


class ErrorMessageBase:
    """
    Base class for generating an error message from a validation error.
    Used when no more specific sub-class is relevant.
    """

    # A list of sub-classes will be maintained here. The frozen attribute is
    # used to prevent additional user-defined sub-classes being added to the
    # list, as they may only be required for one adaptor. Additional class
    # names can be specified in the relevant schema.
    subclasses = []
    frozen = False

    def __init_subclass__(cls):
        """Keep a list of all sub classes."""
        if not ErrorMessageBase.frozen:
            ErrorMessageBase.subclasses.append(cls)

    @classmethod
    def message(cls, error):
        msg = error.message
        # Replace JSON-schema language with Python language
        msg = (
            msg.replace("is not of type 'array'", "is not of type 'list'")
            .replace("is not of type 'object'", "is not of type 'dict'")
            .replace("is not of type 'number'", "is not a valid number")
            .replace("is not of type 'integer'", "is not a valid integer")
        )
        msg = re.sub(r"\bproperty\b", "key", msg)
        msg = re.sub(r"\bproperties\b", "keys", msg)
        return msg


class MinMaxItems(ErrorMessageBase):
    """Generates error messages for arrays of incorrect length."""

    @classmethod
    def relevant(cls, error):
        return error.validator in ["minItems", "maxItems"]

    @classmethod
    def message(cls, error, rmapping):
        msg = {
            "minItems": f"{error.instance}: list has too few items",
            "maxItems": f"{error.instance}: list has too many items",
        }[error.validator]
        minItems = error.schema.get("minItems")
        maxItems = error.schema.get("maxItems")
        if minItems and minItems == maxItems:
            msg += f". Should have exactly {minItems}"
        elif error.validator == "minItems" and minItems:
            msg += f". Should have at least {minItems}"
        elif error.validator == "maxItems" and maxItems:
            msg = (
                f"{error.instance} list has too many items. Should have "
                f"no more than {maxItems}"
            )
        return msg


class MinMaxStringLength(ErrorMessageBase):
    """Generates error messages for strings of incorrect length."""

    @classmethod
    def relevant(cls, error):
        return error.validator in ["minLength", "maxLength"]

    @classmethod
    def message(cls, error, rmapping):
        msg = {
            "minLength": f"'{error.instance}': string is too short",
            "maxLength": f"'{error.instance}': string is too long",
        }[error.validator]
        minLength = error.schema.get("minLength")
        maxLength = error.schema.get("maxLength")
        if minLength is not None and minLength == maxLength:
            msg += f". Should have exactly {minLength} character(s)"
        elif error.validator == "minLength" and minLength:
            msg += f". Should have at least {minLength} character(s)"
        elif error.validator == "maxLength" and maxLength:
            msg += f". Should have no more than {maxLength} character(s)"
        return msg


class RequiredProperties(ErrorMessageBase):
    """Generates error messages for objects without obligatory properties."""

    @classmethod
    def relevant(cls, error):
        return error.validator == "required"

    @classmethod
    def message(cls, error, rmapping):
        m = re.match(r"^'(.*)' is a required property.*", error.message)
        if m:
            key = rmapping["rename"].get(m.group(1), m.group(1))
            msg = f"missing mandatory key: '{key}'"
        else:
            # We were unable to obtain the missing key name so just return
            # the error message we were given. This should never happen.
            msg = super().message(error, rmapping)
        return msg


class UniqueItems(ErrorMessageBase):
    """Generates error messages for lists with repeated items."""

    @classmethod
    def relevant(cls, error):
        return error.validator == "uniqueItems"

    @classmethod
    def message(cls, error, rmapping):
        msg = "has repeated values in the list"
        # Try to get an example of a repeated item. There's no known reason this
        # should fail but we'll be cautious anyway and ignore any error
        try:
            key = error.absolute_path[-1]
            key = rmapping["rename"].get(key, key)
            # The item list should be in error.instance. Find a repeated value.
            for item in error.instance:
                if error.instance.count(item) > 1:
                    # Attempt to unmap back to front-end space and add to msg
                    value = rmapping["remap"].get(key, {}).get(item, item)
                    msg += f", e.g. '{value}'"
                    break
        except Exception:
            pass
        return msg


class AdditionalProperties(ErrorMessageBase):
    """Generates error messages for objects with forbidden properties."""

    @classmethod
    def relevant(cls, error):
        return error.validator == "additionalProperties"

    @classmethod
    def message(cls, error, rmapping):

        # Unfortunately, in the currently used version of jsonschema, the list
        # of offending keys is not provided in the error object and has to be
        # parsed from the error message.

        # Property is forbidden because the name doesn't match a regex?
        m = re.match(
            r"^'(.*)' does not match any of the regexes:(.*)",
            error.message,
            flags=re.DOTALL,
        )
        if m:
            key = rmapping["rename"].get(m.group(1), m.group(1))
            regexes = m.group(2)
            msg = f"'{key}' is an invalid key name"
            if error.schema.get("_onErrorShowPattern", True):
                msg += (
                    " because it does not match one of the following "
                    f"regular expressions:{regexes}"
                )
            return msg

        # Property is forbidden because it's not in a white-list of names?
        m = re.match(
            "Additional properties are not allowed \((.*) " "(was|were) unexpected\)",
            error.message,
            flags=re.DOTALL,
        )
        if m:
            try:
                keystring = m.group(1)
                keys = []
                # keystring should be of form "'key1', 'key2', 'key3', ...".
                # Parse to ["'key1'", "'key2'", "'key3'", ...]
                while keystring:
                    m = re.match(r" *(?P<k>'[^']+')(, *(?P<r>.+)| *)", keystring)
                    if not m:
                        raise Exception("Unexpected message format: " f'"{keystring}"')
                    keys.append(m.group("k"))
                    keystring = m.group("r")
                keys = sorted(keys)  # Order they appear in message is unstable
                name_s = "name" + ("s" if (len(keys) > 1) else "")
                return f"Invalid key {name_s}: " + ", ".join(keys)
            except Exception:
                pass

        return super().message(error, rmapping)


class PropertyName(ErrorMessageBase):
    """Generates error messages for invalid property names."""

    @classmethod
    def relevant(cls, error):
        try:
            return list(error.schema_path)[-2:] == ["propertyNames", "pattern"]
        except Exception:
            return False

    @classmethod
    def message(cls, error, rmapping):
        key = rmapping["rename"].get(error.instance, error.instance)
        return (
            f"'{key}' is an invalid key name because it does not match "
            "the following regular expression: " + repr(error.validator_value)
        )


class StringPattern(ErrorMessageBase):
    """Generates error messages for strings not matching a regex."""

    @classmethod
    def relevant(cls, error):
        return error.validator == "pattern"

    @classmethod
    def message(cls, error, rmapping):
        msg = f"invalid value: '{error.instance}'"
        if error.schema.get("_onErrorShowPattern", True):
            msg += (
                ". Valid values must match regular expression "
                + f"'{error.validator_value}'"
            )
        return msg


class Date(ErrorMessageBase):
    """Generates error messages for malformatted date strings."""

    @classmethod
    def relevant(cls, error):
        return error.validator == "format" and error.validator_value == "date"

    @classmethod
    def message(cls, error, rmapping):
        return (
            f'"{error.instance}" is not a valid date. Expected format ' "is yyyy-mm-dd"
        )


class BadFormat(ErrorMessageBase):
    """Generates error messages for malformatted strings."""

    @classmethod
    def relevant(cls, error):
        return error.validator == "format"

    @classmethod
    def message(cls, error, rmapping):
        return f"'{error.instance}' is not a valid {error.validator_value}"


# Do not add any more sub classes to the list
ErrorMessageBase.frozen = True

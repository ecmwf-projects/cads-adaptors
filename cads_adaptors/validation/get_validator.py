from datetime import datetime

import jsonschema as js


@js.FormatChecker.cls_checks("numeric string")
def numeric_checker(item):
    """Register jsonschema format checker for format='numeric string'."""
    try:
        assert isinstance(item, str)
        float(item)
    except Exception:
        return False
    else:
        return True


@js.FormatChecker.cls_checks("positive numeric string")
def positive_numeric_checker(item):
    """Register jsonschema format checker for format='positive numeric string'."""
    try:
        assert isinstance(item, str)
        return float(item) > 0
    except Exception:
        return False


@js.FormatChecker.cls_checks("date or date range")
def date_or_date_range_checker(item):
    """Register jsonschema format checker for format='date or date range'."""
    return date_or_date_range(item)


@js.FormatChecker.cls_checks("date range")
def date_range_checker(item):
    """Register jsonschema format checker for format='date range'."""
    return date_or_date_range(item) and "/" in item


def date_or_date_range(item):
    """
    Return True if the item is a %Y-%m-%d date or a %Y-%m-%d/%Y-%m-%d date
    range and False otherwise.
    """
    try:
        items = item.split("/")
        dates = [datetime.strptime(item, "%Y-%m-%d") for item in items]
    except Exception:
        return False
    if len(items) not in [1, 2]:
        return False
    if len(items) == 2 and dates[1] < dates[0]:
        return False
    return True


def get_validator(schema):
    """
    Return object which will do the schema validation. Any additional
    FormatChecker classes must be already defined before this call.
    """
    # Get the validator class for the requested draft. If $schema is specified
    # attempt to look up the class name from the URI. Otherwise require "_draft"
    # to be specified.
    if schema.get("$schema"):
        # List of all validator classes. At time of writing can't use
        # isinstance(base class) because they don't share one.
        validators = [
            getattr(js, name)
            for name in dir(js)
            if hasattr(getattr(js, name), "META_SCHEMA")
        ]
        if not validators:
            raise Exception("Found no validator objects in jsonschema???")

        # Get the validator class for this schema
        schema2validator = {v.META_SCHEMA["$schema"]: v for v in validators}
        if schema["$schema"] not in schema2validator:
            raise Exception(
                'Unrecognised schema: "' + schema["$schema"] + '". '
                "Known schemas are: " + ", ".join(schema2validator.keys())
            )
        cls = schema2validator[schema["$schema"]]

    else:
        draft = schema.get("_draft")
        if draft:
            xdraft = str(draft).replace("-", "")
            cls = getattr(js, f"Draft{xdraft}Validator", None)
            if cls is None:
                raise Exception(f"Invalid draft in schema: {draft}")
        else:
            raise Exception('You should set "$schema" or "_draft" in your schema')

    return cls(schema, format_checker=js.FormatChecker())

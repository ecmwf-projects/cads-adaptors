"""Main module of the request-constraints API."""
import copy
import re
from typing import Any

from datetimerange import DateTimeRange


SUPPORTED_CONSTRAINTS = [
    "StringListWidget",
    "StringListArrayWidget",
    "StringChoiceWidget",
    "DateRangeWidget",
]


class ParameterError(TypeError):
    pass


def remove_unsupported_vars(
    constraints: list[dict[str, set[Any]]], unsupported_vars: list[str]
) -> list[dict[str, set[Any]]]:
    constraints = copy.deepcopy(constraints)
    for constraint in constraints:
        for var in unsupported_vars:
            constraint.pop(var, None)
    return constraints


def ensure_sequence(v: Any) -> list[Any] | tuple[Any]:
    if not isinstance(v, list | tuple):
        v = [v]
    return v  # type: ignore


def parse_constraints(
    constraints: list[dict[str, Any]] | dict[str, Any] | None,
) -> list[dict[str, set[Any]]]:
    """
    Parse constraints for a given dataset. Convert dict[str, list[Any]] into dict[str, Set[Any]].

    :param constraints: constraints in JSON format
    :type: list[dict[str, list[Any]]]

    :rtype: list[dict[str, set[Any]]]:
    :return: list of dict[str, set[Any]] containing all constraints
    for a given dataset.

    """
    if constraints is None:
        constraints = []
    if not isinstance(constraints, list):
        constraints = [constraints]
    result = []
    for combination in constraints:
        parsed_combination = {}
        for key, values in combination.items():
            parsed_combination[key] = set(ensure_sequence(values))
        result.append(parsed_combination)
    return result


def parse_selection(selection: dict[str, list[Any] | str]) -> dict[str, set[Any]]:
    """
    Parse current selection and convert dict[str, list[Any]] into dict[str, set[Any]].

    :param selection: a dictionary containing the current selection
    :type: dict[str, list[Any]]

    :rtype: dict[str, set[Any]]:
    :return: a dict[str, set[Any]] containing the current selection.
    """
    result = {}
    for field_name, field_values in selection.items():
        result[field_name] = set(ensure_sequence(field_values))
    return result


def apply_constraints(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
    widget_type: dict[str, str],
) -> dict[str, list[Any]]:
    """
    Apply dataset constraints to the current selection.

    :param form: a dictionary of all selectable values
    grouped by field name
    :param constraints: a list of all constraints
    :param selection: a dictionary containing the current selection
    :param widget_type: a dictionary, the keys are the name of the parameters
    and the values are the type of widget used.
    :return: a dictionary containing all values that should be left
    active for selection, in JSON format
    """
    always_valid = dict()

    form = copy.deepcopy(form)
    selection = copy.deepcopy(selection)
    for key, value in form.copy().items():
        if key not in get_keys(constraints):
            always_valid[key] = form.pop(key)
            selection.pop(key, None)

    result = get_form_state(form, selection, constraints, widget_type=widget_type)
    result.update(always_valid)

    return format_to_json(result)


def get_possible_values(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
    widget_type: dict[str, str] = {}
) -> dict[str, set[Any]]:
    """
    Get possible values given the current selection.

    Works only for enumerated fields, i.e. fields with values
    that must be selected one by one (no ranges).
    Checks the current selection against all constraints.
    A combination is valid if every field contains
    at least one value from the current selection.
    If a combination is valid, its values are added to the pool
    of valid values (i.e. those that can still be selected without
    running into an invalid request).

    :param form: a dict of all selectable fields and values
    e.g. form = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"}
    }
    :type: dict[str, set[Any]]:

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :param widget_type: a dictionary, the keys are the name of the parameters
    and the values are the type of widget used.
    e.g. widget_type = {
        "param": "StringListWidget",
        "step": "StringListWidget", "date":
        "DateRangeWidget"
    }
    :type: dict[str, str]:

    :param selection: a dictionary containing the current selection
    e.g. selection = {
        "param": {"T"},
        "level": {"850", "500"},
        "step": {"36"}
    }
    :type: dict[str, set[Any]]:

    :rtype: dict[str, set[Any]]
    :return: a dictionary containing all possible values given the current selection
    e.g.
    {'level': {'500', '850'}, 'param': {'T', 'Z'}, 'step': {'24', '36', '48'}}

    """
    print(widget_type)
    result: dict[str, set[Any]] = {key: set() for key in form}
    for combination in constraints:
        ok = True
        for key, values in selection.items():
            if key in combination.keys():
                if key != "date":
                    if len(values & combination[key]) == 0:
                        ok = False
                        break
                else:
                    selected = gen_time_range_from_string(values.copy().pop())
                    valid = [
                        gen_time_range_from_string(valid) for valid in combination[key]
                    ]
                    if not temporal_intersection_between(selected, valid):
                        ok = False
                        break

            elif key in form.keys():
                ok = False
                break
            else:
                raise ParameterError(f"invalid param '{key}'")
        if ok:
            for key, values in combination.items():
                result[key] |= set(values)

    return result


def format_to_json(result: dict[str, set[Any]]) -> dict[str, list[Any]]:
    """
    Convert dict[str, set[Any]] into dict[str, list[Any]].

    :param result: dict[str, set[Any]] containing a possible form state
    :type: dict[str, set[Any]]:

    :rtype: dict[str, list[Any]]
    :return: the same values in dict[str, list[Any]] format

    """
    return {k: sorted(v) for (k, v) in result.items()}


def get_form_state(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
    widget_type: dict[str, str] = {},
) -> dict[str, set[Any]]:
    """
    Call get_possible_values() once for each key in form.

    :param form: a dict of all selectable fields and values
    e.g. form = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"}
    }
    :type: dict[str, set[Any]]:

    :param selection: a dictionary containing the current selection
    e.g. selection = {
        "param": {"T"},
        "level": {"850", "500"},
        "step": {"36"}
    }
    :type: dict[str, set[Any]]:

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :param widget_type: a dictionary, the keys are the name of the parameters
    and the values are the type of widget used.
    e.g. widget_type = {
        "param": "StringListWidget",
        "step": "StringListWidget", "date":
        "DateRangeWidget"
    }
    :type: dict[str, str]:

    :rtype: dict[str, set[Any]]
    :return: a dictionary containing all form values to be left active given the current selection

    e.g.
    {'level': {'500', '850'}, 'param': {'T', 'Z'}, 'step': {'24', '36', '48'}}

    """
    result: dict[str, set[Any]] = {key: set() for key in form}

    for key in form:
        sub_selection = selection.copy()
        if key in sub_selection:
            sub_selection.pop(key)
        sub_results = get_possible_values(form, sub_selection, constraints, widget_type=widget_type)
        result[key] = sub_results.setdefault(key, set())
    return result


def get_always_valid_params(
    form: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
) -> dict[str, set[Any]]:
    """
    Get always valid field and values.

    :param form: a dict of all selectable fields and values
    e.g. form = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"}
    }
    :type: dict[str, set[Any]]:

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :rtype: dict[str, set[Any]]
    :return: A dictionary containing fields and values that are not constrained (i.e. they are always valid)

    """
    result: dict[str, set[Any]] = {}
    for field_name, field_values in form.items():
        if field_name not in get_keys(constraints):
            result.setdefault(field_name, field_values)
    return result


def values_from_groups(groups: list[dict[str, Any]]):
    values = []
    for group in groups:
        if "values" in group:
            values.extend(group["values"])
        elif "groups" in group:
            v = values_from_groups(group["groups"])
            values.extend(v)
        else:
            pass
    return values


def values_from_string_list_array(schema: dict[str, Any]):
    return set(values_from_groups(schema["details"]["groups"]))


def values_from_string_list(schema: dict[str, Any]):
    values = schema["details"]["values"]
    values = ensure_sequence(values)
    return set(values)


def range_from_date_range(schema: dict[str, Any]):
    return schema["details"]["range"]


SCHEMA_VALUES_PARSER = {
    "StringListWidget": values_from_string_list,
    "StringListArrayWidget": values_from_string_list_array,
    "StringChoiceWidget": values_from_string_list,
    "DateRangeWidget": range_from_date_range,
}


def parse_form(
    form: dict[str, set[Any]] | list[dict[str, set[Any]]] | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """
    list[Any] | dict[str, Any]
            CDS form.
    """
    if form is None:
        form = []
    if not isinstance(form, list):
        form = [form]

    values = {}
    widget_type = {}
    for schema in form:
        name = schema["name"]
        type_ = schema["type"]
        if type_ in SCHEMA_VALUES_PARSER:
            widget_type[name] = type_
            values[name] = SCHEMA_VALUES_PARSER[type_](schema)
    return values, widget_type


def validate_constraints(
    form: list[dict[str, Any]] | dict[str, Any] | None,
    request: dict[str, dict[str, Any]],
    constraints: list[dict[str, Any]] | dict[str, Any] | None,
) -> dict[str, list[str]]:

    """
    :param form: cds form
    :param request: user request
    :param constraints: cds constraints
    """
    values, widget_type = parse_form(form)
    constraints = parse_constraints(constraints)
    selection = parse_selection(request["inputs"])
    return apply_constraints(values, selection, constraints, widget_type=widget_type)


def get_keys(constraints: list[dict[str, Any]]) -> set[str]:
    keys = set()
    for constraint in constraints:
        keys |= set(constraint.keys())
    return keys


def temporal_intersection_between(
    selected: DateTimeRange, ranges: list[DateTimeRange]
) -> bool:
    for valid in ranges:
        if selected.intersection(valid).is_valid_timerange():
            return True
    return False


def gen_time_range_from_string(string: str) -> DateTimeRange:
    dates = re.split("[;/]", string)
    if len(dates) == 1:
        dates *= 2
    time_range = DateTimeRange(dates[0], dates[1])
    time_range.start_time_format = "%Y-%m-%d"
    time_range.end_time_format = "%Y-%m-%d"
    if time_range.is_valid_timerange():
        return time_range
    else:
        raise ValueError("Start date must be before end date")


def get_bounds(ranges: list[DateTimeRange] | set[DateTimeRange]) -> str:
    ranges = [gen_time_range_from_string(_range) for _range in ranges]
    _min = ranges[0].start_datetime
    _max = ranges[0].end_datetime
    if len(ranges) > 1:
        for _range in ranges[1:]:
            if _range.start_datetime < _min:
                _min = _range.start_datetime
            if _range.end_datetime > _max:
                _max = _range.end_datetime

    return f"{_min.strftime('%Y-%m-%d')}/{_max.strftime('%Y-%m-%d')}"

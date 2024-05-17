"""Main module of the request-constraints API."""

import copy
import re
from typing import Any

from datetimerange import DateTimeRange

from . import adaptors, exceptions, translators


def get_unsupported_vars(
    ogc_form: list[dict[str, Any]] | dict[str, Any] | None,
) -> list[str]:
    if ogc_form is None:
        ogc_form = []
    if not isinstance(ogc_form, list):
        ogc_form = [ogc_form]
    unsupported_vars = []
    for schema in ogc_form:
        if schema["type"] not in translators.SCHEMA_TRANSLATORS:
            unsupported_vars.append(schema["name"])
    return unsupported_vars


def remove_unsupported_vars(
    constraints: list[dict[str, set[Any]]], unsupported_vars: list[str]
) -> list[dict[str, set[Any]]]:
    return [
        {k: v for k, v in constraint.items() if k not in unsupported_vars}
        for constraint in constraints
    ]


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
        for field_name, field_values in combination.items():
            parsed_combination[field_name] = set(ensure_sequence(field_values))
        result.append(parsed_combination)
    return result


def parse_selection(
    selection: dict[str, list[Any] | str], unsupported_vars: list[str] = []
) -> dict[str, set[Any]]:
    """
    Parse current selection and convert dict[str, list[Any]] into dict[str, set[Any]].

    :param selection: a dictionary containing the current selection
    :type: dict[str, list[Any]]
    :param unsupported_vars: list of variables not supported
    :type: list[str]

    :rtype: dict[str, set[Any]]:
    :return: a dict[str, set[Any]] containing the current selection.
    """
    result = {}
    for field_name, field_values in selection.items():
        if field_name not in unsupported_vars:
            result[field_name] = set(ensure_sequence(field_values))
    return result


def apply_constraints(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
    widget_types: dict[str, str] = dict(),
) -> dict[str, list[Any]]:
    """
    Apply dataset constraints to the current selection.

    :param form: a dictionary of all selectable values
    grouped by field name
    :param constraints: a list of all constraints
    :param selection: a dictionary containing the current selection
    :return: a dictionary containing all values that should be left
    active for selection, in JSON format
    """
    constraint_keys = get_keys(constraints)
    always_valid = get_always_valid_params(form, constraint_keys)

    form = copy.deepcopy(form)
    selection = copy.deepcopy(selection)
    for key in form.copy():
        if key not in constraint_keys:
            form.pop(key, None)
            selection.pop(key, None)
    result = apply_constraints_in_old_cds_fashion(
        form, selection, constraints, widget_types=widget_types
    )
    result.update(format_to_json(always_valid))

    return result


def get_possible_values(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
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
    result: dict[str, set[Any]] = {key: set() for key in form}
    for combination in constraints:
        ok = True
        for field_name, selected_values in selection.items():
            if field_name in combination.keys():
                if len(selected_values & combination[field_name]) == 0:
                    ok = False
                    break
            elif field_name in form.keys():
                ok = False
                break
            else:
                raise exceptions.ParameterError(f"invalid param '{field_name}'")
        if ok:
            for field_name, valid_values in combination.items():
                result[field_name] |= set(valid_values)

    return result


def get_clean_selection(selection: dict[str, set[Any]]):
    clean_selection = selection.copy()
    NOT_INTERESTING_SELECTION = ["location_x", "location_y"]
    for k in NOT_INTERESTING_SELECTION:
        clean_selection.pop(k, None)
    return clean_selection


def apply_constraints_in_old_cds_fashion(
    form: dict[str, set[Any]],
    selection: dict[str, set[Any]],
    constraints: list[dict[str, set[Any]]],
    widget_types: dict[str, str] = dict(),
) -> dict[str, list[Any]]:
    result: dict[str, set[Any]] = {}

    daterange_widgets = [k for k, v in widget_types.items() if v == "DateRangeWidget"]
    selected_daterange_widgets = [k for k in daterange_widgets if k in list(selection)]
    is_daterange_selection_empty = [
        selection[k] == {""} for k in selected_daterange_widgets
    ]

    if len(selection) == 0 or (
        len(daterange_widgets) > 0
        and len(daterange_widgets) == len(selected_daterange_widgets)
        and all(is_daterange_selection_empty)
    ):
        return format_to_json(form)

    clean_selection = get_clean_selection(selection)

    for constraint in constraints:
        # the per-selected-widget result is the union of:
        # - all constraints containing the selected widget with at least one
        #   value/option in common with the selected values/options (Category 1)
        # - all constraints NOT containing the selected widget (Category 2)

        # loop over the widgets in the selection
        # as a general rule, a widget cannot decide for itself (but only for others)
        # only other widgets can enable/disable options/values in the "current" widget
        per_constraint_result: dict[str, dict[str, set[Any]]] = {}
        for selected_widget_name, selected_widget_options in clean_selection.items():
            selected_widget_type = widget_types.get(
                selected_widget_name, "UNKNOWN_WIDGET_TYPE"
            )
            if selected_widget_name in constraint:
                constraint_is_intersected = False
                if selected_widget_type == "DateRangeWidget":
                    assert (
                        len(selected_widget_options) == 1
                    ), "More than one selected date range!"
                    selected_range = gen_time_range_from_string(
                        next(iter(selected_widget_options))
                    )
                    valid_ranges = [
                        gen_time_range_from_string(valid_range)
                        for valid_range in constraint[selected_widget_name]
                    ]
                    if temporal_intersection_between(selected_range, valid_ranges):
                        constraint_is_intersected = True
                else:
                    constraint_selection_intersection = (
                        selected_widget_options & constraint[selected_widget_name]
                    )
                    if len(constraint_selection_intersection):
                        constraint_is_intersected = True
                if constraint_is_intersected:
                    # factoring in Category 1 constraints
                    if selected_widget_name not in per_constraint_result:
                        per_constraint_result[selected_widget_name] = {}
                        for widget_name in form:
                            if widget_name != selected_widget_name:
                                per_constraint_result[selected_widget_name][
                                    widget_name
                                ] = set()
                    for widget_name, widget_options in constraint.items():
                        if widget_name != selected_widget_name:
                            if (
                                widget_name
                                in per_constraint_result[selected_widget_name]
                            ):
                                per_constraint_result[selected_widget_name][
                                    widget_name
                                ] |= set(widget_options)
                            else:
                                per_constraint_result[selected_widget_name][
                                    widget_name
                                ] = set(widget_options)
            elif selected_widget_name in form.keys():
                # factoring in Category 2 constraints
                if selected_widget_name not in per_constraint_result:
                    per_constraint_result[selected_widget_name] = {}
                    for widget_name in form:
                        if widget_name != selected_widget_name:
                            per_constraint_result[selected_widget_name][
                                widget_name
                            ] = set()
                for widget_name, widget_options in constraint.items():
                    if widget_name in per_constraint_result[selected_widget_name]:
                        per_constraint_result[selected_widget_name][widget_name] |= set(
                            widget_options
                        )
                    else:
                        per_constraint_result[selected_widget_name][widget_name] = set(
                            widget_options
                        )
            else:
                raise exceptions.ParameterError(
                    f"invalid param '{selected_widget_name}'"
                )

        for widget_name in form:
            per_constraint_result_agg: set[Any] = set()
            for selected_widget_name in clean_selection:
                if widget_name != selected_widget_name:
                    if selected_widget_name in per_constraint_result:
                        if per_constraint_result_agg:
                            per_constraint_result_agg &= per_constraint_result[
                                selected_widget_name
                            ][widget_name]
                        else:
                            per_constraint_result_agg = per_constraint_result[
                                selected_widget_name
                            ][widget_name]
                    else:
                        per_constraint_result_agg = set()
                        break
            if widget_name in result:
                result[widget_name] |= per_constraint_result_agg
            else:
                result[widget_name] = per_constraint_result_agg

    for widget_name in form:
        if widget_name not in result:
            result[widget_name] = set()

    # as a general rule, a widget cannot decide for itself (but only for others)
    # only other widgets can enable/disable options/values in the "current" widget
    # when the selection contains only one widget, we need to enable all options for that widget
    # (as an exception from the general rule)
    if len(clean_selection) == 1:
        only_widget_in_selection = next(iter(clean_selection))
        result[only_widget_in_selection] = form[only_widget_in_selection]

    return format_to_json(result)


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

    :param constraints: a list of dictionaries representing
    all constraints for a specific dataset
    e.g. constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    :type: list[dict[str, set[Any]]]:

    :param selection: a dictionary containing the current selection
    e.g. selection = {
        "param": {"T"},
        "level": {"850", "500"},
        "step": {"36"}
    }
    :type: dict[str, set[Any]]:

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
        sub_results = get_possible_values(form, sub_selection, constraints)
        result[key] = sub_results.setdefault(key, set())
    return result


def get_always_valid_params(
    form: dict[str, set[Any]],
    constraint_keys: set[str],
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

    :param constraint_keys: a set of strings representing all constraints keys for a specific dataset
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
        if field_name not in constraint_keys:
            result.setdefault(field_name, field_values)
    return result


def parse_form(cds_form: list[Any] | dict[str, Any] | None) -> dict[str, set[Any]]:
    """
    Parse the form for a given dataset extracting the information on the possible selections.
    :param raw_form: a dictionary containing
    all possible selections in JSON format
    :type: dict[str, list[Any]]
    :rtype: dict[str, set[Any]]:
    :return: a dict[str, set[Any]] containing all possible selections.
    """
    if cds_form is None:
        cds_form = list()
    ogc_form = translators.translate_cds_form(cds_form)
    form = {}
    for field_name in ogc_form:
        try:
            if ogc_form[field_name]["schema_"]["type"] == "array":
                if ogc_form[field_name]["schema_"]["items"].get("enum"):
                    form[field_name] = set(
                        ogc_form[field_name]["schema_"]["items"]["enum"]
                    )
                else:
                    handled = False
                    if ogc_form[field_name]["schema_"].get("default", None):
                        if ogc_form[field_name]["schema_"]["default"].get(
                            "minStart", None
                        ) and ogc_form[field_name]["schema_"]["default"].get(
                            "maxEnd", None
                        ):
                            minStart = ogc_form[field_name]["schema_"]["default"][
                                "minStart"
                            ]
                            maxEnd = ogc_form[field_name]["schema_"]["default"][
                                "maxEnd"
                            ]
                            form[field_name] = set([f"{minStart}/{maxEnd}"])
                            handled = True
                    if not handled:
                        # FIXME: temporarely fix for making constraints working from UI
                        form[field_name] = set()  # type: ignore
            else:
                form[field_name] = set(ogc_form[field_name]["schema_"]["enum"])
        except KeyError:
            pass
    return form


def validate_constraints(
    cds_form: list[dict[str, Any]] | dict[str, Any] | None,
    request: adaptors.Request,
    constraints: list[dict[str, Any]] | dict[str, Any] | None,
) -> dict[str, list[str]]:
    parsed_form = parse_form(cds_form)
    unsupported_vars = get_unsupported_vars(cds_form)
    constraints = parse_constraints(constraints)
    constraints = remove_unsupported_vars(constraints, unsupported_vars)
    selection = parse_selection(request, unsupported_vars)
    # The following 2 cases should not happen, but they have ben typescript, so need to include safeguard
    if isinstance(cds_form, dict):
        cds_form = [cds_form]
    elif cds_form is None:
        cds_form = list([])
    widget_types: dict[str, Any] = {
        widget.get("name", "unknown_widget"): widget["type"]
        for widget in cds_form
        if "type" in widget
    }

    return apply_constraints(
        parsed_form, selection, constraints, widget_types=widget_types
    )


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

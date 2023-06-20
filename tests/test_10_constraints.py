from typing import Any

from cads_adaptors import constraints


def test_get_possible_values() -> None:
    form = {
        "level": {"500", "850"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"mean"},
    }

    raw_constraints = [
        {"level": {"500"}, "param": {"Z", "T"}, "time": {"12:00", "00:00"}},
        {"level": {"850"}, "param": {"T"}, "time": {"12:00", "00:00"}},
        {"level": {"500"}, "param": {"Z", "T"}, "stat": {"mean"}},
    ]

    assert constraints.get_possible_values(
        form, {"stat": {"mean"}}, raw_constraints
    ) == {
        "level": {"500"},
        "time": set(),
        "param": {"Z", "T"},
        "stat": {"mean"},
    }
    assert constraints.get_possible_values(
        form, {"time": {"12:00"}}, raw_constraints
    ) == {
        "level": {"850", "500"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": set(),
    }
    assert constraints.get_possible_values(
        form, {"stat": {"mean"}, "time": {"12:00"}}, raw_constraints
    ) == {"level": set(), "time": set(), "param": set(), "stat": set()}
    assert constraints.get_possible_values(form, {"param": {"Z"}}, raw_constraints) == {
        "level": {"500"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"mean"},
    }
    assert constraints.get_possible_values(
        form, {"level": {"500", "850"}}, raw_constraints
    ) == {
        "level": {"500", "850"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"mean"},
    }


def test_get_form_state() -> None:
    form = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    raw_constraints = [
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert constraints.get_form_state(form, {"level": {"500"}}, raw_constraints) == {
        "level": {"500", "850"},
        "param": {"Z"},
    }


def test_apply_constraints() -> None:
    form = {"level": {"500", "850"}, "param": {"Z", "T"}, "number": {"1"}}

    raw_constraints = [
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert constraints.apply_constraints(form, {"level": {"500"}}, raw_constraints)[
        "number"
    ] == ["1"]


def test_parse_constraints() -> None:
    raw_constraints: list[dict[str, list[Any]]] = [
        {"level": ["500"], "param": ["Z", "T"], "step": ["24", "36", "48"]},
        {"level": ["1000"], "param": ["Z"], "step": ["24", "48"]},
        {"level": ["850"], "param": ["T"], "step": ["36", "48"]},
    ]

    parsed_constraints: list[dict[str, set[Any]]] = [
        {"level": {"500"}, "param": {"Z", "T"}, "step": {"24", "36", "48"}},
        {"level": {"1000"}, "param": {"Z"}, "step": {"24", "48"}},
        {"level": {"850"}, "param": {"T"}, "step": {"36", "48"}},
    ]
    assert parsed_constraints == constraints.parse_constraints(raw_constraints)
    assert [{}] == constraints.parse_constraints([{}])


def test_remove_unsupported_vars() -> None:
    raw_constraints: list[dict[str, Any]] = [
        {"level": ["500"], "param": ["Z", "T"], "step": ["24", "36", "48"]},
        {"level": ["1000"], "param": ["Z"], "step": ["24", "48"]},
        {"level": ["850"], "param": ["T"], "step": ["36", "48"], "unknown": "foo"},
    ]

    parsed_constraints: list[dict[str, list[Any]]] = [
        {"level": ["500"], "param": ["Z", "T"], "step": ["24", "36", "48"]},
        {"level": ["1000"], "param": ["Z"], "step": ["24", "48"]},
        {"level": ["850"], "param": ["T"], "step": ["36", "48"]},
    ]

    form: list[dict[str, Any]] = [
        {
            "details": {
                "groups": [{"values": ["Z"]}, {"values": ["T"]}],
                "default": "Z",
            },
            "name": "param",
            "label": "Variable",
            "type": "StringListArrayWidget",
        },
        {
            "details": {"values": ["500", "850", "1000"], "default": "500"},
            "name": "level",
            "label": "Pressure Level",
            "type": "StringListWidget",
        },
        {
            "details": {"values": ["24", "36", "48"], "default": "24"},
            "name": "step",
            "label": "Step",
            "type": "StringListWidget",
        },
        {
            "details": {"values": ["1", "2", "3"], "default": "1"},
            "name": "number",
            "label": "Number",
            "type": "StringChoiceWidget",
        },
        {
            "details": {"values": ["1", "2", "3"], "default": "1"},
            "name": "area",
            "label": "Area",
            "type": "GeographicExtentWidget",
        },
        {
            "name": "unknown",
            "label": "Don't know hot to handle this",
            "type": "UnknownWidget",
        },
    ]
    unsupported_vars = constraints.get_unsupported_vars(form)

    assert unsupported_vars == ["unknown"]
    assert parsed_constraints == constraints.remove_unsupported_vars(
        raw_constraints, unsupported_vars
    )


def test_parse_form() -> None:
    form: list[dict[str, Any]] = [
        {
            "details": {
                "groups": [{"values": ["Z"]}, {"values": ["T"]}],
                "default": "Z",
            },
            "name": "param",
            "label": "Variable",
            "type": "StringListArrayWidget",
        },
        {
            "details": {"values": ["500", "850", "1000"], "default": "500"},
            "name": "level",
            "label": "Pressure Level",
            "type": "StringListWidget",
        },
        {
            "details": {"values": ["24", "36", "48"], "default": "24"},
            "name": "step",
            "label": "Step",
            "type": "StringListWidget",
        },
        {
            "details": {"values": ["1", "2", "3"], "default": "1"},
            "name": "number",
            "label": "Number",
            "type": "StringChoiceWidget",
        },
    ]

    parsed_form: dict[str, set[Any]] = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"},
    }

    assert parsed_form == constraints.parse_form(form)
    assert {} == constraints.parse_form([])


def test_parse_selection() -> None:
    selections: list[dict[str, list[Any] | str]] = [
        {"number": "1"},
        {},  # 0
        {"number": ["1", "2"]},  # 1
        {"level": ["850"], "param": ["Z"]},  # 2
    ]

    parsed_selections: list[dict[str, set[Any]]] = [
        {"number": {"1"}},
        {},  # 0
        {"number": {"1", "2"}},  # 1
        {"level": {"850"}, "param": {"Z"}},  # 2
    ]
    for i in range(len(selections)):
        try:
            assert parsed_selections[i] == constraints.parse_selection(selections[i])
        except AssertionError:
            print(
                f"Iteration number {i} of " f"{test_parse_selection.__name__}() failed!"
            )
            raise AssertionError


def test_ensure_sequence() -> None:
    assert constraints.ensure_sequence([]) == []
    assert constraints.ensure_sequence(("1",)) == ("1",)
    assert constraints.ensure_sequence("1") == ["1"]


def test_validate_constraints() -> None:
    raw_form: list[dict[str, Any]] = [
        {
            "details": {
                "groups": [{"values": ["1"]}, {"values": ["2", "3"]}],
                "default": "A",
            },
            "name": "param1",
            "label": "Param1",
            "type": "StringListArrayWidget",
        },
        {
            "details": {"values": ["1", "2", "3"], "default": "1"},
            "name": "param2",
            "label": "Param2",
            "type": "StringListWidget",
        },
        {
            "details": {"values": ["1", "2", "3"], "default": "1"},
            "name": "param3",
            "label": "Param3",
            "type": "StringListWidget",
        },
        {
            "details": {"values": ["1", "2", "3"], "default": "1"},
            "name": "param4",
            "label": "Param4",
            "type": "UnsupportedWidget",
        },
    ]

    selections: dict[str, Any] = {
        "inputs": {"param1": "1", "param2": "1", "param4": "1"}
    }

    raw_constraints: list[dict[str, list[Any]]] = [
        {"param1": ["1"], "param2": ["1", "2", "3"], "param3": ["1", "2", "3"]},
        {"param1": ["2"], "param2": ["2"], "param3": ["2"]},
        {"param1": ["3"], "param2": ["3"], "param3": ["3"]},
    ]

    constraints.validate_constraints(raw_form, selections, raw_constraints)

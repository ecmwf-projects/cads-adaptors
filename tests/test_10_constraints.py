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


def test_parse_form() -> None:
    ogc_form: dict[str, Any] = {
        "param": {
            "schema_": {
                "type": "array",
                "items": {"enum": ["Z", "T"], "type": "string"},
            },
        },
        "level": {
            "schema_": {
                "type": "array",
                "items": {"enum": ["500", "850", "1000"], "type": "string"},
            },
        },
        "step": {
            "schema_": {
                "type": "array",
                "items": {"enum": ["24", "36", "48"], "type": "string"},
            },
        },
        "number": {
            "schema_": {
                "type": "array",
                "items": {"enum": ["1", "2", "3"], "type": "string"},
            },
        },
    }

    parsed_form: dict[str, set[Any]] = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
        "number": {"1", "2", "3"},
    }

    assert parsed_form == constraints.parse_form(ogc_form)
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

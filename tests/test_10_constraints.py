from typing import Any

import pytest
from datetimerange import DateTimeRange

from cads_adaptors import constraints


def test_get_possible_values() -> None:
    daterange_key = "date"
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

    form = {
        "date": {""},
        "city": {"rome", "paris", "london"},
        "level": {"1000", "850", "500"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
    }

    _constraints = [
        {
            "date": {"1990-01-01;1999-12-31", "2010-10-10;2011-11-11"},
            "city": {"rome", "paris", "london"},
            "level": {"500"},
            "param": {"Z", "T"},
            "step": {"24", "36", "48"},
        },
        {
            "date": {"1990-01-01;2011-12-31"},
            "city": {"paris", "london"},
            "level": {"1000"},
            "param": {"Z"},
            "step": {"24", "48"},
        },
        {
            "date": {"1980-01-01;2011-12-31"},
            "city": {"rome", "paris", "london"},
            "level": {"850"},
            "param": {"T"},
            "step": {"36", "48"},
        },
    ]

    selection = {"level": {"1000", "850"}, "date": {"1990-01-01;2011-12-31"}}

    assert constraints.get_possible_values(
        form, selection, _constraints, daterange_key=daterange_key
    ) == {
        "date": {"1980-01-01;2011-12-31", "1990-01-01;2011-12-31"},
        "city": {"london", "paris", "rome"},
        "level": {"1000", "850"},
        "param": {"T", "Z"},
        "step": {"24", "36", "48"},
    }

    selection = {
        "invalid_param": {"value"},
    }

    with pytest.raises(constraints.ParameterError):
        constraints.get_possible_values(
            form, selection, _constraints, daterange_key=daterange_key
        )

    selection = {"date": {"1600-01-01;1600-12-31"}}

    assert constraints.get_possible_values(
        form, selection, _constraints, daterange_key=daterange_key
    ) == {
        "date": set(),
        "city": set(),
        "level": set(),
        "param": set(),
        "step": set(),
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
    daterange_key = "date"

    assert constraints.get_form_state(form, {"level": {"500"}}, raw_constraints) == {
        "level": {"500", "850"},
        "param": {"Z"},
    }

    form = {
        "date": {""},
        "city": {"rome", "paris", "london"},
        "level": {"1000", "850", "500"},
        "param": {"Z", "T"},
        "step": {"24", "36", "48"},
    }

    _constraints = [
        {
            "date": {"1990-01-01;1999-12-31", "2010-10-10;2011-11-11"},
            "city": {"rome", "paris", "london"},
            "level": {"500"},
            "param": {"Z", "T"},
            "step": {"24", "36", "48"},
        },
        {
            "date": {"1990-01-01;2011-12-31"},
            "city": {"paris", "london"},
            "level": {"1000"},
            "param": {"Z"},
            "step": {"24", "48"},
        },
        {
            "date": {"1980-01-01;2011-12-31"},
            "city": {"rome", "paris", "london"},
            "level": {"850"},
            "param": {"T"},
            "step": {"36", "48"},
        },
    ]

    selection = {"level": {"1000", "850"}, "date": {"1990-01-01;2011-12-31"}}

    assert constraints.get_form_state(
        form, selection, _constraints, daterange_key=daterange_key
    ) == {
        "date": {"1980-01-01;2011-12-31", "1990-01-01;2011-12-31"},
        "city": {"london", "paris", "rome"},
        "level": {"1000", "850", "500"},
        "param": {"T", "Z"},
        "step": {"24", "36", "48"},
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
    out, daterange_key = constraints.parse_form(form)
    assert parsed_form == out

    out, daterange_key = constraints.parse_form([])
    assert {} == out


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


def test_temporal_intersection_between() -> None:
    tr1 = DateTimeRange("1990-01-01", "2000-01-01")
    tr2 = DateTimeRange("2010-01-01", "2020-01-01")
    tr3 = DateTimeRange("1995-01-01", "1996-01-01")
    tr4 = DateTimeRange("1995-01-01", "2005-01-01")

    assert constraints.temporal_intersection_between(tr1, [tr2]) is False
    assert constraints.temporal_intersection_between(tr1, [tr2]) is False
    assert constraints.temporal_intersection_between(tr1, [tr3]) is True
    assert constraints.temporal_intersection_between(tr1, [tr4]) is True


def test_gen_time_range_from_string() -> None:
    assert constraints.gen_time_range_from_string(
        "1990-01-01;1999-12-31"
    ) == DateTimeRange(start_datetime="1990-01-01", end_datetime="1999-12-31")
    assert constraints.gen_time_range_from_string(
        "2000-01-01;2000-01-01"
    ) == DateTimeRange(start_datetime="2000-01-01", end_datetime="2000-01-01")
    assert constraints.gen_time_range_from_string("2000-01-01") == DateTimeRange(
        start_datetime="2000-01-01", end_datetime="2000-01-01"
    )
    with pytest.raises(ValueError):
        constraints.gen_time_range_from_string("2000-01-01;1999-01-01")


def test_get_bounds() -> None:
    ranges = [
        "1980-01-01;1999-12-31",
        "1990-01-01;2011-12-31",
    ]
    assert constraints.get_bounds(ranges) == "1980-01-01/2011-12-31"

    ranges = [
        "1990-01-01;2011-12-31",
        "1980-01-01;2022-12-31",
    ]
    assert constraints.get_bounds(ranges) == "1980-01-01/2022-12-31"

    ranges = {"1990-01-01;2011-12-31", "1980-01-01;2000-12-31", "2000-01-01;2000-12-31"}
    assert constraints.get_bounds(ranges) == "1980-01-01/2011-12-31"

    assert constraints.get_bounds({"1980-01-01;1999-12-31"}) == "1980-01-01/1999-12-31"

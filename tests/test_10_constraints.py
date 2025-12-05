from typing import Any

import pytest

from cads_adaptors import constraints, exceptions


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


def is_a_match(d1:dict[str,list[Any]], d2:dict[str,list[Any]]):
    return d1.keys() == d2.keys() and all(set(d1[k]) == set(d2[k]) for k in d1.keys())

def test_apply_constraints_in_old_cds_fashion() -> None:
    form = {
        "param": {"lA", "lB", "lC", "D", "E"},
        "level": {"500", "850"}, 
        "number": {"1", "2", "3"}
    }

    raw_constraints = [
        {"param": {"lA", "lB"}, "level": {"500"}, "number": {"1","2"}},
        {"param": {"lC"}, "level": {"850"}, "number": {"1"}},
        {"param": {"D"}, "number": {"3"}},
        {"param": {"E"}, "number": {"1", "3"}}
    ]

    expected_answers_to_queries = [
        ({"level": {"500"}}, 
         {"param": ["lA", "lB", "D", "E"], "level": ["500", "850"], "number": ["1","2","3"]}),
        ({"level": {"850"}}, 
         {"param": ["lC", "D", "E"], "level": ["500", "850"], "number": ["1","3"]}),
        ({"number": {"1"}},
         {"param": ["lA", "lB", "lC", "E"], "level": ["500", "850"], "number": ["1","2","3"]}),
        ({"param": {"lC"}},
         {"param": ["lA","lB","lC", "D", "E"], "level": ["850"], "number": ["1"]}),
        ({"param": {"E"}},
         {"param": ["lA","lB","lC", "D", "E"], "level": [], "number": ["1", "3"]}),
        ({"param": {"lA","E"}},
         {"param": ["lA","lB","lC", "D", "E"], "level": ["500"], "number": ["1", "2", "3"]}),
        ({"param": {"lA", "E"}, "number": {"3"}},
         {"param": ["D", "E"], "level": [], "number": ["1","2","3"]}),
        ({"param": {"lA", "E"}, "level": {"500"}},
         {"param": ["lA","lB", "D", "E"], "level": ["500"], "number": ["1", "2", "3"]}),
        ({"param": {"lA", "E"}, "level": {"500"}, "number": {"3"}},
         {"param": ["D", "E"], "level": [], "number": ["1", "2", "3"]}),
    ]

    for query,expected_answer in expected_answers_to_queries:
        answer = constraints.apply_constraints_in_old_cds_fashion(form, query, raw_constraints)
        assert is_a_match(answer,expected_answer)


def test_apply_constraints_in_old_cds_fashion_for_dateranges() -> None:
    form = {
        "param": {"lA", "lB", "lC", "D", "E"},
        "level": {"500", "850"}, 
        "date": {"2000-01-01/2025-12-01"}
    }

    raw_constraints = [
        {"param": {"lA", "lB"}, "level": {"500"}, "date": {"2000-01-01/2020-01-01","2023-01-01/2023-07-01"}},
        {"param": {"lC"}, "level": {"850"}, "date": {"2023-07-01/2024-12-31"}},
        {"param": {"D"}, "date": {"2000-01-01/2024-12-31"}},
        {"param": {"E"}, "date": {"2000-01-01/2025-12-01"}}
    ]

    expected_answers_to_queries = [
        ({"level": {"500"}}, 
         {"param": ["lA", "lB", "D", "E"], "level": ["500", "850"], "date": ["2000-01-01/2020-01-01","2023-01-01/2023-07-01","2000-01-01/2024-12-31","2000-01-01/2025-12-01"]}),
        ({"date": {"2010-01-01/2011-12-31"}},
         {"param": ["lA", "lB", "D", "E"], "level": ["500"], "date": ["2000-01-01/2025-12-01"]}),
        ({"date": {"2021-01-01/2022-01-01"}},
         {"param": ["D", "E"], "level": [], "date": ["2000-01-01/2025-12-01"]}),
        ({"date": {"2023-07-01"}},
         {"param": ["lA", "lB", "lC", "D", "E"], "level": ["500", "850"], "date": ["2000-01-01/2025-12-01"]}),
        ({"level": {"850"}, "date": {"2023-07-01"}},
         {"param": ["lC", "D", "E"], "level": ["500", "850"], "date": ["2000-01-01/2024-12-31", "2000-01-01/2025-12-01", "2023-07-01/2024-12-31"]}),
        ({"param": {"lA"}, "date": {"2019-07-01"}},
         {"param": ["lA", "lB", "D", "E"], "level": ["500"], "date": ["2000-01-01/2020-01-01", "2023-01-01/2023-07-01"]}),
    ]

    widget_types = {"date": "DateRangeWidget"}

    for query,expected_answer in expected_answers_to_queries:
        answer = constraints.apply_constraints_in_old_cds_fashion(form, query, raw_constraints, widget_types)
        assert is_a_match(answer,expected_answer)


def test_apply_constraints() -> None:
    form = {"level": {"500", "850"}, "param": {"Z", "T"}, "number": {"1"}}

    raw_constraints = [
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert constraints.apply_constraints(form, {"level": {"500"}}, raw_constraints)[
        "number"
    ] == ["1"]


@pytest.mark.parametrize(
    "selections",
    (
        {"foo": {"500"}},
        {"foo": {"500"}, "level": {"500"}},
    ),
)
def test_apply_constraints_errors(selections: dict[str, set[Any]]) -> None:
    form = {"level": {"500", "850"}, "param": {"Z", "T"}, "number": {"1"}}

    raw_constraints = [
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]
    with pytest.raises(exceptions.ParameterError, match="invalid param 'foo'"):
        constraints.apply_constraints(form, selections, raw_constraints)


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
            print(f"Iteration number {i} of {test_parse_selection.__name__}() failed!")
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

    selections: dict[str, Any] = {"param1": "1", "param2": "1", "param4": "1"}

    raw_constraints: list[dict[str, list[Any]]] = [
        {"param1": ["1"], "param2": ["1", "2", "3"], "param3": ["1", "2", "3"]},
        {"param1": ["2"], "param2": ["2"], "param3": ["2"]},
        {"param1": ["3"], "param2": ["3"], "param3": ["3"]},
    ]

    constraints.validate_constraints(raw_form, selections, raw_constraints)


def test_legacy_intersect_constraints():
    raw_constraints = [
        {
            "experiment": ["rcp_4_5"],
            "gcm": ["ec_earth"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": [
                "wind_speed_at_10m",
                "surface_downwelling_shortwave_radiation",
                "2m_air_temperature",
            ],
        },
        {
            "experiment": ["rcp_4_5"],
            "gcm": ["hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["wind_speed_at_10m", "2m_air_temperature"],
        },
        {
            "experiment": ["rcp_8_5"],
            "gcm": ["ec_earth", "hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": [
                "wind_speed_at_10m",
                "surface_downwelling_shortwave_radiation",
                "2m_air_temperature",
            ],
        },
        {
            "energy_product_type": ["capacity_factor_ratio"],
            "experiment": ["rcp_4_5", "rcp_8_5"],
            "gcm": ["ec_earth", "hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["wind_power_generation_onshore"],
        },
        {
            "energy_product_type": ["energy", "power"],
            "experiment": ["rcp_4_5"],
            "gcm": ["mpi_esm_lr"],
            "rcm": ["cclm4_8_17"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["electricity_demand"],
        },
        {
            "energy_product_type": ["energy", "power"],
            "experiment": ["rcp_4_5", "rcp_8_5"],
            "gcm": ["ec_earth", "hadgem2_es"],
            "rcm": ["racmo22e"],
            "temporal_aggregation": ["monthly", "seasonal"],
            "variable": ["electricity_demand"],
        },
    ]
    request = {
        "variable": ["surface_downwelling_shortwave_radiation", "electricity_demand"],
        "spatial_aggregation": "country_level",
        "temporal_aggregation": ["monthly"],
        "energy_product_type": ["energy"],
        "experiment": ["rcp_8_5"],
        "rcm": ["racmo22e"],
        "gcm": ["hadgem2_es"],
    }
    expected = [
        {
            "variable": ["surface_downwelling_shortwave_radiation"],
            "spatial_aggregation": "country_level",
            "temporal_aggregation": ["monthly"],
            "experiment": ["rcp_8_5"],
            "rcm": ["racmo22e"],
            "gcm": ["hadgem2_es"],
        },
        {
            "variable": ["electricity_demand"],
            "spatial_aggregation": "country_level",
            "temporal_aggregation": ["monthly"],
            "energy_product_type": ["energy"],
            "experiment": ["rcp_8_5"],
            "rcm": ["racmo22e"],
            "gcm": ["hadgem2_es"],
        },
    ]
    actual = constraints.legacy_intersect_constraints(request, raw_constraints)
    assert actual == expected


def test_legacy_intersect_empty_constraints():
    raw_constraints = []
    request = {"foo": "bar"}
    actual = constraints.legacy_intersect_constraints(request, raw_constraints)
    assert actual == [{"foo": "bar"}]


def test_legacy_intersect_dtype_differences():
    raw_constraints = [{"foo": ["1", "2"], "bar": "3"}]
    request = {"foo": 1, "bar": [3, 4]}
    actual = constraints.legacy_intersect_constraints(request, raw_constraints)
    assert actual == [{"foo": [1], "bar": [3]}]

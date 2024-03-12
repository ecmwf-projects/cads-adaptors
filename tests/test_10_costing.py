from typing import Any

from cads_adaptors import costing


def test_compute_combinations() -> None:
    assert costing.compute_combinations(dict()) == []

    result = costing.compute_combinations({"param1": {"1", "2"}})
    expected = [{"param1": "2"}, {"param1": "1"}]
    assert len(result) == len(expected) and all(
        combination in expected for combination in result
    )

    result = costing.compute_combinations({"param1": {"1", "2"}, "param2": {"a", "b"}})
    expected = [
        {"param1": "1", "param2": "b"},
        {"param1": "1", "param2": "a"},
        {"param1": "2", "param2": "b"},
        {"param1": "2", "param2": "a"},
    ]
    assert len(result) == len(expected) and all(
        combination in expected for combination in result
    )


def test_remove_duplicates() -> None:
    result = costing.remove_duplicates(
        [{"level": {"500"}, "param": {"Z", "T"}}, {"level": {"500"}, "param": {"Z"}}]
    )
    expected = [{"level": "500", "param": "Z"}, {"level": "500", "param": "T"}]
    assert len(result) == len(expected) and all(
        combination in expected for combination in result
    )


def test_estimate_granules_basic() -> None:
    form_key_values = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert (
        costing.estimate_granules(
            form_key_values, {"param": {"Z", "T"}, "level": {"500"}}, constraints
        )
        == 2
    )
    assert (
        costing.estimate_granules(
            form_key_values, {"param": {"Z", "T"}, "level": {"500", "850"}}, constraints
        )
        == 3
    )


def test_estimate_granules_safe() -> None:
    form_key_values = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"500"}, "param": {"Z"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T"}, "level": {"500"}},
            constraints,
            safe=False,
        )
        == 3
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T"}, "level": {"500"}},
            constraints,
            safe=True,
        )
        == 2
    )


def test_estimate_granules_long() -> None:
    form_key_values = {
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"daily_mean", "hourly"},
    }

    constraints = [
        {"param": {"Z"}, "time": {"12:00", "00:00"}, "stat": {"hourly"}},
        {"param": {"Z"}, "stat": {"daily_mean"}},
    ]

    assert (
        costing.estimate_granules(
            form_key_values, {"param": {"Z"}, "stat": {"daily_mean"}}, constraints
        )
        == 1
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z"}, "time": {"12:00", "00:00"}, "stat": {"hourly"}},
            constraints,
        )
        == 2
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z"}, "time": {"12:00", "00:00"}, "stat": {"daily_mean"}},
            constraints,
        )
        == 1
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {},
            constraints,
        )
        == 0
    )

    form_key_values = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
        "number": {"1", "2", "3"},
        "model": {"a", "b"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"850"}, "param": {"T"}},
    ]

    assert (
        costing.estimate_granules(
            form_key_values,
            {
                "param": {"Z"},
                "level": {"500"},
                "number": {"1", "2"},
                "model": {"a", "b"},
            },
            constraints,
        )
        == 4
    )

    form_key_values = {
        "level": {"500", "850"},
        "param": {"Z", "T"},
    }

    constraints = [
        {"level": {"500"}, "param": {"Z", "T"}},
        {"level": {"850"}, "param": {"T"}},
        {"level": {"500"}, "param": {"T"}},
    ]

    selection = {"param": {"Z", "T"}, "level": {"500"}}

    assert (
        costing.estimate_granules(form_key_values, selection, constraints, safe=True)
        == 2
    )

    form_key_values = {
        "type": {"projection", "historical"},
        "ensemble": {"1", "2"},
        "time": {"00:00", "12:00"},
        "stat": {"hourly", "daily_mean"},
    }

    constraints = [
        {
            "type": {"projection"},
            "ensemble": {"1", "2"},
            "time": {"00:00", "12:00"},
            "stat": {"hourly"},
        },
        {"type": {"projection"}, "ensemble": {"1"}, "stat": {"daily_mean"}},
        {"type": {"historical"}, "time": {"00:00", "12:00"}, "stat": {"hourly"}},
        {"type": {"historical"}, "stat": {"daily_mean"}},
    ]

    selection = {
        "type": {"projection", "historical"},
        "ensemble": {"1"},
        "time": {"00:00"},
        "stat": {"hourly", "daily_mean"},
    }

    assert costing.estimate_granules(form_key_values, selection, constraints) == 4

    form_key_values = {
        "level": {"500", "850"},
        "time": {"12:00", "00:00"},
        "param": {"Z", "T"},
        "stat": {"daily_mean", "hourly"},
        "number": {"1", "2", "3"},
        "model": {"a", "b", "c"},
    }

    selection = {
        "param": {"Z", "T"},
        "level": {"500", "850"},
        "stat": {"daily_mean", "hourly"},
        "time": {"12:00", "00:00"},
        "number": {"1", "2"},
        "model": {"a", "b"},
    }

    constraints = [
        {
            "level": {"500"},
            "param": {"Z", "T"},
            "time": {"12:00", "00:00"},
            "stat": {"hourly"},
        },
        {"level": {"850"}, "param": {"T"}, "time": {"12:00"}, "stat": {"hourly"}},
        {"level": {"500"}, "param": {"Z", "T"}, "stat": {"daily_mean"}},
        {"level": {"850"}, "param": {"T"}, "stat": {"daily_mean"}},
    ]

    assert costing.estimate_granules(form_key_values, selection, constraints) == 32


def test_estimate_granules_weighted_keys() -> None:
    form_key_values = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T", "Q"},
    }

    weighted_keys = {"param": 2}

    assert (
        costing.estimate_granules(
            form_key_values,
            {},
            [],
            weighted_keys=weighted_keys,
        )
        == 0
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T", "Q"}, "level": {"500"}},
            [],
            weighted_keys=weighted_keys,
        )
        == 6
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z"}, "level": {"500", "850", "1000"}},
            [],
            weighted_keys=weighted_keys,
        )
        == 6
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T", "Q"}, "level": {"500", "850", "1000"}},
            [],
            weighted_keys=weighted_keys,
        )
        == 18
    )


def test_estimate_granules_weighted_values() -> None:
    form_key_values = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T", "Q"},
    }

    constraints = [form_key_values]

    weighted_values = {"param": {"Q": 2}}

    assert (
        costing.estimate_granules(
            form_key_values,
            {},
            constraints,
            weighted_values=weighted_values,
        )
        == 0
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T"}, "level": {"500"}},
            constraints,
            weighted_values=weighted_values,
        )
        == 2
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "Q"}, "level": {"500"}},
            constraints,
            weighted_values=weighted_values,
        )
        == 3
    )


def test_estimate_granules_weighted_keys_and_values() -> None:
    form_key_values = {
        "level": {"500", "850", "1000"},
        "param": {"Z", "T", "Q"},
    }

    constraints = [form_key_values]

    weighted_keys = {"param": 2}
    weighted_values = {"param": {"Q": 2}}

    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T"}, "level": {"500"}},
            constraints,
            weighted_values=weighted_values,
            weighted_keys=weighted_keys,
        )
        == 4
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "Q"}, "level": {"500"}},
            constraints,
            weighted_values=weighted_values,
            weighted_keys=weighted_keys,
        )
        == 6
    )

    # Check for empty selection
    assert (
        costing.estimate_granules(
            form_key_values,
            {},
            constraints,
            weighted_values=weighted_values,
            weighted_keys=weighted_keys,
        )
        == 0
    )


def test_estimate_request_size() -> None:
    form = [
        {
            "name": "param",
            "label": "Param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        }
    ]
    constraints = [{"param": {"Z", "T"}}]

    assert costing.estimate_size(form, {"param": {"Z", "T"}}, constraints) == 2
    assert costing.estimate_size(form, {"param": {"Z"}}, constraints) == 1


def test_get_excluded_keys() -> None:
    test_form = [
        {"name": "var1", "type": "AllowedType"},
        {"name": "var2", "type": "GeographicExtentWidget"},
    ]
    exp_excluded_keys = ["var2"]
    excluded_keys = costing.get_excluded_keys(test_form)
    assert excluded_keys == exp_excluded_keys

    exp_excluded_keys = []
    excluded_keys = costing.get_excluded_keys(None)
    assert excluded_keys == exp_excluded_keys

    test_form = {"name": "var2", "type": "GeographicExtentWidget"}  # type: ignore
    exp_excluded_keys = ["var2"]
    excluded_keys = costing.get_excluded_keys(test_form)
    assert excluded_keys == exp_excluded_keys


def test_estimate_costs() -> None:
    from cads_adaptors import DummyCdsAdaptor

    form = [
        {
            "name": "param",
            "label": "Param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        }
    ]
    adaptor = DummyCdsAdaptor(form, constraints=[{"param": {"Z", "T"}}])

    # Test empty selection
    request: dict[str, Any] = {"inputs": dict()}
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 1
    assert costs["number_of_fields"] == 1

    request = {"inputs": {"param": {"Z", "T"}}}
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 2
    assert costs["number_of_fields"] == 2


def test_estimate_costs_2() -> None:
    from cads_adaptors import DummyCdsAdaptor

    form: list[dict[str, Any]] = [
        {
            "name": "variable",
            "label": "Variable",
            "help": "Please, consult the product.",
            "required": True,
            "css": "todo",
            "type": "StringChoiceWidget",
            "details": {
                "values": [
                    "average_temperature",
                    "maximum_temperature",
                    "minimum_temperature",
                ],
                "columns": 3,
                "labels": {
                    "average_temperature": "Average temperature",
                    "maximum_temperature": "Maximum temperature",
                    "minimum_temperature": "Minimum temperature",
                },
            },
            "id": 0,
        },
        {
            "details": {
                "accordionGroups": True,
                "accordionOptions": {
                    "openGroups": ["Temperature", "Lakes"],
                    "searchable": False,
                },
                "displayaslist": False,
                "groups": [
                    {
                        "columns": 2,
                        "label": "Single level",
                        "labels": {
                            "10m_u_component_of_wind": "10m u-component of wind",
                            "10m_v_component_of_wind": "10m v-component of wind",
                            "2m_dewpoint_temperature": "2m dewpoint temperature",
                            "2m_temperature": "2m temperature",
                        },
                        "values": [
                            "10m_u_component_of_wind",
                            "10m_v_component_of_wind",
                            "2m_dewpoint_temperature",
                            "2m_temperature",
                        ],
                    },
                    {
                        "label": "Slow access",
                        "details": {
                            "accordionGroups": True,
                            "accordionOptions": {
                                "openGroups": [
                                    "Single-level chemical vertical integrals"
                                ],
                                "searchable": False,
                            },
                            "displayaslist": False,
                            "id": 2,
                            "groups": [
                                {
                                    "columns": 2,
                                    "label": "Single-level chemical vertical integrals",
                                    "labels": {
                                        "total_column_acetone": "Total column acetone",
                                        "total_column_acetone_product": "Total column acetone product",
                                        "total_column_aldehydes": "Total column aldehydes",
                                        "total_column_amine": "Total column amine",
                                        "total_column_ammonia": "Total column ammonia",
                                    },
                                    "values": [
                                        "total_column_acetone",
                                        "total_column_acetone_product",
                                        "total_column_aldehydes",
                                        "total_column_amine",
                                        "total_column_ammonia",
                                    ],
                                },
                                {
                                    "columns": 2,
                                    "label": "Single-level meteorological",
                                    "labels": {
                                        "boundary_layer_height": "Boundary layer height",
                                        "cloud_base_height": "Cloud base height",
                                        "convective_inhibition": "Convective inhibition",
                                    },
                                    "values": [
                                        "boundary_layer_height",
                                        "cloud_base_height",
                                        "convective_inhibition",
                                    ],
                                },
                            ],
                        },
                    },
                ],
                "id": 1,
            },
            "help": None,
            "label": "Nested Variable",
            "name": "nested_variable",
            "required": True,
            "type": "StringListArrayWidget",
        },
        {
            "name": "statistic",
            "label": "Statistic",
            "help": "Statistsics are computed for the period  1971 to 2099.",
            "required": True,
            "css": "todo",
            "type": "StringListArrayWidget",
            "details": {
                "groups": [
                    {
                        "label": "Mean and median",
                        "values": ["time_average", "50th_percentile"],
                        "labels": {
                            "time_average": "Time average",
                            "50th_percentile": "50th percentile",
                        },
                        "columns": 3,
                    },
                    {
                        "label": "Percentiles",
                        "details": {
                            "displayaslist": False,
                            "accordionGroups": True,
                            "accordionOptions": {"searchable": False, "openGroups": []},
                            "id": 10,
                            "groups": [
                                {
                                    "label": "Lower tercile",
                                    "columns": 3,
                                    "values": [
                                        "10th_percentile",
                                        "25th_percentile",
                                        "1st_percentile",
                                        "5th_percentile",
                                    ],
                                    "labels": {
                                        "10th_percentile": "10th percentile",
                                        "25th_percentile": "25th percentile",
                                        "1st_percentile": "1st percentile",
                                        "5th_percentile": "5th percentile",
                                    },
                                },
                                {
                                    "label": "Upper tercile",
                                    "columns": 3,
                                    "values": [
                                        "75th_percentile",
                                        "90th_percentile",
                                        "95th_percentile",
                                        "97th_percentile",
                                        "99th_percentile",
                                    ],
                                    "labels": {
                                        "75th_percentile": "75th percentile",
                                        "90th_percentile": "90th percentile",
                                        "95th_percentile": "95th percentile",
                                        "97th_percentile": "97th percentile",
                                        "99th_percentile": "99th percentile",
                                    },
                                },
                            ],
                        },
                    },
                ],
                "displayaslist": False,
                "accordionGroups": True,
                "accordionOptions": {
                    "searchable": False,
                    "openGroups": ["Mean and median", "Percentiles"],
                },
            },
            "id": 2,
        },
        {
            "name": "experiment",
            "label": "Experiment",
            "help": "Each experiment is a simulation of the climate system under specific hypothesis.",
            "required": True,
            "css": "todo",
            "type": "StringListWidget",
            "details": {
                "values": ["rcp4_5", "rcp8_5"],
                "columns": 3,
                "labels": {"rcp4_5": "RCP4.5", "rcp8_5": "RCP8.5"},
            },
            "id": 3,
        },
        {
            "name": "ensemble_statistic",
            "label": "Ensemble_statistic",
            "help": None,
            "required": True,
            "css": "todo",
            "type": "StringListWidget",
            "details": {
                "values": [
                    "ensemble_members_average",
                    "ensemble_members_standard_deviation",
                ],
                "columns": 3,
                "labels": {
                    "ensemble_members_average": "Ensemble members average",
                    "ensemble_members_standard_deviation": "Ensemble members standard deviation",
                },
            },
            "id": 4,
        },
        {
            "name": "text_widget_example",
            "type": "FreeEditionWidget",
            "label": "Text widget example",
            "details": {
                "accordion": False,
                "default-open": True,
                "text": "This is a `FreeEditionWidget` widget.",
            },
            "id": 6,
        },
        {
            "type": "GeographicExtentWidget",
            "label": "Area",
            "name": "area",
            "help": None,
            "details": {
                "precision": 2,
                "extentLabels": {"n": "North", "e": "East", "s": "South", "w": "West"},
                "range": {"n": 90, "e": 180, "s": -90, "w": -180},
                "default": {"n": 90, "e": 180, "s": -90, "w": -180},
            },
        },
        {
            "name": "global",
            "type": "FreeEditionWidget",
            "label": "Whole world",
            "details": {
                "accordion": False,
                "default-open": True,
                "text": "Select whole world",
            },
        },
        {
            "type": "ExclusiveGroupWidget",
            "label": "Geographical area",
            "help": "Lorem ipsum dolor",
            "name": "area_group",
            "children": ["global", "area"],
            "details": {"default": "global"},
        },
    ]
    # constraints = []
    adaptor = DummyCdsAdaptor(form, constraints=[])

    request: dict[str, Any] = {
        "inputs": {
            "variable": "maximum_temperature",
            "freeform": [""],
            "latitude": ["2"],
            "date_range": ["2023-10-12/2023-10-24"],
            "location[0]": ["0"],
            "location[1]": ["0"],
        }
    }

    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 1
    assert costs["number_of_fields"] == 1

    request = {
        "inputs": {
            "variable": "maximum_temperature",
            "nested_variable": [
                "2m_temperature",
                "ammonium_aerosol_optical_depth_550nm",
            ],
            "freeform": [""],
            "latitude": ["2"],
            "date_range": ["2023-10-12/2023-10-24"],
            "location[0]": ["0"],
            "location[1]": ["0"],
        }
    }

    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 2
    assert costs["number_of_fields"] == 2

    costing_kwargs = {
        "weighted_keys": {"variable": 2},
        "weighted_values": {
            "nested_variable": {
                "total_column_acetone": 2,
                "total_column_acetone_product": 2,
                "total_column_aldehydes": 2,
                "total_column_amine": 2,
                "total_column_ammonia": 2,
            }
        },
    }

    weighted_adaptor = DummyCdsAdaptor(
        form, constraints=[], costing={"costing_kwargs": costing_kwargs}
    )
    costs = weighted_adaptor.estimate_costs(request)
    assert costs["size"] == 4
    assert costs["number_of_fields"] == 2

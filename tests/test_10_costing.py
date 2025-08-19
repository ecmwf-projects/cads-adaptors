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

    assert costing.estimate_precise_size(form, {"param": {"Z", "T"}}, constraints) == 2
    assert costing.estimate_precise_size(form, {"param": {"Z"}}, constraints) == 1


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


def test_estimate_number_of_fields() -> None:
    form = [
        {
            "name": "key1",
            "label": "Key1",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
        {
            "name": "key2",
            "label": "Key2",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
        {
            "name": "key3",
            "label": "Key3",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
    ]

    costing_kwargs = {
        "weighted_keys": {
            "key2": 2,
        },
        "weighted_values": {
            "key1": {
                "value1": 2,
            },
            "key2": {
                "value2": 2,
            },
        },
    }

    request = {
        "key1": ["value1", "value2"],
    }
    number_of_fields = costing.estimate_number_of_fields(form, request)
    assert number_of_fields == 2
    number_of_fields = costing.estimate_number_of_fields(
        form, request, **costing_kwargs
    )
    assert number_of_fields == 3

    request = {
        "key2": ["value1"],
    }
    number_of_fields = costing.estimate_number_of_fields(
        form, request, **costing_kwargs
    )
    assert number_of_fields == 2
    request = {
        "key2": ["value1", "value2"],
    }
    number_of_fields = costing.estimate_number_of_fields(
        form, request, **costing_kwargs
    )
    assert number_of_fields == 6


def test_estimate_number_of_fields_ignore_keys() -> None:
    form = [
        {
            "name": "key1",
            "label": "Key1",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
        {
            "name": "key2",
            "label": "Key2",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
        {
            "name": "key3",
            "label": "Key3",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
    ]

    costing_kwargs = {
        "ignore_keys": ["key3"],
    }

    request = {
        "key1": ["value1", "value2"],
        "key2": ["value1", "value2"],
    }
    number_of_fields = costing.estimate_number_of_fields(form, request)
    assert number_of_fields == 4
    number_of_fields = costing.estimate_number_of_fields(
        form, request, **costing_kwargs
    )
    assert number_of_fields == 4

    request = {
        "key1": ["value1", "value2"],
        "key2": ["value1", "value2"],
        "key3": ["value1", "value2"],
    }
    number_of_fields = costing.estimate_number_of_fields(
        form,
        request,
    )
    assert number_of_fields == 8
    number_of_fields = costing.estimate_number_of_fields(
        form, request, **costing_kwargs
    )
    assert number_of_fields == 4


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
    adaptor = DummyCdsAdaptor(
        form,
        constraints=[{"param": {"Z", "T"}}],
        costing={"max_costs": {"size": 10, "precise_size": 10}},
    )

    # Test empty selection
    request: dict[str, Any] = dict()
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 1
    assert costs["precise_size"] == 1

    request = {"param": {"Z", "T"}}
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 2
    assert costs["precise_size"] == 2


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
    adaptor = DummyCdsAdaptor(
        form,
        constraints=[],
        costing={"max_costs": {"size": 10, "precise_size": 10}},
    )

    request: dict[str, Any] = {
        "variable": "maximum_temperature",
        "freeform": [""],
        "latitude": ["2"],
        "date_range": ["2023-10-12/2023-10-24"],
        "location[0]": ["0"],
        "location[1]": ["0"],
    }

    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 1
    assert costs["precise_size"] == 1

    request = {
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

    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 2
    assert costs["precise_size"] == 2

    costing_kwargs = {
        "weighted_keys": {"variable": 2},
        "weighted_values": {
            "nested_variable": {
                "total_column_acetone_product": 2,
                "ammonium_aerosol_optical_depth_550nm": 3,
            }
        },
    }

    weighted_adaptor = DummyCdsAdaptor(
        form,
        constraints=[],
        costing={
            "costing_kwargs": costing_kwargs,
            "max_costs": {"precise_size": 10, "size": 10},
        },
    )
    costs = weighted_adaptor.estimate_costs(request)
    assert costs["precise_size"] == 8
    assert costs["size"] == 8

    request = {
        "variable": "maximum_temperature",
        "nested_variable": [
            "total_column_acetone_product",
            "ammonium_aerosol_optical_depth_550nm",
        ],
        "freeform": [""],
        "latitude": ["2"],
        "date_range": ["2023-10-12/2023-10-24"],
        "location[0]": ["0"],
        "location[1]": ["0"],
    }
    costs = weighted_adaptor.estimate_costs(request)
    assert costs["precise_size"] == 10
    assert costs["size"] == 10


def test_estimate_costs_with_mapping() -> None:
    from cads_adaptors import DummyCdsAdaptor

    form = [
        {
            "name": "key1",
            "label": "Key1",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
        {
            "name": "key2",
            "label": "Key2",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
        {
            "name": "key3",
            "label": "Key3",
            "details": {"values": {"value1", "value2"}},
            "type": "StringListWidget",
        },
    ]

    costing_kwargs = {
        "weighted_keys": {"key1": 2, "key2": 2, "key3": 2},
        "weighted_values": {
            "key1": {
                "value1": 2,
            },
            "key2": {
                "value1": 2,
            },
            "key3": {
                "value1": 2,
            },
        },
    }
    weighted_adaptor = DummyCdsAdaptor(
        form,
        constraints=[],
        costing={
            "costing_kwargs": costing_kwargs,
            "max_costs": {"precise_size": 10, "size": 10},
        },
        mapping={
            "rename": {"key1": "renamed_key1"},
            "remap": {
                "key1": {"value1": "renamed_value1"},
                "key2": {"value1": "renamed_value1"},
            },
        },
    )

    request1 = {
        "key1": ["value1", "value2"],
    }
    request2 = {
        "key2": ["value1", "value2"],
    }
    request3 = {
        "key3": ["value1", "value2"],
    }
    costs1 = weighted_adaptor.estimate_costs(request1)
    costs2 = weighted_adaptor.estimate_costs(request2)
    costs3 = weighted_adaptor.estimate_costs(request3)
    assert costs1["size"] == costs2["size"]
    assert costs2["size"] == costs3["size"]


def test_costing_classes() -> None:
    from cads_adaptors import DummyCdsAdaptor

    form: list[dict[str, Any]] = [
        {
            "name": "variable",
            "label": "Variable",
            "help": None,
            "required": True,
            "css": "todo",
            "type": "StringListWidget",
            "details": {
                "values": [
                    "altitude_of_plume_bottom",
                    "altitude_of_plume_top",
                    "injection_height",
                    "mean_altitude_of_maximum_injection",
                    "wildfire_combustion_rate",
                    "wildfire_flux_of_acetaldehyde",
                    "wildfire_flux_of_acetone",
                    "wildfire_flux_of_ammonia",
                    "wildfire_flux_of_benzene",
                    "wildfire_flux_of_black_carbon",
                    "wildfire_flux_of_butanes",
                    "wildfire_flux_of_butenes",
                    "wildfire_flux_of_carbon_dioxide",
                    "wildfire_flux_of_carbon_monoxide",
                    "wildfire_flux_of_dimethyl_sulfide",
                    "wildfire_flux_of_ethane",
                    "wildfire_flux_of_ethanol",
                    "wildfire_flux_of_ethene",
                    "wildfire_flux_of_formaldehyde",
                    "wildfire_flux_of_heptane",
                    "wildfire_flux_of_hexanes",
                    "wildfire_flux_of_hexene",
                    "wildfire_flux_of_higher_alkanes",
                    "wildfire_flux_of_higher_alkenes",
                    "wildfire_flux_of_hydrogen",
                    "wildfire_flux_of_isoprene",
                    "wildfire_flux_of_methane",
                    "wildfire_flux_of_methanol",
                    "wildfire_flux_of_nitrogen_oxides",
                    "wildfire_flux_of_nitrous_oxide",
                    "wildfire_flux_of_non_methane_hydrocarbons",
                    "wildfire_flux_of_octene",
                    "wildfire_flux_of_organic_carbon",
                    "wildfire_flux_of_pentanes",
                    "wildfire_flux_of_pentenes",
                    "wildfire_flux_of_propane",
                    "wildfire_flux_of_propene",
                    "wildfire_flux_of_sulphur_dioxide",
                    "wildfire_flux_of_terpenes",
                    "wildfire_flux_of_toluene",
                    "wildfire_flux_of_toluene_lump",
                    "wildfire_flux_of_total_carbon_in_aerosols",
                    "wildfire_flux_of_total_particulate_matter",
                    "wildfire_flux_of_xylene",
                    "wildfire_fraction_of_area_observed",
                    "wildfire_overall_flux_of_burnt_carbon",
                    "wildfire_radiative_power",
                ],
                "columns": 2,
                "labels": {
                    "altitude_of_plume_bottom": "Altitude of plume bottom",
                    "altitude_of_plume_top": "Altitude of plume top",
                    "injection_height": "Injection height (from IS4FIRES)",
                    "mean_altitude_of_maximum_injection": "Mean altitude of maximum injection",
                    "wildfire_combustion_rate": "Wildfire combustion rate",
                    "wildfire_flux_of_acetaldehyde": "Wildfire flux of acetaldehyde (C2H4O)",
                    "wildfire_flux_of_acetone": "Wildfire flux of acetone (C3H6O)",
                    "wildfire_flux_of_ammonia": "Wildfire flux of ammonia (NH3)",
                    "wildfire_flux_of_benzene": "Wildfire flux of benzene (C6H6)",
                    "wildfire_flux_of_black_carbon": "Wildfire flux of black carbon",
                    "wildfire_flux_of_butanes": "Wildfire flux of butanes (C4H10)",
                    "wildfire_flux_of_butenes": "Wildfire flux of butenes (C4H8)",
                    "wildfire_flux_of_carbon_dioxide": "Wildfire flux of carbon dioxide (CO2)",
                    "wildfire_flux_of_carbon_monoxide": "Wildfire flux of carbon monoxide (CO)",
                    "wildfire_flux_of_dimethyl_sulfide": "Wildfire flux of dimethyl sulfide (DMS) (C2H6S)",
                    "wildfire_flux_of_ethane": "Wildfire flux of ethane (C2H6)",
                    "wildfire_flux_of_ethanol": "Wildfire flux of ethanol (C2H5OH)",
                    "wildfire_flux_of_ethene": "Wildfire flux of ethene (C2H4)",
                    "wildfire_flux_of_formaldehyde": "Wildfire flux of formaldehyde (CH2O)",
                    "wildfire_flux_of_heptane": "Wildfire flux of heptane (C7H16)",
                    "wildfire_flux_of_hexanes": "Wildfire flux of hexanes (C6H14)",
                    "wildfire_flux_of_hexene": "Wildfire flux of hexene (C6H12)",
                    "wildfire_flux_of_higher_alkanes": "Wildfire flux of higher alkanes (CnH2n+2, c>=4)",
                    "wildfire_flux_of_higher_alkenes": "Wildfire flux of higher alkenes (CnH2n, c>=4)",
                    "wildfire_flux_of_hydrogen": "Wildfire flux of hydrogen (H)",
                    "wildfire_flux_of_isoprene": "Wildfire flux of isoprene (C5H8)",
                    "wildfire_flux_of_methane": "Wildfire flux of methane (CH4)",
                    "wildfire_flux_of_methanol": "Wildfire flux of methanol (CH3OH)",
                    "wildfire_flux_of_nitrogen_oxides": "Wildfire flux of nitrogen oxides (NOx)",
                    "wildfire_flux_of_nitrous_oxide": "Wildfire flux of nitrous oxide (N20)",
                    "wildfire_flux_of_non_methane_hydrocarbons": "Wildfire flux of non-methane hydrocarbons",
                    "wildfire_flux_of_octene": "Wildfire flux of octene (C8H16)",
                    "wildfire_flux_of_organic_carbon": "Wildfire flux of organic carbon",
                    "wildfire_flux_of_pentanes": "Wildfire flux of pentanes (C5H12)",
                    "wildfire_flux_of_pentenes": "Wildfire flux of pentenes (C5H10)",
                    "wildfire_flux_of_propane": "Wildfire flux of propane (C3H8)",
                    "wildfire_flux_of_propene": "Wildfire flux of propene (C3H6)",
                    "wildfire_flux_of_sulphur_dioxide": "Wildfire flux of sulphur dioxide (SO2)",
                    "wildfire_flux_of_terpenes": "Wildfire flux of terpenes ((C5H8)n)",
                    "wildfire_flux_of_toluene": "Wildfire flux of toluene (C7H8)",
                    "wildfire_flux_of_toluene_lump": "Wildfire flux of toluene_lump (C7H8+ C6H6 + C8H10)",
                    "wildfire_flux_of_total_carbon_in_aerosols": "Wildfire flux of total carbon in aerosols",
                    "wildfire_flux_of_total_particulate_matter": "Wildfire flux of total particulate matter",
                    "wildfire_flux_of_xylene": "Wildfire flux of xylene (C8H10)",
                    "wildfire_fraction_of_area_observed": "Wildfire fraction of area observed",
                    "wildfire_overall_flux_of_burnt_carbon": "Wildfire overall flux of burnt carbon",
                    "wildfire_radiative_power": "Wildfire radiative power",
                },
            },
            "id": 0,
        },
        {
            "name": "date",
            "label": "Date",
            "help": None,
            "required": True,
            "css": "todo",
            "type": "DateRangeWidget",
            "details": {
                "minStart": "2025-03-15",
                "maxEnd": "2025-04-09",
                "defaultStart": "2025-04-09",
                "defaultEnd": "2025-04-09",
            },
            "id": 1,
        },
        {
            "name": "data_format",
            "label": "Data format",
            "help": None,
            "required": True,
            "css": "todo",
            "type": "StringChoiceWidget",
            "details": {
                "values": ["grib", "netcdf"],
                "columns": 2,
                "labels": {"grib": "GRIB", "netcdf": "NetCDF4 (Experimental)"},
                "default": ["grib"],
            },
            "id": 2,
        },
    ]

    constraints = [
        {
            "date": [
                "2025-03-15",
                "2025-03-16",
                "2025-03-17",
                "2025-03-18",
                "2025-03-19",
                "2025-03-20",
                "2025-03-21",
                "2025-03-22",
                "2025-03-23",
                "2025-03-24",
                "2025-03-25",
                "2025-03-26",
                "2025-03-27",
                "2025-03-28",
                "2025-03-29",
                "2025-03-30",
                "2025-03-31",
                "2025-04-01",
                "2025-04-02",
                "2025-04-03",
                "2025-04-04",
                "2025-04-05",
                "2025-04-06",
                "2025-04-07",
                "2025-04-08",
                "2025-04-09",
            ],
            "variable": [
                "altitude_of_plume_top",
                "mean_altitude_of_maximum_injection",
                "wildfire_combustion_rate",
                "wildfire_flux_of_acetaldehyde",
                "wildfire_flux_of_acetone",
                "wildfire_flux_of_ammonia",
                "wildfire_flux_of_benzene",
                "wildfire_flux_of_black_carbon",
                "wildfire_flux_of_butanes",
                "wildfire_flux_of_butenes",
                "wildfire_flux_of_carbon_dioxide",
                "wildfire_flux_of_carbon_monoxide",
                "wildfire_flux_of_dimethyl_sulfide",
                "wildfire_flux_of_ethane",
                "wildfire_flux_of_ethanol",
                "wildfire_flux_of_ethene",
                "wildfire_flux_of_formaldehyde",
                "wildfire_flux_of_heptane",
                "wildfire_flux_of_hexanes",
                "wildfire_flux_of_hexene",
                "wildfire_flux_of_higher_alkanes",
                "wildfire_flux_of_higher_alkenes",
                "wildfire_flux_of_hydrogen",
                "wildfire_flux_of_isoprene",
                "wildfire_flux_of_methane",
                "wildfire_flux_of_methanol",
                "wildfire_flux_of_nitrogen_oxides",
                "wildfire_flux_of_nitrous_oxide",
                "wildfire_flux_of_non_methane_hydrocarbons",
                "wildfire_flux_of_octene",
                "wildfire_flux_of_organic_carbon",
                "wildfire_flux_of_particulate_matter_d_2_5_\u00b5m",
                "wildfire_flux_of_pentanes",
                "wildfire_flux_of_pentenes",
                "wildfire_flux_of_propane",
                "wildfire_flux_of_propene",
                "wildfire_flux_of_sulphur_dioxide",
                "wildfire_flux_of_terpenes",
                "wildfire_flux_of_toluene",
                "wildfire_flux_of_toluene_lump",
                "wildfire_flux_of_total_carbon_in_aerosols",
                "wildfire_flux_of_total_particulate_matter",
                "wildfire_flux_of_xylene",
                "wildfire_fraction_of_area_observed",
                "wildfire_overall_flux_of_burnt_carbon",
                "wildfire_radiative_power",
            ],
        },
        {
            "date": [
                "2025-04-01",
                "2025-04-02",
                "2025-04-03",
                "2025-04-04",
                "2025-04-05",
                "2025-04-06",
                "2025-04-07",
                "2025-04-08",
                "2025-04-09",
            ],
            "variable": ["altitude_of_plume_bottom", "injection_height"],
        },
    ]

    costing = {
        "costing_kwargs": {"ignore_keys": ["area"]},
        "costing_class_kwargs": {
            "cost_type": "size",
            "inclusive_upper_bounds": [1, 5, 10, 50, 70],
        },
        "max_costs": {"size": 1000},
    }

    adaptor = DummyCdsAdaptor(form, constraints=constraints, costing=costing)

    request: dict[str, Any] = {
        "variable": ["altitude_of_plume_bottom"],
        "date": ["2025-04-01"],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 1
    assert costs["cost_class"] == 0

    request = {
        "variable": ["altitude_of_plume_bottom", "altitude_of_plume_top"],
        "date": ["2025-04-01"],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 2
    assert costs["cost_class"] == 1

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
        ],
        "date": ["2025-04-01"],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 5
    assert costs["cost_class"] == 1

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
            "wildfire_flux_of_dimethyl_sulfide",
        ],
        "date": ["2025-04-01"],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 6
    assert costs["cost_class"] == 2

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
        ],
        "date": ["2025-04-01", "2025-04-02", "2025-04-07"],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 15
    assert costs["cost_class"] == 3

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
            "wildfire_flux_of_acetaldehyde",
            "wildfire_flux_of_acetone",
            "wildfire_flux_of_ammonia",
            "wildfire_flux_of_benzene",
            "wildfire_flux_of_black_carbon",
        ],
        "date": [
            "2025-04-01",
            "2025-04-02",
            "2025-04-05",
            "2025-04-06",
            "2025-04-07",
            "2025-04-08",
        ],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 60
    assert costs["cost_class"] == 4

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
            "wildfire_flux_of_acetaldehyde",
            "wildfire_flux_of_acetone",
            "wildfire_flux_of_ammonia",
            "wildfire_flux_of_benzene",
            "wildfire_flux_of_black_carbon",
        ],
        "date": [
            "2025-03-21",
            "2025-03-22",
            "2025-03-27",
            "2025-03-28",
            "2025-03-29",
            "2025-04-01",
            "2025-04-02",
            "2025-04-07",
            "2025-04-08",
            "2025-04-09",
        ],
        "data_format": "grib",
    }
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 100
    assert costs["cost_class"] == 5

    costing = {
        "costing_kwargs": {"ignore_keys": ["area"]},
        "costing_class_kwargs": {
            "cost_type": "size",
            "inclusive_upper_bounds": {"small": 5, "medium": 10, "large": 30},
            "last_class_name": "extra_large",
        },
        "max_costs": {"size": 1000},
    }

    adaptor_with_explicit_classes = DummyCdsAdaptor(
        form, constraints=constraints, costing=costing
    )

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
        ],
        "date": ["2025-04-01"],
        "data_format": "grib",
    }
    costs = adaptor_with_explicit_classes.estimate_costs(request)
    assert costs["size"] == 4
    assert costs["cost_class"] == "small"

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
            "wildfire_flux_of_dimethyl_sulfide",
        ],
        "date": ["2025-04-01"],
        "data_format": "grib",
    }
    costs = adaptor_with_explicit_classes.estimate_costs(request)
    assert costs["size"] == 6
    assert costs["cost_class"] == "medium"

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
        ],
        "date": ["2025-04-01", "2025-04-02", "2025-04-07"],
        "data_format": "grib",
    }
    costs = adaptor_with_explicit_classes.estimate_costs(request)
    assert costs["size"] == 15
    assert costs["cost_class"] == "large"

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
            "wildfire_flux_of_acetaldehyde",
            "wildfire_flux_of_acetone",
            "wildfire_flux_of_ammonia",
            "wildfire_flux_of_benzene",
            "wildfire_flux_of_black_carbon",
        ],
        "date": ["2025-04-01", "2025-04-02", "2025-04-05", "2025-04-06", "2025-04-07"],
        "data_format": "grib",
    }
    costs = adaptor_with_explicit_classes.estimate_costs(request)
    assert costs["size"] == 50
    assert costs["cost_class"] == "extra_large"

    costing = {
        "costing_kwargs": {
            "weighted_keys": {"variable": 2},
            "weighted_values": {
                "variable": {
                    "altitude_of_plume_top": 2,
                    "wildfire_combustion_rate": 3,
                }
            },
            "ignore_keys": ["area"],
        },
        "costing_class_kwargs": {
            "cost_type": "size",
            "inclusive_upper_bounds": {"small": 5, "medium": 10, "large": 100},
            "last_class_name": "extra_large",
        },
        "max_costs": {"precise_size": 1000},
    }

    adaptor_with_explicit_classes_and_weights = DummyCdsAdaptor(
        form, constraints=constraints, costing=costing
    )

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
        ],
        "date": ["2025-03-15", "2025-04-02", "2025-04-07"],
        "data_format": "grib",
    }
    costs = adaptor_with_explicit_classes_and_weights.estimate_costs(request)
    assert costs["size"] == 48
    assert costs["precise_size"] == 44
    assert costs["cost_class"] == "large"

    costing = {
        "costing_kwargs": {
            "weighted_keys": {"variable": 2},
            "weighted_values": {
                "variable": {
                    "altitude_of_plume_top": 2,
                    "injection_height": 3,
                }
            },
            "ignore_keys": ["area"],
        },
        "costing_class_kwargs": {
            "cost_type": "highest_cost_limit_ratio",
            "inclusive_upper_bounds": {"small": 5, "medium": 40, "large": 100},
            "last_class_name": "extra_large",
        },
        "max_costs": {"size": 1000, "precise_size": 50},
    }

    adaptor_with_explicit_classes_and_weights_for_highest_cost_limit_ratio = (
        DummyCdsAdaptor(form, constraints=constraints, costing=costing)
    )

    request = {
        "variable": [
            "altitude_of_plume_bottom",
            "altitude_of_plume_top",
            "injection_height",
            "mean_altitude_of_maximum_injection",
            "wildfire_combustion_rate",
        ],
        "date": ["2025-03-15", "2025-04-02", "2025-04-07"],
        "data_format": "grib",
    }
    costs = adaptor_with_explicit_classes_and_weights_for_highest_cost_limit_ratio.estimate_costs(
        request
    )
    assert costs["size"] == 48
    assert costs["precise_size"] == 40
    assert costs["cost_class"] == "medium"


def test_estimate_costs_with_area_as_mapping() -> None:
    from cads_adaptors import DummyCdsAdaptor

    form = [
        {
            "name": "param",
            "label": "Param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        },
        {
            "name": "city",
            "label": "City",
            "details": {"values": {"London", "Paris", "Berlin"}},
            "type": "StringListWidget",
        },
    ]
    adaptor = DummyCdsAdaptor(
        form,
        constraints=[{"param": {"Z", "T"}}],
        costing={"max_costs": {"size": 10, "precise_size": 10}},
        mapping={
            "options": {
                "area_as_mapping": [
                    {"latitude": 51.51, "longitude": -0.13, "city": "London"},
                    {"latitude": 48.86, "longitude": 2.35, "city": "Paris"},
                    {"latitude": 52.52, "longitude": 13.41, "city": "Berlin"},
                ]
            }
        },
    )

    # Test normal selection
    request: dict[str, Any] = {"param": {"Z", "T"}, "city": {"London", "Paris"}}
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 4

    # Test with area as mapping, area covers London and Paris
    request = {"param": {"Z", "T"}, "area": [55, -1, 45, 5]}
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 4

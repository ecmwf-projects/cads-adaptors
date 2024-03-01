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

    constraints = [form_key_values]

    weighted_keys = {"param": 2}

    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T", "Q"}, "level": {"500"}},
            constraints,
            weighted_keys=weighted_keys,
        )
        == 9
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z"}, "level": {"500", "850", "1000"}},
            constraints,
            weighted_keys=weighted_keys,
        )
        == 3
    )
    assert (
        costing.estimate_granules(
            form_key_values,
            {"param": {"Z", "T", "Q"}, "level": {"500", "850", "1000"}},
            constraints,
            weighted_keys=weighted_keys,
        )
        == 27
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

    request = {"inputs": {"param": {"Z", "T"}}}
    costs = adaptor.estimate_costs(request)
    assert costs["size"] == 2
    assert costs["number_of_fields"] == 2

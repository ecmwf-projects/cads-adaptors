from cads_adaptors import costing


def test_estimate_precise_size_basic() -> None:
    form = [
        {
            "name": "param",
            "label": "Param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        },
        {
            "name": "level",
            "label": "Level",
            "details": {"values": {"500", "850"}},
            "type": "StringListWidget",
        },
    ]

    assert (
        costing.estimate_precise_size(form, [{"param": {"Z", "T"}, "level": {"500"}}])
        == 2
    )
    assert (
        costing.estimate_precise_size(
            form, [{"param": {"Z", "T"}, "level": {"500", "850"}}]
        )
        == 4
    )

    assert (
        costing.estimate_precise_size(form, [{"param": {"Z"}, "level": {"500"}}]) == 1
    )
    assert costing.estimate_precise_size(form, [{"param": {"Z"}}]) == 1

    # We still count keys not in form:
    assert (
        costing.estimate_precise_size(
            form, [{"param": {"Z"}, "time": {"12:00", "00:00"}, "level": {"500"}}]
        )
        == 2
    )


def test_estimate_precise_size_long() -> None:
    form = [
        {
            "name": "param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        },
        {
            "name": "level",
            "details": {"values": {"500", "850"}},
            "type": "StringListWidget",
        },
        {
            "name": "model",
            "details": {"values": {"a", "b", "c"}},
            "type": "StringListWidget",
        },
    ]

    assert (
        costing.estimate_precise_size(
            form,
            [
                {"param": {"Z"}},
                {"param": {"Z"}, "level": {"500"}},
                {"param": {"Z"}, "level": {"500"}, "model": {"a", "b"}},
            ],
        )
        == 4
    )

    assert (
        costing.estimate_precise_size(
            form,
            [
                {"param": {"Z"}},
                {"param": {"Z"}, "level": {"500"}},
                {"param": {"Z"}, "level": {"500"}, "model": {"a", "b"}},
                {"param": {"Z", "T"}, "level": {"700"}, "model": {"a", "b"}},
                {"param": {"Z"}, "time": {"01", "02"}, "level": {"500"}},
            ],
        )
        == 10
    )

    # Handle duplicates
    assert (
        costing.estimate_precise_size(
            form,
            [
                {"param": {"Z"}},
                {"param": {"Z"}, "level": {"500"}},
                {"param": {"Z"}, "level": {"500"}, "model": {"a", "b"}},
                {"param": {"Z", "T"}, "level": {"500", "700"}, "model": {"a", "b"}},
                {"param": {"Z"}, "time": {"01", "02"}, "level": {"500"}},
            ],
        )
        == 12
    )


def test_estimate_precise_size_weighted_keys() -> None:
    form = [
        {
            "name": "param",
            "label": "Param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        },
        {
            "name": "level",
            "label": "Level",
            "details": {"values": {"500", "850"}},
            "type": "StringListWidget",
        },
    ]

    assert (
        costing.estimate_precise_size(
            form, [{"param": {"Z"}, "level": {"500"}}], weighted_keys={"param": 2}
        )
        == 2
    )
    assert (
        costing.estimate_precise_size(
            form, [{"param": {"Z"}, "level": {"500"}}], weighted_keys={"level": 3}
        )
        == 3
    )
    assert (
        costing.estimate_precise_size(
            form, [{"param": {"Z", "T"}, "level": {"500"}}], weighted_keys={"param": 2}
        )
        == 4
    )
    assert (
        costing.estimate_precise_size(
            form, [{"param": {"Z"}}], weighted_keys={"level": 2}
        )
        == 1
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Z", "T"}, "level": {"500"}}, {"param": {"Z"}, "a": {"b"}}],
            weighted_keys={"param": 2},
        )
        == 6
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Z", "T"}, "level": {"500"}}, {"level": {"500"}, "a": {"b"}}],
            weighted_keys={"param": 2},
        )
        == 5
    )


def test_estimate_precise_size_weighted_values() -> None:
    form = [
        {
            "name": "param",
            "label": "Param",
            "details": {"values": {"Z", "T"}},
            "type": "StringListWidget",
        },
        {
            "name": "level",
            "label": "Level",
            "details": {"values": {"500", "850"}},
            "type": "StringListWidget",
        },
    ]

    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Z"}, "level": {"500"}}],
            weighted_values={"param": {"Z": 2}},
        )
        == 2
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"T"}, "level": {"500"}}],
            weighted_values={"param": {"Z": 2}},
        )
        == 1
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Z", "T"}, "level": {"500"}}],
            weighted_values={"param": {"Z": 2}},
        )
        == 3
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Z", "T"}, "level": {"500"}}],
            weighted_values={"param": {"Z": 2}, "level": {"500": 3}},
        )
        == 9
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Z", "T"}, "level": {"600"}}],
            weighted_values={"param": {"Z": 2}, "level": {"500": 3}},
        )
        == 3
    )
    assert (
        costing.estimate_precise_size(
            form,
            [{"param": {"Q", "T"}, "level": {"500"}}],
            weighted_values={"param": {"Z": 2}, "level": {"500": 3}},
        )
        == 6
    )
    assert (
        costing.estimate_precise_size(
            form,
            [
                {"param": {"Z", "T"}, "level": {"600"}},
                {"param": {"Q", "T"}, "level": {"500"}},
                {"param": {"Z"}, "level": {"500"}},
                {"level": {"500"}, "a": {"b"}},
            ],
            weighted_values={"param": {"Z": 2}, "level": {"500": 3}},
        )
        == 18
    )

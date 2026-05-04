from datetime import datetime, timedelta


def test_request_tags() -> None:
    from cads_adaptors import DummyCdsAdaptor

    TODAY = datetime.today().strftime("%Y-%m-%d")
    IN_THE_LAST_MONTH = (datetime.today() - timedelta(days=10)).strftime("%Y-%m-%d")

    form = [
        {
            "name": "date",
            "label": "Date",
            "type": "DateRangeWidget",
            "details": {
                "minStart": "2015-01-01",
                "maxEnd": TODAY,
            },
        },
        {
            "name": "t",
            "label": "Type",
            "type": "StringListWidget",
            "details": {
                "values": [
                    "f",
                    "a",
                ],
                "labels": {
                    "f": "Forecast",
                    "a": "Analysis",
                },
            },
        },
        {
            "name": "v",
            "label": "Variable",
            "type": "StringListWidget",
            "details": {
                "values": [
                    "A",
                    "B",
                    "C",
                    "M",
                    "Z",
                ],
                "labels": {
                    "A": "A",
                    "B": "B",
                    "C": "C",
                    "M": "M",
                    "Z": "Z",
                },
            },
        },
    ]
    adaptor = DummyCdsAdaptor(
        form,
        conditional_tagging={
            "LAST_MONTH": [
                {
                    "date": "current-30/current",
                    "t": "f",
                    "v": [
                        "A",
                        "B",
                        "M",
                    ],
                }
            ],
            "TODAY": [{"date": "current/current"}],
        },
    )

    request = {"date": TODAY, "t": "f", "v": {"A", "M"}}
    tags = adaptor.get_request_tags(request)
    assert set(tags) == {"TODAY", "LAST_MONTH"}

    request = {"date": TODAY, "t": "f", "v": {"A", "Z"}}
    tags = adaptor.get_request_tags(request)
    assert set(tags) == {"TODAY"}

    request = {"date": IN_THE_LAST_MONTH, "t": "f", "v": {"A", "M"}}
    tags = adaptor.get_request_tags(request)
    assert set(tags) == {"LAST_MONTH"}

    request = {"date": IN_THE_LAST_MONTH, "t": "f", "v": {"Z", "A"}}
    tags = adaptor.get_request_tags(request)
    assert tags == []

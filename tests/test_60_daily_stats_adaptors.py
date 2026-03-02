import numpy as np
import pytest
import xarray as xr

from cads_adaptors.adaptors import Context
from cads_adaptors.adaptors.daily_statistics import Era5DailyStatisticsCdsAdaptor
from cads_adaptors.exceptions import InvalidRequest


class RecordingContext(Context):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logs: list[str] = []
        self.errors: list[str] = []

    def add_user_visible_log(self, message: str, session=None) -> None:
        self.logs.append(message)

    def add_user_visible_error(self, message: str, session=None) -> None:
        self.errors.append(message)


def _make_dataset():
    times = np.array(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ],
        dtype="datetime64[ns]",
    )
    return xr.Dataset(
        {"var": ("valid_time", [1, 2, 3, 4, 5])},
        coords={"valid_time": times},
    )


def test_remove_partial_periods_consecutive_dates():
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=RecordingContext())
    in_xarray_dict = {"test": _make_dataset()}

    result = adaptor.remove_partial_periods(
        in_xarray_dict, date_list=["2020-01-02", "2020-01-03", "2020-01-04"]
    )

    out_times = result["test"].coords["valid_time"].values
    assert (
        out_times.tolist()
        == np.array(
            ["2020-01-02", "2020-01-03", "2020-01-04"], dtype="datetime64[ns]"
        ).tolist()
    )


def test_remove_partial_periods_non_consecutive_dates():
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=RecordingContext())
    in_xarray_dict = {"test": _make_dataset()}

    result = adaptor.remove_partial_periods(
        in_xarray_dict, date_list=["2020-01-02", "2020-01-04"]
    )

    out_times = result["test"].coords["valid_time"].values
    assert (
        out_times.tolist()
        == np.array(["2020-01-02", "2020-01-04"], dtype="datetime64[ns]").tolist()
    )


def test_pre_mapping_modifications_sets_receipt_and_download_format():
    context = RecordingContext()
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=context)

    request = {
        "post_process": ["ignored-step"],
        "download_format": "zip",
        "daily_statistic": "daily_mean",
        "time_zone": "UTC+00:00",
        "frequency": "1_hourly",
    }
    out_request = adaptor.pre_mapping_modifications(request)

    assert adaptor.receipt is False
    assert adaptor.download_format == "zip"
    assert "receipt" not in out_request
    assert "post_process" not in out_request
    assert "download_format" not in out_request
    assert len(context.logs) == 1


@pytest.mark.parametrize(
    "key, value, expected_error",
    [
        ("daily_statistic", ["daily_mean", "daily_sum"], "Multiple daily statistic"),
        ("time_zone", ["UTC+00:00", "UTC+01:00"], "Multiple time zone"),
        ("frequency", ["1_hourly", "3_hourly"], "Multiple frequency"),
    ],
)
def test_pre_mapping_modifications_rejects_multiple_values(key, value, expected_error):
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=RecordingContext())

    request = {key: value}
    with pytest.raises(InvalidRequest, match=expected_error):
        adaptor.pre_mapping_modifications(request)


def test_get_date_list_extended_adds_buffer_days():
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=RecordingContext())

    dates = adaptor.get_date_list_extended(
        ["2020-01-02", "2020-01-03"],
        time_zone_hour=0,
        first_valid_date_str="1940-01-01",
    )

    assert dates == ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"]


def test_get_date_list_extended_respects_first_valid_date():
    context = RecordingContext()
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=context)

    dates = adaptor.get_date_list_extended(
        ["1940-01-01", "1940-01-02"],
        time_zone_hour=1,
        first_valid_date_str="1940-01-01",
    )

    assert dates == ["1940-01-02", "1940-01-03"]
    assert len(context.errors) == 1


def test_get_validated_accumulation_period_defaults_and_maps():
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=RecordingContext())

    mars_request = {"dataset": ["reanalysis"]}
    accumulation_period = adaptor.get_validated_accumulation_period(mars_request)
    assert accumulation_period == 1
    assert mars_request["dataset"] == "reanalysis"

    mars_request = {"dataset": ["mean"]}
    accumulation_period = adaptor.get_validated_accumulation_period(mars_request)
    assert accumulation_period == 3
    assert mars_request["dataset"] == "mean"


def test_get_validated_accumulation_period_rejects_unknown_dataset():
    adaptor = Era5DailyStatisticsCdsAdaptor(form=None, context=RecordingContext())

    with pytest.raises(InvalidRequest, match="Unrecognised product_type"):
        adaptor.get_validated_accumulation_period({"dataset": ["unknown"]})
